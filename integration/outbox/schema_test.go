//go:build integration

package outbox_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func createV1OutboxEventsTable(t *testing.T, db *gorm.DB) {
	t.Helper()

	require.NoError(t, db.Exec(`
		create table outbox_events (
			id uuid primary key default gen_random_uuid(),
			topic varchar(255) not null,
			payload bytea not null,
			state varchar(50) not null default 'PENDING',
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now()
		)
	`).Error)
	require.NoError(t, db.Exec(`create index on outbox_events (state)`).Error)
	require.NoError(t, db.Exec(`create index on outbox_events (created_at)`).Error)
}

func regclassExists(t *testing.T, db *gorm.DB, name string) bool {
	t.Helper()

	var regclass sql.NullString
	require.NoError(t, db.Raw(`select to_regclass(?)::text`, name).Scan(&regclass).Error)
	return regclass.Valid
}

func TestSchemaV1TableWithoutRetriesPublishesLikeLegacy(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	createV1OutboxEventsTable(t, db)

	js := testenv.NATS(t)
	newStream(t, js, "SCHEMA_V1_OK", "schema.v1.ok")

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "schema.v1.ok",
		Payload: []byte("payload-v1-ok"),
	})

	require.False(t, regclassExists(t, db, "outbox_retries"))

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StateDone
	}, 10*time.Second, 100*time.Millisecond, "a v1 table without v2 columns should still publish like v0.1.9")
}

func TestSchemaV1TableWithoutRetriesTableToleratesPublishFailure(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	createV1OutboxEventsTable(t, db)
	require.False(t, regclassExists(t, db, "outbox_retries"))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "schema.v1.failure",
		Payload: []byte("payload-v1-failure"),
	})

	producer := &testenv.FakeProducer{
		Fail: func(outbox.ProducerEvent) error {
			return errors.New("simulated publish failure")
		},
	}

	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StatePending
	}, 10*time.Second, 100*time.Millisecond, "event should revert to PENDING even without an outbox_retries table")

	require.False(t, regclassExists(t, db, "outbox_retries"), "relay must not create outbox_retries as a side effect of a failed publish")
}

func TestSchemaAutoMigrateIsIdempotentAndAddsV2ColumnsAndRetriesTable(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)

	require.NoError(t, outbox.AutoMigrate(db))
	require.NoError(t, outbox.AutoMigrate(db))

	var v2Columns int64
	require.NoError(
		t,
		db.Raw(`select count(*) from information_schema.columns where table_name = 'outbox_events' and column_name in ('attempts', 'last_error', 'next_attempt_at', 'headers')`).
			Scan(&v2Columns).Error,
	)
	require.Equal(t, int64(4), v2Columns)

	require.True(t, regclassExists(t, db, "outbox_retries"))

	var indexCount int64
	require.NoError(
		t,
		db.Raw(`select count(*) from pg_indexes where tablename = 'outbox_events' and indexname = 'idx_outbox_events_state_updated_at'`).
			Scan(&indexCount).Error,
	)
	require.Equal(t, int64(1), indexCount)
}

func TestSchemaV1EventInsertedIntoV2TableGetsAttemptsZeroAndNullHeaders(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "schema.v2.defaults",
		Payload: []byte("payload-v2-defaults"),
	})

	var attempts int
	var headers sql.NullString
	row := db.Raw(`select attempts, headers from outbox_events where id = ?`, id).Row()
	require.NoError(t, row.Scan(&attempts, &headers))

	require.Equal(t, 0, attempts)
	require.False(t, headers.Valid)
}
