//go:build integration

package outbox_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type sqlRecorder struct {
	mu         sync.Mutex
	statements []string
}

func (r *sqlRecorder) register(t *testing.T, db *gorm.DB) {
	t.Helper()

	record := func(tx *gorm.DB) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.statements = append(
			r.statements,
			tx.Statement.SQL.String(),
		)
	}
	callbacks := db.Callback()
	require.NoError(t, callbacks.Update().After("gorm:update").Register("record:update", record))
	require.NoError(t, callbacks.Delete().After("gorm:delete").Register("record:delete", record))
}

func (r *sqlRecorder) idFilters() (anyCount int, inCount int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, statement := range r.statements {
		if strings.Contains(statement, "id = ANY(") {
			anyCount++
		}
		if strings.Contains(statement, "id IN (") {
			inCount++
		}
	}

	return anyCount, inCount
}

func createUUIDOutboxV2(t *testing.T, db *gorm.DB) {
	t.Helper()

	createV1OutboxEventsTable(t, db)
	for _, statement := range []string{
		`alter table outbox_events add column attempts integer not null default 0`,
		`alter table outbox_events add column last_error text null`,
		`alter table outbox_events add column next_attempt_at timestamptz null`,
		`alter table outbox_events add column headers jsonb null`,
		`create table outbox_retries (id uuid primary key default gen_random_uuid(), event_id uuid not null, error text not null, created_at timestamptz not null default now())`,
	} {
		require.NoError(t, db.Exec(statement).Error)
	}
}

func TestRelaySettlesWithAnyForUUIDAndTextIDs(t *testing.T) {
	t.Parallel()

	for _, idType := range []string{"uuid", "text"} {
		for _, mode := range []outbox.OutboxMode{outbox.ModeUpdateAfterSend, outbox.ModeDeleteAfterSend} {
			for _, batched := range []bool{false, true} {
				idType := idType
				mode := mode
				batched := batched
				t.Run(
					fmt.Sprintf("%s/%s/batched=%v", idType, mode, batched),
					func(t *testing.T) {
						t.Parallel()

						db := testenv.Postgres(t)
						if idType == "uuid" {
							createUUIDOutboxV2(t, db)
						} else {
							require.NoError(t, outbox.AutoMigrate(db))
						}

						var dataType string
						require.NoError(t, db.Raw(`select data_type from information_schema.columns where table_name = 'outbox_events' and column_name = 'id'`).Scan(&dataType).Error)
						require.Equal(t, idType, dataType)

						recorder := &sqlRecorder{}
						recorder.register(
							t,
							db,
						)

						cfg := batchedConfig(mode)
						cfg.Batched = batched
						producer := &testenv.FakeBatchProducer{}
						harness := testenv.NewRelayHarness(
							t,
							db,
							producer,
							cfg,
						)

						messages := make(
							[]outbox.Message,
							5,
						)
						for i := range messages {
							messages[i] = outbox.Message{
								Subject: "id.any",
								Payload: []byte(fmt.Sprintf("m-%d", i)),
							}
						}
						require.NoError(t, harness.Store.SaveMessages(context.Background(), messages))

						harness.Start(t)
						harness.Wake()

						require.Eventually(
							t,
							func() bool {
								if mode == outbox.ModeDeleteAfterSend {
									return countRows(t, db, "select count(*) from outbox_events") == 0
								}
								return countRows(t, db, "select count(*) from outbox_events where state = 'DONE'") == 5
							},
							10*time.Second,
							50*time.Millisecond,
						)
						require.Equal(t, 5, producer.CallCount())

						anyCount, inCount := recorder.idFilters()
						require.Positive(t, anyCount, "settle must filter ids with = ANY")
						require.Zero(t, inCount, "no IN list for ids once the column type is known")
					},
				)
			}
		}
	}
}

func TestRequeueParkedWithAnyOnUUIDIDs(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	createUUIDOutboxV2(t, db)

	store := newStoreForTest(
		t,
		db,
	)
	require.NoError(t, store.SaveMessages(context.Background(), []outbox.Message{
		{
			Subject: "id.parked",
			Payload: []byte("a"),
		},
		{
			Subject: "id.parked",
			Payload: []byte("b"),
		},
	}))
	require.NoError(t, db.Exec(`update outbox_events set state = 'PARKED', attempts = 3`).Error)

	var ids []string
	require.NoError(t, db.Raw(`select id::text from outbox_events order by created_at`).Scan(&ids).Error)

	n, err := store.RequeueParked(
		context.Background(),
		ids[:1],
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, int64(1), countRows(t, db, "select count(*) from outbox_events where state = 'PENDING' and attempts = 0"))
}
