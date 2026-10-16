//go:build integration

package outbox_test

import (
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/stretchr/testify/require"
)

func TestCleanupBatchedDeletesDoneAndParkedKeepsActive(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	require.NoError(t, db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'DONE', now() - interval '3 hour', now() - interval '2 hour' from generate_series(1, 50000) g").Error)
	require.NoError(t, db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'DONE', now(), now() from generate_series(1, 30) g").Error)
	require.NoError(t, db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'PARKED', now() - interval '3 hour', now() - interval '2 hour' from generate_series(1, 700) g").Error)
	require.NoError(t, db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'PENDING', now() - interval '3 hour', now() - interval '2 hour' from generate_series(1, 40) g").Error)
	require.NoError(t, db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'IN_FLIGHT', now() - interval '3 hour', now() - interval '2 hour' from generate_series(1, 20) g").Error)

	harness := testenv.NewRelayHarness(
		t,
		db,
		&testenv.FakeProducer{},
		outbox.Config{
			Mode:            outbox.ModeUpdateAfterSend,
			BatchSize:       10,
			TickInterval:    time.Hour,
			DeleteOlderThan: time.Hour,
			StallThreshold:  10 * time.Hour,
			CleanupInterval: 200 * time.Millisecond,
			RetainParked:    time.Hour,
			CleanupBatch:    5000,
		},
	)
	harness.Relay.Pause()
	harness.Start(t)

	require.Eventually(
		t,
		func() bool {
			return countRows(t, db, "select count(*) from outbox_events where state in ('DONE', 'PARKED') and updated_at < now() - interval '1 hour'") == 0
		},
		60*time.Second,
		100*time.Millisecond,
	)
	require.Equal(t, int64(30), countRows(t, db, "select count(*) from outbox_events where state = 'DONE'"))
	require.Equal(t, int64(40), countRows(t, db, "select count(*) from outbox_events where state = 'PENDING'"))
	require.Equal(t, int64(20), countRows(t, db, "select count(*) from outbox_events where state = 'IN_FLIGHT'"))
	require.Equal(t, int64(0), countRows(t, db, "select count(*) from outbox_events where state = 'PARKED'"))

	harness.Stop()
}
