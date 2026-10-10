//go:build integration

package cleanup_test

import (
	"context"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/cleanup"
	"github.com/dehwyy/txmanagerfx/pkg/txmanager/gormtx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type keyedMsg struct {
	jetstream.Msg
	headers nats.Header
}

func (m *keyedMsg) Data() []byte         { return []byte("payload") }
func (m *keyedMsg) Subject() string      { return "cleanup.test" }
func (m *keyedMsg) Headers() nats.Header { return m.headers }

func TestCleanupPostgres(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, consumer.AutoMigrateProcessed(db))

	require.NoError(t, db.Exec(`insert into processed_messages (message_id, subject, processed_at)
		select 'old-' || g, 'cleanup.test', now() - interval '49 hours' from generate_series(1, 3000) g`).Error)
	require.NoError(t, db.Exec(`insert into processed_messages (message_id, subject, processed_at)
		select 'fresh-' || g, 'cleanup.test', now() - interval '1 hour' from generate_series(1, 8900) g`).Error)
	require.NoError(t, db.Exec(`insert into processed_messages (message_id, subject, processed_at)
		select 'edge-' || g, 'cleanup.test', now() - interval '47 hours 55 minutes' from generate_series(1, 100) g`).Error)

	cleaner, err := cleanup.New(cleanup.Opts{
		DB:         db,
		BatchLimit: 1000,
	})
	require.NoError(t, err)

	deleted, err := cleaner.RunOnce(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 3000, deleted)

	var remaining int64
	require.NoError(t, db.Table("processed_messages").Count(&remaining).Error)
	require.EqualValues(t, 9000, remaining)

	tx, err := gormtx.New(gormtx.Opts{DB: db})
	require.NoError(t, err)
	applied := 0
	handler, err := batch.InTx(batch.InTxOpts{
		TxManager: tx,
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			applied += len(msgs)
			return nil
		},
	})
	require.NoError(t, err)

	headers := nats.Header{}
	headers.Set(jetstream.MsgIDHeader, "edge-1")
	results := handler(context.Background(), []jetstream.Msg{&keyedMsg{headers: headers}})
	require.Equal(t, batch.VerdictAck, results[0].Verdict)
	require.Zero(t, applied)
}

func TestEnsureIndexIdempotent(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, consumer.AutoMigrateProcessed(db))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, cleanup.EnsureIndex(ctx, db))
	require.NoError(t, cleanup.EnsureIndex(ctx, db))

	var indexes int64
	require.NoError(t, db.Raw(
		"select count(*) from pg_indexes where tablename = 'processed_messages' and indexname = 'idx_processed_messages_processed_at'",
	).Scan(&indexes).Error)
	require.EqualValues(t, 1, indexes)
}
