//go:build integration

package batch_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	"github.com/dehwyy/txmanagerfx/pkg/txmanager/gormtx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type pgMsg struct {
	jetstream.Msg
	data    []byte
	headers nats.Header
}

func (m *pgMsg) Data() []byte         { return m.data }
func (m *pgMsg) Subject() string      { return "items.created" }
func (m *pgMsg) Headers() nats.Header { return m.headers }

func pgMsgs(data ...string) []jetstream.Msg {
	out := make([]jetstream.Msg, len(data))
	for i, d := range data {
		headers := nats.Header{}
		headers.Set(jetstream.MsgIDHeader, "id-"+d)
		out[i] = &pgMsg{data: []byte(d), headers: headers}
	}
	return out
}

type item struct {
	Name string `gorm:"primaryKey"`
}

func inTxEnv(t *testing.T) (*gorm.DB, *gormtx.TxManager) {
	t.Helper()
	db := testenv.Postgres(t)
	require.NoError(t, consumer.AutoMigrateProcessed(db))
	require.NoError(t, db.AutoMigrate(&item{}))
	tx, err := gormtx.New(gormtx.Opts{DB: db})
	require.NoError(t, err)
	return db, tx
}

func count(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Table(table).Count(&n).Error)
	return n
}

func TestInTxPostgresDedup(t *testing.T) {
	db, tx := inTxEnv(t)
	applyCalls := 0
	handler, err := batch.InTx(batch.InTxOpts{
		TxManager: tx,
		Apply: func(ctx context.Context, msgs []jetstream.Msg) error {
			applyCalls++
			for _, msg := range msgs {
				if err := tx.GetConnection(ctx).Create(&item{Name: string(msg.Data())}).Error; err != nil {
					return err
				}
			}
			return nil
		},
	})
	require.NoError(t, err)

	msgs := pgMsgs("a", "b", "c", "d", "e")
	for _, result := range handler(context.Background(), msgs) {
		require.Equal(t, batch.VerdictAck, result.Verdict)
	}
	for _, result := range handler(context.Background(), msgs) {
		require.Equal(t, batch.VerdictAck, result.Verdict)
	}

	require.Equal(t, 1, applyCalls)
	require.EqualValues(t, 5, count(t, db, "items"))
	require.EqualValues(t, 5, count(t, db, "processed_messages"))
}

func TestInTxPostgresBisectRollsBackPoisonOnly(t *testing.T) {
	db, tx := inTxEnv(t)
	bad := errors.New("poison payload")
	handler, err := batch.InTx(batch.InTxOpts{
		TxManager: tx,
		Apply: func(ctx context.Context, msgs []jetstream.Msg) error {
			for _, msg := range msgs {
				if err := tx.GetConnection(ctx).Create(&item{Name: string(msg.Data())}).Error; err != nil {
					return err
				}
				if string(msg.Data()) == "poison" {
					return fmt.Errorf("apply %s: %w", msg.Data(), bad)
				}
			}
			return nil
		},
	})
	require.NoError(t, err)

	results := handler(context.Background(), pgMsgs("a", "b", "poison", "c", "d"))

	for i, result := range results {
		if i == 2 {
			require.Equal(t, batch.VerdictNak, result.Verdict)
			require.ErrorIs(t, result.Err, bad)
			continue
		}
		require.Equal(t, batch.VerdictAck, result.Verdict)
	}
	require.EqualValues(t, 4, count(t, db, "items"))
	require.EqualValues(t, 4, count(t, db, "processed_messages"))
	var poisonKeys int64
	require.NoError(t, db.Table("processed_messages").Where("message_id = ?", "id-poison").Count(&poisonKeys).Error)
	require.Zero(t, poisonKeys)
}
