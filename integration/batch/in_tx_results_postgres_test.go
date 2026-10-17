//go:build integration

package batch_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestInTxPostgresApplyResultsReleasesRetryAndPoisonClaims(t *testing.T) {
	db, tx := inTxEnv(t)
	handler, err := batch.InTx(batch.InTxOpts{
		TxManager: tx,
		ApplyResults: func(ctx context.Context, msgs []jetstream.Msg) ([]batch.Result, error) {
			results := make(
				[]batch.Result,
				len(msgs),
			)
			for i, msg := range msgs {
				switch string(msg.Data()) {
				case "retry":
					results[i] = batch.NakDelay(
						time.Second,
						errors.New("later"),
					)
				case "poison":
					results[i] = batch.Term(errors.New("bad"))
				case "skip":
					results[i] = batch.Skip()
				default:
					if err := tx.GetConnection(ctx).Create(&item{Name: string(msg.Data())}).Error; err != nil {
						return nil, err
					}
					results[i] = batch.Ack()
				}
			}
			return results, nil
		},
	})
	require.NoError(t, err)

	results := handler(
		context.Background(),
		pgMsgs("a", "retry", "skip", "poison", "b"),
	)
	require.Equal(t, batch.VerdictAck, results[0].Verdict)
	require.Equal(t, batch.VerdictNakDelay, results[1].Verdict)
	require.Equal(t, batch.VerdictSkip, results[2].Verdict)
	require.Equal(t, batch.VerdictTerm, results[3].Verdict)
	require.Equal(t, batch.VerdictAck, results[4].Verdict)

	require.EqualValues(t, 2, count(t, db, "items"))
	var claimed []string
	require.NoError(t, db.Raw("select message_id from processed_messages order by message_id").Scan(&claimed).Error)
	require.Equal(t, []string{"id-a", "id-b", "id-skip"}, claimed)

	again := handler(
		context.Background(),
		pgMsgs("retry"),
	)
	require.Equal(t, batch.VerdictNakDelay, again[0].Verdict)
}

func TestInTxPostgresOrderKeyCommitsOtherKeysAndHoldsTail(t *testing.T) {
	db, tx := inTxEnv(t)
	require.NoError(t, db.Create(&item{Name: "A2"}).Error)

	handler, err := batch.InTx(batch.InTxOpts{
		TxManager: tx,
		OrderKey: func(msg jetstream.Msg) string {
			return string(msg.Data()[:1])
		},
		Apply: func(ctx context.Context, msgs []jetstream.Msg) error {
			for _, msg := range msgs {
				if err := tx.GetConnection(ctx).Create(&item{Name: string(msg.Data())}).Error; err != nil {
					return err
				}
			}
			return nil
		},
	})
	require.NoError(t, err)

	results := handler(
		context.Background(),
		pgMsgs("A1", "B1", "A2", "A3"),
	)
	require.Equal(t, batch.VerdictAck, results[0].Verdict)
	require.Equal(t, batch.VerdictAck, results[1].Verdict)
	require.Equal(t, batch.VerdictNak, results[2].Verdict)
	require.ErrorIs(t, results[3].Err, batch.ErrOrderBlocked)

	var names []string
	require.NoError(t, db.Raw("select name from items order by name").Scan(&names).Error)
	require.Equal(t, []string{"A1", "A2", "B1"}, names)

	var claimed []string
	require.NoError(t, db.Raw("select message_id from processed_messages order by message_id").Scan(&claimed).Error)
	require.Equal(t, []string{"id-A1", "id-B1"}, claimed)
}
