//go:build integration

package frames_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/txmanagerfx/pkg/txmanager/gormtx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

var errApply = errors.New("apply failed")

type item struct {
	Name string `gorm:"primaryKey"`
}

func dedupEnv(t *testing.T) (*gorm.DB, *gormtx.TxManager) {
	t.Helper()
	db := testenv.Postgres(t)
	require.NoError(t, consumer.AutoMigrateProcessed(db))
	require.NoError(t, db.AutoMigrate(&item{}))
	tx, err := gormtx.New(gormtx.Opts{DB: db})
	require.NoError(t, err)
	return db, tx
}

func rows(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Table(table).Count(&n).Error)
	return n
}

type poisonLog struct {
	mu   sync.Mutex
	msgs []jetstream.Msg
	errs []error
}

func (p *poisonLog) fn(_ context.Context, msg jetstream.Msg, err error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msg)
	p.errs = append(p.errs, err)
	return nil
}

func (p *poisonLog) snapshot() ([]jetstream.Msg, []error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]jetstream.Msg(nil), p.msgs...), append([]error(nil), p.errs...)
}

func drainBatch(t *testing.T, c *batch.Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, c.Drain(ctx))
}

func delivery(t *testing.T, msg jetstream.Msg) uint64 {
	t.Helper()
	meta, err := msg.Metadata()
	require.NoError(t, err)
	return meta.NumDelivered
}

func inTxBatch(t *testing.T, js jetstream.JetStream, name string, tx *gormtx.TxManager, failing func(jetstream.Msg) bool, maxAttempts int, poison *poisonLog) (*batch.Consumer, *recorder, *recorder) {
	t.Helper()
	seen := newRecorder()
	redelivered := newRecorder()
	handler, err := batch.InTx(batch.InTxOpts{
		TxManager: tx,
		Apply: func(ctx context.Context, msgs []jetstream.Msg) error {
			for _, msg := range msgs {
				seen.add(msg)
				if delivery(t, msg) > 1 {
					redelivered.add(msg)
				}
				if failing(msg) {
					return errApply
				}
				if err := tx.GetConnection(ctx).Create(&item{Name: string(msg.Data())}).Error; err != nil {
					return err
				}
			}
			return nil
		},
	})
	require.NoError(t, err)
	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              newStream(t, js, name),
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("lane", true).WithFilterSubject(name + ".>"),
		Handler:             handler,
		Delays:              []time.Duration{200 * time.Millisecond},
		MaxAttempts:         maxAttempts,
		OnPoison:            poison.fn,
	})
	require.NoError(t, err)
	return c, seen, redelivered
}

func TestFramePartialFailureAppliesEachElementOnce(t *testing.T) {
	db, tx := dedupEnv(t)
	js, _ := testenv.EmbeddedNATS(t)
	poison := &poisonLog{}
	c, _, redelivered := inTxBatch(t, js, "PARTIAL", tx, func(msg jetstream.Msg) bool {
		return string(msg.Data()) == "p2" && delivery(t, msg) == 1
	}, 5, poison)

	publishFrame(t, js, "PARTIAL.cmd", "p1", "p2", "p3")

	require.Eventually(t, func() bool { return rows(t, db, "items") == 3 }, 15*time.Second, 50*time.Millisecond)
	settledClean(t, js, "PARTIAL", "lane")
	drainBatch(t, c)

	require.EqualValues(t, 3, rows(t, db, "processed_messages"))
	require.EqualValues(t, 2, consumerInfo(t, js, "PARTIAL", "lane").Delivered.Consumer)
	require.Equal(t, []string{"p2"}, redelivered.subject("PARTIAL.cmd"))
	msgs, _ := poison.snapshot()
	require.Empty(t, msgs)
}

func TestFramePoisonElementGoesAloneToDLQ(t *testing.T) {
	db, tx := dedupEnv(t)
	js, _ := testenv.EmbeddedNATS(t)
	poison := &poisonLog{}
	c, _, _ := inTxBatch(t, js, "POISON", tx, func(msg jetstream.Msg) bool {
		return string(msg.Data()) == "bad"
	}, 2, poison)

	publishFrame(t, js, "POISON.cmd", "good1", "bad", "good2")

	require.Eventually(t, func() bool {
		msgs, _ := poison.snapshot()
		return len(msgs) == 1
	}, 15*time.Second, 50*time.Millisecond)
	settledClean(t, js, "POISON", "lane")
	time.Sleep(500 * time.Millisecond)
	drainBatch(t, c)

	msgs, errs := poison.snapshot()
	require.Len(t, msgs, 1)
	require.Equal(t, "bad", string(msgs[0].Data()))
	require.Equal(t, "bad", msgs[0].Headers().Get(jetstream.MsgIDHeader))
	require.ErrorIs(t, errs[0], errApply)
	require.EqualValues(t, 2, rows(t, db, "items"))
	require.EqualValues(t, 2, rows(t, db, "processed_messages"))
}

func TestFrameRecompositionIsDedupedByElementID(t *testing.T) {
	db, tx := dedupEnv(t)
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "RECOMPOSE")
	applied := newRecorder()
	_, err := consumer.New(consumer.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("lane", true).WithFilterSubject("RECOMPOSE.>"),
		MaxInFlight:         1,
		HandlerFunc: consumer.WithIdempotency(tx, nil, func(ctx context.Context, msg jetstream.Msg) error {
			applied.add(msg)
			return tx.GetConnection(ctx).Create(&item{Name: string(msg.Data())}).Error
		}),
	})
	require.NoError(t, err)

	publishFrame(t, js, "RECOMPOSE.cmd", "x1", "x2")
	publishFrame(t, js, "RECOMPOSE.cmd", "x2", "x3")

	require.EqualValues(t, 2, streamMsgs(t, js, "RECOMPOSE"))
	require.Eventually(t, func() bool { return rows(t, db, "items") == 3 }, 15*time.Second, 50*time.Millisecond)
	settledClean(t, js, "RECOMPOSE", "lane")
	require.Equal(t, 1, applied.callsOf("x2"))
	require.EqualValues(t, 3, rows(t, db, "processed_messages"))
}

func TestBrokenFrameGoesWholeToDLQ(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "BROKEN")
	batchPoison := &poisonLog{}
	singlePoison := &poisonLog{}
	batchConsumer, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("batch", true).WithFilterSubject("BROKEN.>"),
		Handler: func(context.Context, []jetstream.Msg) []batch.Result {
			t.Error("a broken frame must not reach the batch handler")
			return nil
		},
		OnPoison: batchPoison.fn,
	})
	require.NoError(t, err)
	_, err = consumer.New(consumer.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("single", true).WithFilterSubject("BROKEN.>"),
		HandlerFunc: func(context.Context, jetstream.Msg) error {
			t.Error("a broken frame must not reach the handler")
			return nil
		},
		OnPoison: singlePoison.fn,
	})
	require.NoError(t, err)

	msg := nats.NewMsg("BROKEN.cmd")
	msg.Header.Set(frame.HeaderVersion, frame.Version)
	msg.Header.Set(frame.HeaderCount, "2")
	msg.Data = []byte("not msgpack")
	_, err = js.PublishMsg(context.Background(), msg)
	require.NoError(t, err)

	settledClean(t, js, "BROKEN", "batch")
	settledClean(t, js, "BROKEN", "single")
	drainBatch(t, batchConsumer)
	for _, log := range []*poisonLog{batchPoison, singlePoison} {
		msgs, errs := log.snapshot()
		require.Len(t, msgs, 1)
		require.Equal(t, "not msgpack", string(msgs[0].Data()))
		require.ErrorIs(t, errs[0], frame.ErrBadFrame)
	}
}
