//go:build integration

package batch_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

var errBusiness = errors.New("business failure")

func newStream(t *testing.T, js jetstream.JetStream, name string) *stream.Stream {
	t.Helper()
	_, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{name + ".>"},
		Storage:  jetstream.FileStorage,
	})
	require.NoError(t, err)
	bound, err := stream.Bind(js, name)
	require.NoError(t, err)
	return bound
}

func publish(t *testing.T, js jetstream.JetStream, subject string, data ...string) {
	t.Helper()
	for _, d := range data {
		_, err := js.Publish(context.Background(), subject, []byte(d))
		require.NoError(t, err)
	}
}

func builder(name, filter string) *consumeroptsbuilder.ConsumerOptsBuilder {
	return consumeroptsbuilder.NewDefault().WithName(name, true).WithFilterSubject(filter)
}

func consumerInfo(t *testing.T, js jetstream.JetStream, streamName, name string) *jetstream.ConsumerInfo {
	t.Helper()
	c, err := js.Consumer(context.Background(), streamName, name)
	require.NoError(t, err)
	info, err := c.Info(context.Background())
	require.NoError(t, err)
	return info
}

func drain(t *testing.T, c *batch.Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, c.Drain(ctx))
}

func ackAll(msgs []jetstream.Msg) []batch.Result {
	results := make([]batch.Result, len(msgs))
	for i := range results {
		results[i] = batch.Ack()
	}
	return results
}

type poisonLog struct {
	mu    sync.Mutex
	items []string
	errs  []error
	fail  atomic.Int32
}

func (p *poisonLog) fn(_ context.Context, msg jetstream.Msg, err error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items = append(p.items, string(msg.Data()))
	p.errs = append(p.errs, err)
	if p.fail.Load() > 0 {
		p.fail.Add(-1)
		return errors.New("dlq unavailable")
	}
	return nil
}

func (p *poisonLog) snapshot() ([]string, []error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.items...), append([]error(nil), p.errs...)
}

func TestBatchPoisonOnlyTerm(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "POISONONLY")
	data := make([]string, 10)
	for i := range data {
		data[i] = fmt.Sprintf("m%d", i)
	}
	data[4] = "poison"
	publish(t, js, "POISONONLY.x", data...)

	poison := &poisonLog{}
	var maxBatch atomic.Int32
	var handled atomic.Int32
	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("poison-only", "POISONONLY.>"),
		Max:                 16,
		Linger:              50 * time.Millisecond,
		OnPoison:            poison.fn,
		Handler: func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
			if int32(len(msgs)) > maxBatch.Load() {
				maxBatch.Store(int32(len(msgs)))
			}
			handled.Add(int32(len(msgs)))
			results := make([]batch.Result, len(msgs))
			for i, msg := range msgs {
				if string(msg.Data()) == "poison" {
					results[i] = batch.Term(errBusiness)
					continue
				}
				results[i] = batch.Ack()
			}
			return results
		},
	})
	require.NoError(t, err)
	defer drain(t, c)

	require.Eventually(t, func() bool {
		info := consumerInfo(t, js, "POISONONLY", "poison-only")
		return handled.Load() == 10 && info.NumAckPending == 0 && info.NumPending == 0
	}, 10*time.Second, 50*time.Millisecond)

	info := consumerInfo(t, js, "POISONONLY", "poison-only")
	require.Zero(t, info.NumRedelivered)
	items, errs := poison.snapshot()
	require.Equal(t, []string{"poison"}, items)
	require.ErrorIs(t, errs[0], errBusiness)
	require.Greater(t, maxBatch.Load(), int32(1))
}

func TestBatchNewRejectsMaxAckPending(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "MAXACK")

	_, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("maxack", "MAXACK.>").WithMaxAckPending(8),
		Max:                 16,
		Pipeline:            2,
		Handler:             func(_ context.Context, msgs []jetstream.Msg) []batch.Result { return ackAll(msgs) },
	})
	require.ErrorIs(t, err, batch.ErrMaxAckPending)
}

func TestBatchNewRejectsLowMaxDeliver(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "MAXDELIVER")

	_, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("maxdeliver", "MAXDELIVER.>").WithMaxDeliver(3),
		MaxAttempts:         3,
		Handler:             func(_ context.Context, msgs []jetstream.Msg) []batch.Result { return ackAll(msgs) },
	})
	require.ErrorIs(t, err, batch.ErrMaxDeliver)
}

func TestBatchNewSetsMaxAckPending(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "SETACK")

	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("setack", "SETACK.>"),
		Max:                 8,
		Pipeline:            2,
		Handler:             func(_ context.Context, msgs []jetstream.Msg) []batch.Result { return ackAll(msgs) },
	})
	require.NoError(t, err)
	defer drain(t, c)

	require.Equal(t, 16, consumerInfo(t, js, "SETACK", "setack").Config.MaxAckPending)
}

func TestBatchDrainReturnsFetched(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "DRAINFETCHED")
	data := make([]string, 12)
	for i := range data {
		data[i] = fmt.Sprintf("m%d", i)
	}
	publish(t, js, "DRAINFETCHED.x", data...)

	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("drain-fetched", "DRAINFETCHED.>").WithAckWait(30 * time.Second),
		Max:                 4,
		Linger:              20 * time.Millisecond,
		Handler: func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
			once.Do(func() { close(entered) })
			<-release
			return ackAll(msgs)
		},
	})
	require.NoError(t, err)

	<-entered
	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		drained <- c.Drain(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	require.NoError(t, <-drained)

	require.Eventually(t, func() bool {
		return consumerInfo(t, js, "DRAINFETCHED", "drain-fetched").NumAckPending == 0
	}, time.Second, 20*time.Millisecond)
}

func TestBatchHeartbeatKeepsLongBatch(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "HEARTBEAT")
	publish(t, js, "HEARTBEAT.x", "a", "b", "c")

	done := make(chan struct{})
	var once sync.Once
	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("heartbeat", "HEARTBEAT.>").WithAckWait(time.Second),
		Linger:              50 * time.Millisecond,
		Handler: func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
			time.Sleep(2500 * time.Millisecond)
			once.Do(func() { close(done) })
			return ackAll(msgs)
		},
	})
	require.NoError(t, err)
	defer drain(t, c)

	<-done
	require.Eventually(t, func() bool {
		info := consumerInfo(t, js, "HEARTBEAT", "heartbeat")
		return info.NumAckPending == 0 && info.NumPending == 0
	}, 10*time.Second, 50*time.Millisecond)
	require.Zero(t, consumerInfo(t, js, "HEARTBEAT", "heartbeat").NumRedelivered)
}

func TestBatchSurvivesServerRestart(t *testing.T) {
	js, restart := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "RESTART")

	var mu sync.Mutex
	seen := map[string]bool{}
	var fatal atomic.Int32
	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("restart", "RESTART.>"),
		OnFatal:             func(error) { fatal.Add(1) },
		Handler: func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
			mu.Lock()
			for _, msg := range msgs {
				seen[string(msg.Data())] = true
			}
			mu.Unlock()
			return ackAll(msgs)
		},
	})
	require.NoError(t, err)
	defer drain(t, c)

	publish(t, js, "RESTART.x", "before")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen["before"]
	}, 10*time.Second, 20*time.Millisecond)

	restart()

	require.Eventually(t, func() bool {
		_, err := js.Publish(context.Background(), "RESTART.x", []byte("after"))
		return err == nil
	}, 20*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen["after"]
	}, 30*time.Second, 50*time.Millisecond)
	require.Zero(t, fatal.Load())
}

func TestBatchLadderThenPoison(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "LADDER")
	publish(t, js, "LADDER.x", "retry", "poison")

	var mu sync.Mutex
	deliveries := map[string][]time.Time{}
	poison := &poisonLog{}
	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("ladder", "LADDER.>").WithMaxDeliver(10),
		Delays:              []time.Duration{50 * time.Millisecond, 100 * time.Millisecond},
		MaxAttempts:         3,
		OnPoison:            poison.fn,
		Handler: func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
			mu.Lock()
			defer mu.Unlock()
			results := make([]batch.Result, len(msgs))
			for i, msg := range msgs {
				deliveries[string(msg.Data())] = append(deliveries[string(msg.Data())], time.Now())
				if string(msg.Data()) == "poison" {
					results[i] = batch.Nak(batch.Poison(errBusiness))
					continue
				}
				results[i] = batch.Nak(errBusiness)
			}
			return results
		},
	})
	require.NoError(t, err)
	defer drain(t, c)

	require.Eventually(t, func() bool {
		items, _ := poison.snapshot()
		return len(items) == 2 && consumerInfo(t, js, "LADDER", "ladder").NumAckPending == 0
	}, 10*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	retry := deliveries["retry"]
	require.Len(t, retry, 3)
	require.GreaterOrEqual(t, retry[1].Sub(retry[0]), 50*time.Millisecond)
	require.GreaterOrEqual(t, retry[2].Sub(retry[1]), 100*time.Millisecond)
	require.Len(t, deliveries["poison"], 1)

	items, errs := poison.snapshot()
	require.ElementsMatch(t, []string{"retry", "poison"}, items)
	for i, item := range items {
		require.ErrorIs(t, errs[i], errBusiness)
		if item == "poison" {
			require.ErrorIs(t, errs[i], batch.ErrPoison)
		}
	}
}

func TestBatchOnPoisonFailureRedelivers(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "DLQDOWN")
	publish(t, js, "DLQDOWN.x", "bad")

	poison := &poisonLog{}
	poison.fail.Store(1)
	var deliveries atomic.Int32
	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("dlqdown", "DLQDOWN.>"),
		Delays:              []time.Duration{50 * time.Millisecond},
		MaxAttempts:         1,
		OnPoison:            poison.fn,
		Handler: func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
			deliveries.Add(int32(len(msgs)))
			results := make([]batch.Result, len(msgs))
			for i := range results {
				results[i] = batch.Nak(errBusiness)
			}
			return results
		},
	})
	require.NoError(t, err)
	defer drain(t, c)

	require.Eventually(t, func() bool {
		items, _ := poison.snapshot()
		return len(items) == 2 && consumerInfo(t, js, "DLQDOWN", "dlqdown").NumAckPending == 0
	}, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, int32(2), deliveries.Load())
}

func TestBatchDrainIdleIsPrompt(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "IDLE")

	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("idle", "IDLE.>"),
		Handler:             func(_ context.Context, msgs []jetstream.Msg) []batch.Result { return ackAll(msgs) },
	})
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.Drain(ctx))
	require.Less(t, time.Since(started), time.Second)
	require.NoError(t, c.Drain(ctx))
}

func TestBatchResumeFromOldDurable(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "RESUME")
	publish(t, js, "RESUME.x", "m1", "m2", "m3", "m4", "m5")

	missing, err := batch.ResumeSequence(context.Background(), js, "RESUME", "old")
	require.NoError(t, err)
	require.Zero(t, missing)

	old, err := js.CreateConsumer(context.Background(), "RESUME", builder("old", "RESUME.>").Build())
	require.NoError(t, err)
	fetched, err := old.Fetch(3, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, err)
	for msg := range fetched.Messages() {
		require.NoError(t, msg.DoubleAck(context.Background()))
	}

	start, err := batch.ResumeSequence(context.Background(), js, "RESUME", "old")
	require.NoError(t, err)
	require.Equal(t, uint64(4), start)

	var mu sync.Mutex
	var seen []string
	handler := func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range msgs {
			seen = append(seen, string(msg.Data()))
		}
		return ackAll(msgs)
	}

	c, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("resume-batch", "RESUME.>").WithDeliverByStartSequence(start),
		Handler:             handler,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 2
	}, 10*time.Second, 20*time.Millisecond)
	drain(t, c)
	require.Equal(t, []string{"m4", "m5"}, seen)

	again, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("resume-batch", "RESUME.>").WithDeliverByStartSequence(1).WithAckWait(20 * time.Second),
		Handler:             handler,
	})
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)
	drain(t, again)

	info := consumerInfo(t, js, "RESUME", "resume-batch")
	require.Equal(t, uint64(4), info.Config.OptStartSeq)
	require.Equal(t, 20*time.Second, info.Config.AckWait)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"m4", "m5"}, seen)
}
