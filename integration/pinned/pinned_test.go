//go:build integration

package pinned_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

const group = "lanes"

func newStream(t *testing.T, js jetstream.JetStream, name string, retention jetstream.RetentionPolicy) *stream.Stream {
	t.Helper()
	_, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{name + ".>"},
		Storage:   jetstream.FileStorage,
		Retention: retention,
	})
	require.NoError(t, err)
	bound, err := stream.Bind(js, name)
	require.NoError(t, err)
	return bound
}

func publish(t *testing.T, js jetstream.JetStream, subject string, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		_, err := js.Publish(context.Background(), subject, []byte(strconv.Itoa(i)), jetstream.WithMsgID(subject+"-"+strconv.Itoa(i)))
		require.NoError(t, err)
	}
}

func partitions(t *testing.T, name string, ackWait, ttl time.Duration, count int) []jetstream.ConsumerConfig {
	t.Helper()
	configs, err := consumeroptsbuilder.PartitionConfigs(consumeroptsbuilder.Partitions{
		Base:      consumeroptsbuilder.NewDefault().WithName("lane", true).WithAckWait(ackWait).Build(),
		Count:     count,
		Filters:   func(i int) []string { return []string{name + ".cmd.p" + strconv.Itoa(i)} },
		Group:     group,
		PinnedTTL: ttl,
	})
	require.NoError(t, err)
	return configs
}

type ledger struct {
	mu      sync.Mutex
	applied map[string]int
	byPod   map[string]int
	dups    int
	redeliv int
}

func newLedger() *ledger {
	return &ledger{applied: map[string]int{}, byPod: map[string]int{}}
}

func (l *ledger) apply(pod string, msg jetstream.Msg) {
	l.mu.Lock()
	defer l.mu.Unlock()
	id := msg.Headers().Get(jetstream.MsgIDHeader)
	if meta, err := msg.Metadata(); err == nil && meta.NumDelivered > 1 {
		l.redeliv++
	}
	l.byPod[pod]++
	if l.applied[id] > 0 {
		l.dups++
		return
	}
	l.applied[id]++
}

func (l *ledger) handler(pod string) batch.Handler {
	return func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
		results := make([]batch.Result, len(msgs))
		for i, msg := range msgs {
			l.apply(pod, msg)
			results[i] = batch.Ack()
		}
		return results
	}
}

func (l *ledger) snapshot() (int, map[string]int, int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pods := map[string]int{}
	for pod, n := range l.byPod {
		pods[pod] = n
	}
	return len(l.applied), pods, l.dups, l.redeliv
}

func startBatch(t *testing.T, js jetstream.JetStream, st *stream.Stream, cfg jetstream.ConsumerConfig, handler batch.Handler) *batch.Consumer {
	t.Helper()
	c, err := batch.New(batch.Opts{JetStream: js, Stream: st, ConsumerOptsBuilder: consumeroptsbuilder.FromConfig(cfg), Handler: handler, StrictUpdate: true})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, c.Drain(ctx))
	})
	return c
}

func TestPartitionDurablesPinOneOfTwoBatchConsumers(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "PIN", jetstream.WorkQueuePolicy)
	configs := partitions(t, "PIN", 2*time.Second, 8*time.Second, 2)
	require.Equal(t, "lane-p0", configs[0].Durable)
	require.Equal(t, "lane-p1", configs[1].Durable)

	first := newLedger()
	second := newLedger()
	startBatch(t, js, st, configs[0], first.handler("a"))
	startBatch(t, js, st, configs[0], first.handler("b"))
	startBatch(t, js, st, configs[1], second.handler("c"))

	publish(t, js, "PIN.cmd.p0", 0, 30)
	publish(t, js, "PIN.cmd.p1", 0, 10)

	require.Eventually(t, func() bool {
		applied, _, _, _ := first.snapshot()
		other, _, _, _ := second.snapshot()
		return applied == 30 && other == 10
	}, 15*time.Second, 20*time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	_, pods, dups, _ := first.snapshot()
	require.Zero(t, dups)
	require.Len(t, pods, 1, "one pod holds the partition: %v", pods)

	info, err := js.Consumer(context.Background(), "PIN", "lane-p0")
	require.NoError(t, err)
	cfg := info.CachedInfo().Config
	require.Equal(t, jetstream.PriorityPolicyPinned, cfg.PriorityPolicy)
	require.Equal(t, []string{group}, cfg.PriorityGroups)
	require.Equal(t, jetstream.AckExplicitPolicy, cfg.AckPolicy)
}

func TestPinMovesAfterTTLAndTheDuplicateIsCaughtByDedup(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "MOVE", jetstream.WorkQueuePolicy)
	configs := partitions(t, "MOVE", time.Second, 7*time.Second, 1)
	_, err := js.CreateOrUpdateConsumer(context.Background(), "MOVE", configs[0])
	require.NoError(t, err)
	publish(t, js, "MOVE.cmd.p0", 0, 5)

	book := newLedger()
	raw, err := js.Consumer(context.Background(), "MOVE", "lane-p0")
	require.NoError(t, err)
	held, err := raw.Fetch(5, jetstream.FetchPriorityGroup(group), jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)
	for msg := range held.Messages() {
		book.apply("crashed", msg)
	}
	applied, _, _, _ := book.snapshot()
	require.Equal(t, 5, applied)
	pinnedAt := time.Now()

	startBatch(t, js, st, configs[0], book.handler("next"))
	publish(t, js, "MOVE.cmd.p0", 5, 10)

	time.Sleep(3 * time.Second)
	_, pods, _, _ := book.snapshot()
	require.Zero(t, pods["next"], "the crashed pod keeps the pin until PinnedTTL")

	require.Eventually(t, func() bool {
		applied, _, _, _ := book.snapshot()
		return applied == 10
	}, 20*time.Second, 50*time.Millisecond)
	require.GreaterOrEqual(t, time.Since(pinnedAt), 6*time.Second)
	time.Sleep(2 * time.Second)

	applied, pods, dups, redelivered := book.snapshot()
	require.Equal(t, 10, applied)
	require.Equal(t, 5, dups, "the held messages come back to the new pin")
	require.Equal(t, 5, redelivered)
	require.Equal(t, 10, pods["next"])
}

func TestPerMessageConsumerHonoursThePin(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "ONE", jetstream.WorkQueuePolicy)
	configs := partitions(t, "ONE", 5*time.Second, 40*time.Second, 1)

	book := newLedger()
	for _, pod := range []string{"a", "b"} {
		_, err := consumer.New(consumer.Opts{
			JetStream:           js,
			Stream:              st,
			ConsumerOptsBuilder: consumeroptsbuilder.FromConfig(configs[0]),
			StrictUpdate:        true,
			HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
				book.apply(pod, msg)
				return nil
			},
		})
		require.NoError(t, err)
	}

	publish(t, js, "ONE.cmd.p0", 0, 20)
	require.Eventually(t, func() bool {
		applied, _, _, _ := book.snapshot()
		return applied == 20
	}, 15*time.Second, 20*time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	_, pods, dups, _ := book.snapshot()
	require.Zero(t, dups)
	require.Len(t, pods, 1, "one consumer holds the pin: %v", pods)
}

func TestPinnedTTLBelowAckWaitAndIdlePollFailsBatchNew(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "TTL", jetstream.WorkQueuePolicy)
	configs := partitions(t, "TTL", 2*time.Second, 6*time.Second, 1)
	_, err := batch.New(batch.Opts{JetStream: js, Stream: st, ConsumerOptsBuilder: consumeroptsbuilder.FromConfig(configs[0]), Handler: newLedger().handler("a")})
	require.ErrorIs(t, err, batch.ErrInvalidOpts)
	require.ErrorIs(t, err, consumer.ErrInvalidPinned)
}

func TestQ78PriorityPolicyNoneToPinnedUpdatesInPlace(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "Q78", jetstream.WorkQueuePolicy)
	plain := func() *consumeroptsbuilder.ConsumerOptsBuilder {
		return consumeroptsbuilder.NewDefault().WithName("lane", true).WithAckWait(2 * time.Second).WithFilterSubject("Q78.cmd")
	}

	book := newLedger()
	before, err := batch.New(batch.Opts{JetStream: js, Stream: st, ConsumerOptsBuilder: plain(), Handler: book.handler("old")})
	require.NoError(t, err)
	publish(t, js, "Q78.cmd", 0, 3)
	require.Eventually(t, func() bool {
		applied, _, _, _ := book.snapshot()
		return applied == 3
	}, 10*time.Second, 20*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, before.Drain(ctx))
	publish(t, js, "Q78.cmd", 3, 5)

	pinned := plain().WithPinnedGroup(group, 8*time.Second).Build()
	startBatch(t, js, st, pinned, book.handler("new"))

	info, err := js.Consumer(context.Background(), "Q78", "lane")
	require.NoError(t, err)
	cfg := info.CachedInfo().Config
	require.Equal(t, jetstream.PriorityPolicyPinned, cfg.PriorityPolicy)
	require.Equal(t, []string{group}, cfg.PriorityGroups)
	require.Equal(t, 8*time.Second, cfg.PinnedTTL)

	require.Eventually(t, func() bool {
		applied, _, _, _ := book.snapshot()
		return applied == 5
	}, 10*time.Second, 20*time.Millisecond)
	_, pods, dups, _ := book.snapshot()
	require.Zero(t, dups)
	require.Equal(t, 2, pods["new"])

	back, err := js.UpdateConsumer(context.Background(), "Q78", plain().Build())
	require.NoError(t, err)
	require.Equal(t, jetstream.PriorityPolicyNone, back.CachedInfo().Config.PriorityPolicy)
}
