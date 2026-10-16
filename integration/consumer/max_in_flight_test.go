//go:build integration

package consumer_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type deliveries struct {
	mu    sync.Mutex
	seen  map[string][]uint64
	times map[string][]time.Time
}

func newDeliveries() *deliveries {
	return &deliveries{
		seen:  map[string][]uint64{},
		times: map[string][]time.Time{},
	}
}

func (d *deliveries) add(
	t *testing.T,
	msg jetstream.Msg,
) uint64 {
	metadata, err := msg.Metadata()
	require.NoError(
		t,
		err,
	)

	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[msg.Subject()] = append(
		d.seen[msg.Subject()],
		metadata.NumDelivered,
	)
	d.times[msg.Subject()] = append(
		d.times[msg.Subject()],
		time.Now(),
	)

	return metadata.NumDelivered
}

func (d *deliveries) total() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, numbers := range d.seen {
		count += len(numbers)
	}
	return count
}

func (d *deliveries) of(subject string) ([]uint64, []time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append(
			[]uint64(nil),
			d.seen[subject]...,
		), append(
			[]time.Time(nil),
			d.times[subject]...,
		)
}

func boundedConsumer(
	t *testing.T,
	js jetstream.JetStream,
	name string,
	opts consumer.Opts,
) *consumer.Consumer {
	t.Helper()

	st, err := stream.New(
		stream.Opts{
			JetStream: js,
			StreamOptsBuilder: streamoptsbuilder.NewDefault().
				WithName(name).
				WithSubjects([]string{name + ".>"}),
		},
	)
	require.NoError(
		t,
		err,
	)

	opts.JetStream = js
	opts.Stream = st
	opts.ConsumerOptsBuilder = consumeroptsbuilder.NewDefault().
		WithName(
			name+"-c",
			true,
		).
		WithFilterSubject(name + ".>").
		WithAckWait(2 * time.Second)

	created, err := consumer.New(opts)
	require.NoError(
		t,
		err,
	)

	return created
}

func TestMaxInFlightSlowHandlerIsNotRedelivered(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	name := "INFLIGHT"
	sink := newDeliveries()

	var running, peak atomic.Int32
	created := boundedConsumer(
		t,
		js,
		name,
		consumer.Opts{
			MaxInFlight: 1,
			HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
				now := running.Add(1)
				defer running.Add(-1)
				for {
					seen := peak.Load()
					if now <= seen || peak.CompareAndSwap(
						seen,
						now,
					) {
						break
					}
				}
				sink.add(
					t,
					msg,
				)
				time.Sleep(2500 * time.Millisecond)
				return nil
			},
		},
	)

	const total = 3
	for i := range total {
		_, err := js.Publish(
			context.Background(),
			name+".m"+string(rune('a'+i)),
			[]byte("p"),
		)
		require.NoError(
			t,
			err,
		)
	}

	drainer := consumer.NewDrainer()
	drainer.Append(*created)
	require.Eventually(
		t,
		func() bool {
			return sink.total() >= total
		},
		20*time.Second,
		50*time.Millisecond,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()
	require.NoError(
		t,
		drainer.DrainContext(ctx),
	)

	require.Equal(
		t,
		total,
		sink.total(),
	)
	for i := range total {
		numbers, _ := sink.of(name + ".m" + string(rune('a'+i)))
		require.Equal(
			t,
			[]uint64{1},
			numbers,
		)
	}
	require.Equal(
		t,
		int32(1),
		peak.Load(),
	)

	durable, err := js.Consumer(
		context.Background(),
		name,
		name+"-c",
	)
	require.NoError(
		t,
		err,
	)
	state, err := durable.Info(context.Background())
	require.NoError(
		t,
		err,
	)
	require.Zero(
		t,
		state.NumRedelivered,
	)
	require.Zero(
		t,
		state.NumAckPending,
	)
}

func TestNakBackoffDelaysRedelivery(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	name := "NAKBACKOFF"
	sink := newDeliveries()

	boundedConsumer(
		t,
		js,
		name,
		consumer.Opts{
			NakBackoff: []time.Duration{
				1500 * time.Millisecond,
			},
			HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
				if sink.add(
					t,
					msg,
				) == 1 {
					return errors.New("transient")
				}
				return nil
			},
		},
	)

	_, err := js.Publish(
		context.Background(),
		name+".x",
		[]byte("p"),
	)
	require.NoError(
		t,
		err,
	)

	require.Eventually(
		t,
		func() bool {
			return sink.total() >= 2
		},
		10*time.Second,
		50*time.Millisecond,
	)

	numbers, times := sink.of(name + ".x")
	require.Equal(
		t,
		[]uint64{
			1,
			2,
		},
		numbers,
	)
	require.GreaterOrEqual(
		t,
		times[1].Sub(times[0]),
		1400*time.Millisecond,
	)
}
