//go:build integration

package consumer_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startConsumer(
	t *testing.T,
	js jetstream.JetStream,
	name string,
	handler func(ctx context.Context, msg jetstream.Msg) error,
) {
	t.Helper()

	st, err := stream.New(stream.Opts{
		JetStream: js,
		StreamOptsBuilder: streamoptsbuilder.NewDefault().
			WithName(name).
			WithSubjects([]string{name + ".>"}),
	})
	require.NoError(t, err)

	_, err = consumer.New(consumer.Opts{
		JetStream: js,
		Stream:    st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().
			WithName(name+"-c", true).
			WithFilterSubject(name + ".>"),
		HandlerFunc: handler,
	})
	require.NoError(t, err)
}

func TestNakWithDelayThenErrorKeepsDelayAndLogsNoError(t *testing.T) {
	buf := &syncBuf{}
	prev := log.Logger
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = prev })

	js := testenv.NATS(t)
	name := "SETTLEDNAK"

	var mu sync.Mutex
	var times []time.Time
	var delivered []uint64
	var calls atomic.Int32

	startConsumer(t, js, name, func(_ context.Context, msg jetstream.Msg) error {
		meta, err := msg.Metadata()
		require.NoError(t, err)
		mu.Lock()
		times = append(times, time.Now())
		delivered = append(delivered, meta.NumDelivered)
		mu.Unlock()
		if calls.Add(1) == 1 {
			_ = msg.NakWithDelay(2 * time.Second)
			return errors.New("transient")
		}
		return nil
	})

	_, err := js.Publish(context.Background(), name+".x", []byte("p"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(times) >= 2
	}, 10*time.Second, 50*time.Millisecond)

	mu.Lock()
	require.GreaterOrEqual(t, times[1].Sub(times[0]), 1800*time.Millisecond)
	require.Equal(t, uint64(2), delivered[1])
	mu.Unlock()

	time.Sleep(300 * time.Millisecond)
	require.NotContains(t, buf.String(), "failed to NAK")
	require.NotContains(t, buf.String(), "failed to ACK")
}

func TestTermThenNilDoesNotRedeliverNorLogAckFailure(t *testing.T) {
	buf := &syncBuf{}
	prev := log.Logger
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = prev })

	js := testenv.NATS(t)
	name := "SETTLEDTERM"

	var calls atomic.Int32
	startConsumer(t, js, name, func(_ context.Context, msg jetstream.Msg) error {
		calls.Add(1)
		return msg.Term()
	})

	_, err := js.Publish(context.Background(), name+".x", []byte("p"))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return calls.Load() >= 1 }, 10*time.Second, 50*time.Millisecond)
	time.Sleep(3 * time.Second)

	require.Equal(t, int32(1), calls.Load())
	require.False(t, strings.Contains(buf.String(), "failed to ACK"))
}
