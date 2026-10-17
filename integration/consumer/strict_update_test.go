//go:build integration

package consumer_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func strictStream(t *testing.T, js jetstream.JetStream, name string) *stream.Stream {
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

func captureLog(t *testing.T) *syncBuf {
	t.Helper()
	buf := &syncBuf{}
	prev := log.Logger
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = prev })
	return buf
}

func noop(context.Context, jetstream.Msg) error { return nil }

func TestStrictUpdateRejectsAckPolicyChange(t *testing.T) {
	buf := captureLog(t)
	js := testenv.NATS(t)
	st := strictStream(t, js, "STRICTACK")
	_, err := js.CreateConsumer(context.Background(), "STRICTACK", consumeroptsbuilder.NewDefault().WithName("worker", true).WithFilterSubject("STRICTACK.>").Build())
	require.NoError(t, err)

	changed := consumeroptsbuilder.NewDefault().WithName("worker", true).WithFilterSubject("STRICTACK.>").WithAckPolicy(jetstream.AckAllPolicy)

	_, err = consumer.New(consumer.Opts{JetStream: js, Stream: st, ConsumerOptsBuilder: changed, HandlerFunc: noop, StrictUpdate: true})
	require.ErrorIs(t, err, consumer.ErrUpdateRejected)
	require.Contains(t, err.Error(), "STRICTACK")
	require.Contains(t, err.Error(), "worker")
	require.Contains(t, err.Error(), "err_code 10012")
	require.NotContains(t, buf.String(), "consumer config update rejected")

	_, err = consumer.New(consumer.Opts{JetStream: js, Stream: st, ConsumerOptsBuilder: changed, HandlerFunc: noop})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "consumer config update rejected, using existing durable as is")

	live, err := js.Consumer(context.Background(), "STRICTACK", "worker")
	require.NoError(t, err)
	require.Equal(t, jetstream.AckExplicitPolicy, live.CachedInfo().Config.AckPolicy)
}

func TestStrictUpdateEnvTurnsItOff(t *testing.T) {
	t.Setenv("BROKERFX_CONSUMER_STRICT_UPDATE", "false")
	buf := captureLog(t)
	js := testenv.NATS(t)
	st := strictStream(t, js, "STRICTENV")
	_, err := js.CreateConsumer(context.Background(), "STRICTENV", consumeroptsbuilder.NewDefault().WithName("worker", true).WithFilterSubject("STRICTENV.>").Build())
	require.NoError(t, err)

	changed := consumeroptsbuilder.NewDefault().WithName("worker", true).WithFilterSubject("STRICTENV.>").WithMaxWaiting(7)
	_, err = consumer.New(consumer.Opts{JetStream: js, Stream: st, ConsumerOptsBuilder: changed, HandlerFunc: noop, StrictUpdate: true})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "consumer config update rejected")
}

func TestStrictUpdateRetriesWhileServerIsDown(t *testing.T) {
	buf := captureLog(t)
	js, control := testenv.EmbeddedNATSControl(t)
	st := strictStream(t, js, "STRICTDOWN")
	_, err := js.CreateConsumer(context.Background(), "STRICTDOWN", consumeroptsbuilder.NewDefault().WithName("worker", true).WithFilterSubject("STRICTDOWN.>").Build())
	require.NoError(t, err)

	short, err := jetstream.New(js.Conn(), jetstream.WithDefaultTimeout(300*time.Millisecond))
	require.NoError(t, err)

	control.Stop()
	go func() {
		time.Sleep(1500 * time.Millisecond)
		control.Start()
	}()

	started := time.Now()
	_, err = consumer.New(consumer.Opts{
		JetStream:           short,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("worker", true).WithFilterSubject("STRICTDOWN.>").WithAckWait(40 * time.Second),
		HandlerFunc:         noop,
		StrictUpdate:        true,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(started), time.Second)
	require.True(t, strings.Contains(buf.String(), "transient error, retrying"), buf.String())

	live, err := js.Consumer(context.Background(), "STRICTDOWN", "worker")
	require.NoError(t, err)
	require.Equal(t, 40*time.Second, live.CachedInfo().Config.AckWait)
}
