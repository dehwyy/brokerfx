//go:build integration

package consumer_test

import (
	"context"
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

func TestConsumerReplicasOnWorkQueueStreamAreInherited(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	name := "REPLICAS"

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
	require.Equal(
		t,
		jetstream.WorkQueuePolicy,
		st.Config().Retention,
	)

	var handled atomic.Int32
	_, err = consumer.New(
		consumer.Opts{
			JetStream: js,
			Stream:    st,
			ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().
				WithName(
					name+"-c",
					true,
				).
				WithFilterSubject(name + ".>").
				WithReplicas(st.Config().Replicas + 2),
			HandlerFunc: func(context.Context, jetstream.Msg) error {
				handled.Add(1)
				return nil
			},
		},
	)
	require.NoError(
		t,
		err,
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
	require.Zero(
		t,
		durable.CachedInfo().Config.Replicas,
	)

	_, err = js.Publish(
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
			return handled.Load() == 1
		},
		5*time.Second,
		20*time.Millisecond,
	)
}
