//go:build integration

package batch_test

import (
	"context"
	"testing"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func replicaOpts(
	js jetstream.JetStream,
	st *stream.Stream,
	replicas int,
	maxAckPending int,
) batch.Opts {
	b := builder(
		"replicas",
		"REPLICAS.>",
	)
	if maxAckPending != 0 {
		b = b.WithMaxAckPending(maxAckPending)
	}

	return batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: b,
		Max:                 8,
		Pipeline:            2,
		Replicas:            replicas,
		Handler: func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
			return ackAll(msgs)
		},
	}
}

func TestBatchReplicasRaiseMaxAckPendingAcrossRollingUpdate(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(
		t,
		js,
		"REPLICAS",
	)

	scaled, err := batch.New(replicaOpts(
		js,
		st,
		3,
		0,
	))
	require.NoError(
		t,
		err,
	)
	require.Equal(
		t,
		batch.MaxAckPendingFor(
			3,
			2,
			8,
		),
		consumerInfo(
			t,
			js,
			"REPLICAS",
			"replicas",
		).Config.MaxAckPending,
	)

	old, err := batch.New(replicaOpts(
		js,
		st,
		0,
		0,
	))
	require.NoError(
		t,
		err,
	)
	require.Equal(
		t,
		16,
		consumerInfo(
			t,
			js,
			"REPLICAS",
			"replicas",
		).Config.MaxAckPending,
	)

	again, err := batch.New(replicaOpts(
		js,
		st,
		3,
		0,
	))
	require.NoError(
		t,
		err,
	)
	require.Equal(
		t,
		72,
		consumerInfo(
			t,
			js,
			"REPLICAS",
			"replicas",
		).Config.MaxAckPending,
	)

	drain(
		t,
		again,
	)
	drain(
		t,
		old,
	)
	drain(
		t,
		scaled,
	)

	_, err = batch.New(replicaOpts(
		js,
		st,
		3,
		50,
	))
	require.ErrorIs(
		t,
		err,
		batch.ErrMaxAckPending,
	)
}
