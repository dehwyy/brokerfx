//go:build integration

package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/kv"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestEnsureCreatesBucketWithConfiguredDuplicatesWindow(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)

	got, err := kv.Ensure(context.Background(), js, kv.Opts{
		Bucket:   "ENSURE1",
		Replicas: 1,
		History:  1,
	})
	require.NoError(t, err)
	require.NotNil(t, got)

	stream, err := js.Stream(context.Background(), "KV_ENSURE1")
	require.NoError(t, err)

	info, err := stream.Info(context.Background())
	require.NoError(t, err)
	require.Equal(t, 15*time.Minute, info.Config.Duplicates)
}

func TestEnsureIsIdempotentOnRepeatedCallWithSameOpts(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)

	opts := kv.Opts{
		Bucket:   "ENSURE2",
		Replicas: 1,
		History:  1,
	}

	_, err := kv.Ensure(context.Background(), js, opts)
	require.NoError(t, err)

	_, err = kv.Ensure(context.Background(), js, opts)
	require.NoError(t, err)

	stream, err := js.Stream(context.Background(), "KV_ENSURE2")
	require.NoError(t, err)

	info, err := stream.Info(context.Background())
	require.NoError(t, err)
	require.Equal(t, 15*time.Minute, info.Config.Duplicates)
	require.EqualValues(t, 1, info.Config.MaxMsgsPerSubject)
}

func TestEnsureRejectsZeroReplicas(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)

	_, err := kv.Ensure(context.Background(), js, kv.Opts{
		Bucket:   "ENSURE3",
		Replicas: 0,
		History:  1,
	})
	require.ErrorIs(t, err, kv.ErrReplicasRequired)

	_, err = js.Stream(context.Background(), "KV_ENSURE3")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound)
}

func TestEnsureUpdatesConfigOnRepeatedCallWithDifferentHistory(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)

	_, err := kv.Ensure(context.Background(), js, kv.Opts{
		Bucket:   "ENSURE4",
		Replicas: 1,
		History:  1,
	})
	require.NoError(t, err)

	_, err = kv.Ensure(context.Background(), js, kv.Opts{
		Bucket:   "ENSURE4",
		Replicas: 1,
		History:  5,
	})
	require.NoError(t, err)

	stream, err := js.Stream(context.Background(), "KV_ENSURE4")
	require.NoError(t, err)

	info, err := stream.Info(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 5, info.Config.MaxMsgsPerSubject)
	require.Equal(t, 15*time.Minute, info.Config.Duplicates)
}
