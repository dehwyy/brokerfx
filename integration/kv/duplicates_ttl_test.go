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

func TestEnsureOnShortTTLBucketFitsDuplicatesIntoTTL(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	ctx := context.Background()

	_, err := js.CreateOrUpdateKeyValue(
		ctx,
		jetstream.KeyValueConfig{
			Bucket:   "ENSURE_TTL",
			TTL:      2 * time.Minute,
			Replicas: 1,
		},
	)
	require.NoError(
		t,
		err,
	)

	_, err = kv.Ensure(
		ctx,
		js,
		kv.Opts{
			Bucket:   "ENSURE_TTL",
			Replicas: 1,
		},
	)
	require.NoError(
		t,
		err,
	)

	stream, err := js.Stream(
		ctx,
		"KV_ENSURE_TTL",
	)
	require.NoError(
		t,
		err,
	)
	info, err := stream.Info(ctx)
	require.NoError(
		t,
		err,
	)
	require.Equal(
		t,
		2*time.Minute,
		info.Config.MaxAge,
	)
	require.Equal(
		t,
		2*time.Minute,
		info.Config.Duplicates,
	)
}
