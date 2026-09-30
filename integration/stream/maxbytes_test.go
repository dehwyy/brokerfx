//go:build integration

package stream_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/kv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

const mib = 1 << 20

func newStream(t *testing.T, js jetstream.JetStream, name string, maxBytes int64) {
	t.Helper()

	builder := streamoptsbuilder.NewDefault().
		WithName(name).
		WithSubjects([]string{name + ".>"})
	if maxBytes > 0 {
		builder = builder.WithMaxBytes(maxBytes)
	}

	_, err := stream.New(stream.Opts{JetStream: js, StreamOptsBuilder: builder})
	require.NoError(t, err)
}

func streamInfo(t *testing.T, js jetstream.JetStream, name string) *jetstream.StreamInfo {
	t.Helper()

	s, err := js.Stream(context.Background(), name)
	require.NoError(t, err)

	info, err := s.Info(context.Background())
	require.NoError(t, err)

	return info
}

func fill(t *testing.T, js jetstream.JetStream, name string, chunks int) {
	t.Helper()

	payload := bytes.Repeat([]byte("x"), 256*1024)
	for i := 0; i < chunks; i++ {
		_, err := js.Publish(context.Background(), name+".data", payload)
		require.NoError(t, err)
	}
}

func TestNewDefaultsToBuilderDefaultMaxBytes(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "")
	js := testenv.NATS(t)

	newStream(t, js, "MB_DEFAULT", 0)

	require.Equal(t, streamoptsbuilder.DefaultMaxBytes, streamInfo(t, js, "MB_DEFAULT").Config.MaxBytes)
	require.Equal(t, int64(256*mib), streamInfo(t, js, "MB_DEFAULT").Config.MaxBytes)
}

func TestNewUsesEnvMaxBytes(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "33554432")
	js := testenv.NATS(t)

	newStream(t, js, "MB_ENV", 0)

	require.Equal(t, int64(32*mib), streamInfo(t, js, "MB_ENV").Config.MaxBytes)
}

func TestNewExplicitMaxBytesBeatsEnv(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "33554432")
	js := testenv.NATS(t)

	newStream(t, js, "MB_EXPLICIT", 64*mib)

	require.Equal(t, int64(64*mib), streamInfo(t, js, "MB_EXPLICIT").Config.MaxBytes)
}

func TestNewInvalidEnvFailsStartup(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "lots")
	js := testenv.NATS(t)

	got, err := stream.New(stream.Opts{
		JetStream: js,
		StreamOptsBuilder: streamoptsbuilder.NewDefault().
			WithName("MB_INVALID").
			WithSubjects([]string{"MB_INVALID.>"}),
	})
	require.ErrorIs(t, err, streamoptsbuilder.ErrInvalidMaxBytesEnv)
	require.Nil(t, got)

	_, err = js.Stream(context.Background(), "MB_INVALID")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound)
}

func TestNewShrinksEmptyStream(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "")
	js := testenv.NATS(t)

	newStream(t, js, "MB_SHRINK_EMPTY", 2048*mib)
	require.Equal(t, int64(2048*mib), streamInfo(t, js, "MB_SHRINK_EMPTY").Config.MaxBytes)

	newStream(t, js, "MB_SHRINK_EMPTY", 0)

	require.Equal(t, int64(256*mib), streamInfo(t, js, "MB_SHRINK_EMPTY").Config.MaxBytes)
}

func TestNewShrinksWhenDataFitsHalf(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "")
	js := testenv.NATS(t)

	newStream(t, js, "MB_SHRINK_SMALL", 64*mib)
	fill(t, js, "MB_SHRINK_SMALL", 4)

	newStream(t, js, "MB_SHRINK_SMALL", 4*mib)

	info := streamInfo(t, js, "MB_SHRINK_SMALL")
	require.Equal(t, int64(4*mib), info.Config.MaxBytes)
	require.Equal(t, uint64(4), info.State.Msgs)
}

func TestNewRefusesShrinkWhenDataExceedsHalf(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "")
	js := testenv.NATS(t)

	newStream(t, js, "MB_REFUSE", 64*mib)
	fill(t, js, "MB_REFUSE", 16)
	before := streamInfo(t, js, "MB_REFUSE")
	require.Equal(t, uint64(16), before.State.Msgs)
	require.Greater(t, before.State.Bytes, uint64(2*mib))

	newStream(t, js, "MB_REFUSE", 4*mib)

	after := streamInfo(t, js, "MB_REFUSE")
	require.Equal(t, int64(64*mib), after.Config.MaxBytes)
	require.Equal(t, before.State.Msgs, after.State.Msgs)
	require.Equal(t, before.State.Bytes, after.State.Bytes)
}

func TestNewRefusesShrinkOfDefaultSizedStreamWithData(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "")
	js := testenv.NATS(t)

	newStream(t, js, "MB_REFUSE_LEGACY", 2048*mib)
	fill(t, js, "MB_REFUSE_LEGACY", 4)

	t.Setenv(streamoptsbuilder.EnvMaxBytes, "2097152")
	newStream(t, js, "MB_REFUSE_LEGACY", 0)

	info := streamInfo(t, js, "MB_REFUSE_LEGACY")
	require.Equal(t, int64(2048*mib), info.Config.MaxBytes)
	require.Equal(t, uint64(4), info.State.Msgs)
}

func TestNewRaisesMaxBytes(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "")
	js := testenv.NATS(t)

	newStream(t, js, "MB_RAISE", 8*mib)
	fill(t, js, "MB_RAISE", 8)

	newStream(t, js, "MB_RAISE", 64*mib)

	info := streamInfo(t, js, "MB_RAISE")
	require.Equal(t, int64(64*mib), info.Config.MaxBytes)
	require.Equal(t, uint64(8), info.State.Msgs)
}

func TestKVEnsureIgnoresStreamMaxBytesEnv(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "33554432")
	js := testenv.NATS(t)

	_, err := kv.Ensure(context.Background(), js, kv.Opts{Bucket: "MB_KV", Replicas: 1, History: 1})
	require.NoError(t, err)

	require.Equal(t, int64(-1), streamInfo(t, js, "KV_MB_KV").Config.MaxBytes)
}

func TestCreateOrUpdateGuardsRawConfig(t *testing.T) {
	js := testenv.NATS(t)

	cfg := jetstream.StreamConfig{
		Name:     "MB_RAW",
		Subjects: []string{"MB_RAW.>"},
		Storage:  jetstream.FileStorage,
		MaxBytes: 64 * mib,
	}
	_, err := stream.CreateOrUpdate(context.Background(), js, cfg)
	require.NoError(t, err)
	fill(t, js, "MB_RAW", 16)

	cfg.MaxBytes = 4 * mib
	_, err = stream.CreateOrUpdate(context.Background(), js, cfg)
	require.NoError(t, err)

	info := streamInfo(t, js, "MB_RAW")
	require.Equal(t, int64(64*mib), info.Config.MaxBytes)
	require.Equal(t, uint64(16), info.State.Msgs)
}
