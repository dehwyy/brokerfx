//go:build integration

package stream_test

import (
	"context"
	"testing"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func roleStream(
	t *testing.T,
	js jetstream.JetStream,
	name string,
	role streamoptsbuilder.Role,
) {
	t.Helper()

	_, err := stream.New(
		stream.Opts{
			JetStream: js,
			StreamOptsBuilder: streamoptsbuilder.NewDefault().
				WithName(name).
				WithSubjects([]string{name + ".>"}).
				WithRole(role),
		},
	)
	require.NoError(
		t,
		err,
	)
}

func TestS2StreamSwitchedToCommandRoleDropsCompression(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	name := "COMPRESSION_CMD"

	roleStream(
		t,
		js,
		name,
		streamoptsbuilder.RoleEvent,
	)
	require.Equal(
		t,
		jetstream.S2Compression,
		streamInfo(
			t,
			js,
			name,
		).Config.Compression,
	)

	_, err := js.Publish(
		context.Background(),
		name+".x",
		[]byte("before"),
	)
	require.NoError(
		t,
		err,
	)

	roleStream(
		t,
		js,
		name,
		streamoptsbuilder.RoleCommand,
	)
	info := streamInfo(
		t,
		js,
		name,
	)
	require.Equal(
		t,
		jetstream.NoCompression,
		info.Config.Compression,
	)
	require.Equal(
		t,
		uint64(1),
		info.State.Msgs,
	)
}
