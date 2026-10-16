//go:build integration

package stream_test

import (
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/stretchr/testify/require"
)

func TestShortMaxAgeStreamGetsDuplicatesWithinMaxAge(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	name := "DUPES_SHORT"

	_, err := stream.New(
		stream.Opts{
			JetStream: js,
			StreamOptsBuilder: streamoptsbuilder.NewDefault().
				WithName(name).
				WithSubjects([]string{name + ".>"}).
				WithMaxAge(2 * time.Minute),
		},
	)
	require.NoError(
		t,
		err,
	)

	info := streamInfo(
		t,
		js,
		name,
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
