package stream

import (
	"context"

	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
)

type Opts struct {
	JetStream         jetstream.JetStream
	StreamOptsBuilder *streamoptsbuilder.StreamOptsBuilder
}

type Stream struct {
	stream jetstream.Stream
}

func New(opts Opts) (*Stream, error) {
	stream, err := opts.JetStream.CreateOrUpdateStream(
		context.Background(),
		opts.StreamOptsBuilder.Build(),
	)
	if err != nil {
		return nil, err
	}

	return &Stream{stream}, nil
}

// Bind attaches to an already-existing JetStream stream by name without
// creating or updating it.
//
// Use this when the stream is owned by another service (or pre-provisioned by
// infra) and this process only needs to attach consumers to it. Unlike New,
// Bind never calls CreateOrUpdateStream, so it cannot trigger a `subjects
// overlap with an existing stream` (10065) error and cannot mutate the
// stream's config (retention, subjects, limits).
//
// Returns an error if no stream with the given name exists.
func Bind(
	js jetstream.JetStream,
	name string,
) (*Stream, error) {
	stream, err := js.Stream(
		context.Background(),
		name,
	)
	if err != nil {
		return nil, err
	}

	return &Stream{stream}, nil
}

func (s *Stream) Name() string {
	return s.stream.CachedInfo().Config.Name
}
