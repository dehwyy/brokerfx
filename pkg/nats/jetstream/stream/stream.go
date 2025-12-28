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

func New(opts Opts) *Stream {
	stream, err := opts.JetStream.CreateOrUpdateStream(
		context.Background(),
		opts.StreamOptsBuilder.Build(),
	)
	if err != nil {
		panic(err)
	}

	return &Stream{stream}
}

func (s *Stream) Name() string {
	return s.stream.CachedInfo().Config.Name
}
