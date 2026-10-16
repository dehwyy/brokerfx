package stream

import "github.com/nats-io/nats.go/jetstream"

func (s *Stream) Config() jetstream.StreamConfig {
	return s.stream.CachedInfo().Config
}
