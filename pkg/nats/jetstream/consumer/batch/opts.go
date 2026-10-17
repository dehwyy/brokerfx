package batch

import (
	"time"

	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	"github.com/nats-io/nats.go/jetstream"
)

type Opts struct {
	JetStream           jetstream.JetStream
	Stream              *stream.Stream
	ConsumerOptsBuilder *consumeroptsbuilder.ConsumerOptsBuilder
	Handler             Handler
	Max                 int
	Linger              time.Duration
	MinBatch            int
	LingerMax           time.Duration
	Pipeline            int
	Delays              []time.Duration
	MaxAttempts         int
	OnPoison            PoisonFunc
	OnFatal             func(error)
	Observer            Observer
}
