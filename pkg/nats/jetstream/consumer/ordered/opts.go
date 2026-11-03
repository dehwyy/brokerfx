package ordered

import (
	"context"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/middleware"
	"github.com/nats-io/nats.go/jetstream"
)

type Opts struct {
	JetStream               jetstream.JetStream
	Stream                  string
	FilterSubjects          []string
	DeliverPolicy           jetstream.DeliverPolicy
	HandlerFunc             func(ctx context.Context, msg jetstream.Msg) error
	BeforeHandlerMiddleware []middleware.Middleware
	OnFatal                 func(error)
}
