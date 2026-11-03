package itemctx

import (
	"context"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"github.com/dehwyy/tracerfx/pkg/tracer/wire"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/baggage"
)

func WithBaggage(ctx context.Context, headers nats.Header) context.Context {
	if len(headers) == 0 {
		return ctx
	}

	extracted := wire.Propagator().Extract(
		context.Background(),
		&tracenats.NATSHeaderCarrier{Header: headers},
	)

	members := baggage.FromContext(extracted)
	if members.Len() == 0 {
		return ctx
	}

	return baggage.ContextWithBaggage(
		ctx,
		members,
	)
}
