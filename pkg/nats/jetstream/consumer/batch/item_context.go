package batch

import (
	"context"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/itemctx"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

func ItemContext(ctx context.Context, msg jetstream.Msg) context.Context {
	itemCtx := itemctx.WithBaggage(
		ctx,
		msg.Headers(),
	)

	carrier, ok := msg.(itemSpanCarrier)
	if !ok {
		return itemCtx
	}

	spanContext := carrier.itemSpanContext()
	if !spanContext.IsValid() {
		return itemCtx
	}

	return trace.ContextWithSpanContext(
		itemCtx,
		spanContext,
	)
}
