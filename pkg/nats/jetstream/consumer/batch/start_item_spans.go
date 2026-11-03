package batch

import (
	"context"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/consumeattr"
	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

func startItemSpans(batchSpan trace.Span, msgs []jetstream.Msg) itemSpans {
	spans := make(
		itemSpans,
		len(msgs),
	)
	for i, msg := range msgs {
		_, span := tracenats.StartConsumer(
			context.Background(),
			msg.Subject(),
			msg.Headers(),
			consumeattr.Attributes(msg)...,
		)
		span.AddLink(trace.Link{SpanContext: batchSpan.SpanContext()})
		spans[i] = span
	}

	return spans
}
