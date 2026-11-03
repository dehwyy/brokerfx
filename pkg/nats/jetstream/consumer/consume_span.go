package consumer

import (
	"context"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/consumeattr"
	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func startConsumeSpan(ctx context.Context, msg jetstream.Msg) (context.Context, trace.Span) {
	return tracenats.StartConsumer(
		ctx,
		msg.Subject(),
		msg.Headers(),
		consumeattr.Attributes(msg)...,
	)
}

func endConsumeSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(
			codes.Error,
			err.Error(),
		)
	}
	span.End()
}
