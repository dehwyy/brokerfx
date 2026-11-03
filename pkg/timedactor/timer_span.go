package timedactor

import (
	"context"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

func startTimerSpan(key string, stored map[string]string) (context.Context, trace.Span) {
	extracted := tracenats.ExtractMap(
		context.Background(),
		stored,
	)

	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithAttributes(
			attribute.String(
				"timer.key",
				key,
			),
		),
	}

	if scheduler := trace.SpanContextFromContext(extracted); scheduler.IsValid() {
		opts = append(
			opts,
			trace.WithLinks(
				trace.Link{
					SpanContext: scheduler,
				},
			),
		)
	}

	return otel.Tracer("github.com/dehwyy/brokerfx").Start(
		extracted,
		"timer.fire",
		opts...,
	)
}

func timerCallbackContext(
	parent context.Context,
	spanCtx context.Context,
	span trace.Span,
) context.Context {
	return trace.ContextWithSpan(
		baggage.ContextWithBaggage(
			parent,
			baggage.FromContext(spanCtx),
		),
		span,
	)
}
