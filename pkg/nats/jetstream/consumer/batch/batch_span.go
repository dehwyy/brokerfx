package batch

import (
	"context"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func startBatchSpan(ctx context.Context, msgs []jetstream.Msg) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.String(
			"messaging.system",
			"nats",
		),
		attribute.Int(
			"messaging.batch.message_count",
			len(msgs),
		),
	}
	if group := batchGroup(msgs); group != "" {
		attrs = append(
			attrs,
			attribute.String(
				"messaging.consumer.group.name",
				group,
			),
		)
	}

	return otel.Tracer("github.com/dehwyy/brokerfx").Start(
		ctx,
		"nats.consume.batch",
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(batchLinks(msgs)...),
		trace.WithAttributes(attrs...),
	)
}

func batchGroup(msgs []jetstream.Msg) string {
	if len(msgs) == 0 {
		return ""
	}

	meta, err := msgs[0].Metadata()
	if err != nil || meta == nil {
		return ""
	}

	return meta.Consumer
}

func batchLinks(msgs []jetstream.Msg) []trace.Link {
	links := make(
		[]trace.Link,
		0,
		len(msgs),
	)
	for _, msg := range msgs {
		extracted := propagation.TraceContext{}.Extract(
			context.Background(),
			&tracenats.NATSHeaderCarrier{Header: msg.Headers()},
		)

		spanContext := trace.SpanContextFromContext(extracted)
		if !spanContext.IsValid() {
			continue
		}

		links = append(
			links,
			trace.Link{SpanContext: spanContext},
		)
	}

	return links
}

func endBatchSpan(span trace.Span, completed bool, failed int) {
	if failed > 0 {
		span.SetAttributes(attribute.Int(
			"messaging.batch.failed_count",
			failed,
		))
		span.SetStatus(
			codes.Error,
			"nack",
		)
	}
	if !completed {
		span.SetStatus(
			codes.Error,
			"panic",
		)
	}
	span.End()
}
