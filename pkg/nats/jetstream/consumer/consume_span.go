package consumer

import (
	"context"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func startConsumeSpan(ctx context.Context, msg jetstream.Msg) (context.Context, trace.Span) {
	return tracenats.StartConsumer(
		ctx,
		msg.Subject(),
		msg.Headers(),
		consumeAttributes(msg)...,
	)
}

func consumeAttributes(msg jetstream.Msg) []attribute.KeyValue {
	var attrs []attribute.KeyValue

	if id := msg.Headers().Get(nats.MsgIdHdr); id != "" {
		attrs = append(
			attrs,
			attribute.String(
				"messaging.message.id",
				id,
			),
		)
	}

	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return attrs
	}

	return append(
		attrs,
		attribute.String(
			"messaging.nats.stream",
			meta.Stream,
		),
		attribute.String(
			"messaging.consumer.group.name",
			meta.Consumer,
		),
		attribute.Int64(
			"messaging.nats.sequence.stream",
			int64(meta.Sequence.Stream),
		),
		attribute.Int64(
			"messaging.nats.num_delivered",
			int64(meta.NumDelivered),
		),
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
