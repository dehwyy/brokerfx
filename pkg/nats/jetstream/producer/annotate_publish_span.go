package producer

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func annotatePublishSpan(ctx context.Context, msgID string) {
	if msgID == "" {
		return
	}

	trace.SpanFromContext(ctx).SetAttributes(attribute.String(
		"messaging.message.id",
		msgID,
	))
}
