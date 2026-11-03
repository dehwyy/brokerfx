package outbox

import (
	"context"
	"strings"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var traceHeaderKeys = []string{
	"traceparent",
	"tracestate",
	"baggage",
}

func startRelaySpan(event relayEvent) (map[string]string, func(error)) {
	if !hasHeaderFold(
		event.Headers,
		"traceparent",
	) {
		return event.Headers, noopEnd
	}

	ctx := tracenats.ExtractMap(
		context.Background(),
		lowerTraceHeaders(event.Headers),
	)

	options := []trace.SpanStartOption{
		trace.WithAttributes(
			attribute.String(
				"messaging.message.id",
				event.ID,
			),
			attribute.Int(
				"outbox.attempts",
				event.Attempts,
			),
		),
	}
	if event.Attempts == 0 && !event.CreatedAt.IsZero() {
		options = append(
			options,
			trace.WithTimestamp(event.CreatedAt),
		)
	}

	ctx, span := tracenats.StartProducer(
		ctx,
		"outbox.relay",
		event.Topic,
		options...,
	)

	headers := make(
		map[string]string,
		len(event.Headers)+len(traceHeaderKeys),
	)
	for key, value := range event.Headers {
		if isTraceHeader(key) {
			continue
		}
		headers[key] = value
	}
	tracenats.InjectMap(
		ctx,
		headers,
	)

	return headers, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(
				codes.Error,
				err.Error(),
			)
		}
		span.End()
	}
}

func noopEnd(error) {}

func lowerTraceHeaders(headers map[string]string) map[string]string {
	lowered := make(
		map[string]string,
		len(traceHeaderKeys),
	)
	for key, value := range headers {
		if isTraceHeader(key) {
			lowered[strings.ToLower(key)] = value
		}
	}

	return lowered
}

func isTraceHeader(key string) bool {
	for _, traceKey := range traceHeaderKeys {
		if strings.EqualFold(
			key,
			traceKey,
		) {
			return true
		}
	}

	return false
}

func (r *OutboxRelay) tracedProducerEvent(event relayEvent) (ProducerEvent, func(error), error) {
	headers, end := startRelaySpan(event)
	event.Headers = headers

	producerEvent, err := r.producerEvent(event)
	if err != nil {
		end(err)
		return ProducerEvent{}, noopEnd, err
	}

	return producerEvent, end, nil
}
