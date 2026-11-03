package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dehwyy/tracerfx/pkg/tracer/correlate"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type relaySpanFixture struct {
	recorder *tracetest.SpanRecorder
	parent   trace.SpanContext
	headers  map[string]string
}

func newRelaySpanFixture(t *testing.T) relaySpanFixture {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
	})

	ctx, parent := provider.Tracer("test").Start(
		context.Background(),
		"save",
	)
	ctx = correlate.WithTxID(
		ctx,
		"tx-relay",
	)
	headers := withTraceHeaders(
		ctx,
		map[string]string{"X-Own": "1"},
		true,
	)
	parent.End()

	return relaySpanFixture{
		recorder: recorder,
		parent:   parent.SpanContext(),
		headers:  headers,
	}
}

func (f relaySpanFixture) relaySpan(t *testing.T) sdktrace.ReadOnlySpan {
	t.Helper()

	for _, span := range f.recorder.Ended() {
		if span.Name() == "outbox.relay" {
			return span
		}
	}

	t.Fatal("outbox.relay span not recorded")

	return nil
}

func attributeValue(span sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}

	return attribute.Value{}, false
}

func TestRelaySpanContinuesStoredTrace(t *testing.T) {
	fixture := newRelaySpanFixture(t)
	createdAt := time.Now().Add(-3 * time.Second).Truncate(time.Microsecond)

	_, end := startRelaySpan(relayEvent{
		ID:        "event-1",
		Topic:     "orders.created",
		Headers:   fixture.headers,
		CreatedAt: createdAt,
	})
	end(nil)

	span := fixture.relaySpan(t)
	if span.Parent().SpanID() != fixture.parent.SpanID() || span.SpanContext().TraceID() != fixture.parent.TraceID() {
		t.Fatalf("relay span must continue stored trace: parent=%v", span.Parent())
	}
	if !span.StartTime().Equal(createdAt) {
		t.Fatalf("start must equal created_at: %v vs %v", span.StartTime(), createdAt)
	}
	if span.SpanKind() != trace.SpanKindProducer {
		t.Fatalf("kind: %v", span.SpanKind())
	}
	if value, ok := attributeValue(
		span,
		"pl.tx_id",
	); !ok || value.AsString() != "tx-relay" {
		t.Fatalf("pl.tx_id attribute missing: %v", span.Attributes())
	}
	if value, ok := attributeValue(
		span,
		"messaging.message.id",
	); !ok || value.AsString() != "event-1" {
		t.Fatalf("messaging.message.id: %v", span.Attributes())
	}
}

func TestRelaySpanReplacesTraceparent(t *testing.T) {
	fixture := newRelaySpanFixture(t)

	headers, end := startRelaySpan(relayEvent{
		ID:      "event-1",
		Topic:   "orders.created",
		Headers: fixture.headers,
	})
	end(nil)

	span := fixture.relaySpan(t)
	want := "00-" + span.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String() + "-01"
	if headers["traceparent"] != want {
		t.Fatalf("traceparent must point to relay span: %s want %s", headers["traceparent"], want)
	}
	if headers["traceparent"] == fixture.headers["traceparent"] {
		t.Fatal("traceparent not replaced")
	}
	if headers["X-Own"] != "1" || headers["baggage"] == "" {
		t.Fatalf("other headers must survive: %v", headers)
	}
}

func TestRelaySpanWithoutContextRecordsNothing(t *testing.T) {
	fixture := newRelaySpanFixture(t)
	input := map[string]string{"X-Own": "1"}

	headers, end := startRelaySpan(relayEvent{
		ID:      "event-1",
		Topic:   "orders.created",
		Headers: input,
	})
	end(errors.New("ignored"))

	if len(headers) != 1 || headers["X-Own"] != "1" {
		t.Fatalf("headers must pass through: %v", headers)
	}
	for _, span := range fixture.recorder.Ended() {
		if span.Name() == "outbox.relay" {
			t.Fatal("no span without stored context")
		}
	}
}

func TestRelaySpanEndWithError(t *testing.T) {
	fixture := newRelaySpanFixture(t)

	_, end := startRelaySpan(relayEvent{
		ID:      "event-1",
		Topic:   "orders.created",
		Headers: fixture.headers,
	})
	end(errors.New("publish failed"))

	span := fixture.relaySpan(t)
	if span.Status().Code != codes.Error {
		t.Fatalf("status: %v", span.Status())
	}
	if len(span.Events()) != 1 || span.Events()[0].Name != "exception" {
		t.Fatalf("error must be recorded: %v", span.Events())
	}
}

func TestRelaySpanCarriesAttempts(t *testing.T) {
	fixture := newRelaySpanFixture(t)

	_, end := startRelaySpan(relayEvent{
		ID:       "event-1",
		Topic:    "orders.created",
		Headers:  fixture.headers,
		Attempts: 2,
	})
	end(nil)

	value, ok := attributeValue(fixture.relaySpan(t), "outbox.attempts")
	if !ok || value.AsInt64() != 2 {
		t.Fatalf("outbox.attempts: %v", value)
	}
}

func TestRelaySpanDoesNotMutateEvent(t *testing.T) {
	fixture := newRelaySpanFixture(t)
	original := fixture.headers["traceparent"]

	_, end := startRelaySpan(relayEvent{
		ID:      "event-1",
		Topic:   "orders.created",
		Headers: fixture.headers,
	})
	end(nil)

	if fixture.headers["traceparent"] != original {
		t.Fatal("event headers mutated")
	}
}
