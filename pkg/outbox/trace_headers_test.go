package outbox

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dehwyy/tracerfx/pkg/tracer/correlate"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func tracedContext(t *testing.T) (context.Context, string) {
	t.Helper()

	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	ctx, span := provider.Tracer("test").Start(
		t.Context(),
		"save",
	)
	t.Cleanup(func() {
		span.End()
	})

	return correlate.WithTxID(
		ctx,
		"tx-42",
	), span.SpanContext().TraceID().String()
}

func TestWithTraceHeadersAddsContext(t *testing.T) {
	ctx, traceID := tracedContext(t)

	got := withTraceHeaders(
		ctx,
		map[string]string{"X-Own": "1"},
		true,
	)

	if !strings.Contains(got["traceparent"], traceID) {
		t.Fatalf("traceparent must carry trace id %s: %v", traceID, got)
	}
	if !strings.Contains(got["baggage"], "pl.tx_id=tx-42") {
		t.Fatalf("baggage must carry pl.tx_id: %v", got)
	}
	if got["X-Own"] != "1" {
		t.Fatalf("existing headers must survive: %v", got)
	}
}

func TestWithTraceHeadersDoesNotMutateInput(t *testing.T) {
	ctx, _ := tracedContext(t)
	input := map[string]string{"X-Own": "1"}

	withTraceHeaders(
		ctx,
		input,
		true,
	)

	if len(input) != 1 {
		t.Fatalf("input must not be mutated: %v", input)
	}
}

func TestWithTraceHeadersLegacySchemaSkips(t *testing.T) {
	ctx, _ := tracedContext(t)

	got := withTraceHeaders(
		ctx,
		nil,
		false,
	)
	if len(got) != 0 {
		t.Fatalf("legacy schema must not get headers: %v", got)
	}

	if _, err := buildMessageRows(
		[]Message{{Subject: "a"}},
		got,
		false,
		time.Now(),
	); err != nil {
		t.Fatalf("legacy schema must not fail: %v", err)
	}
}

func TestWithTraceHeadersKeepsCallerValue(t *testing.T) {
	ctx, _ := tracedContext(t)

	got := withTraceHeaders(
		ctx,
		map[string]string{"Traceparent": "x"},
		true,
	)

	if got["Traceparent"] != "x" {
		t.Fatalf("caller value must be kept: %v", got)
	}
	if _, ok := got["traceparent"]; ok {
		t.Fatalf("lowercase key must not be added next to caller key: %v", got)
	}
	if !strings.Contains(got["baggage"], "pl.tx_id=tx-42") {
		t.Fatalf("other keys still injected: %v", got)
	}
}

func TestWithTraceHeadersNoSpan(t *testing.T) {
	input := map[string]string{"X-Own": "1"}

	got := withTraceHeaders(
		context.Background(),
		input,
		true,
	)

	if len(got) != 1 || got["X-Own"] != "1" {
		t.Fatalf("input must pass through: %v", got)
	}

	if withTraceHeaders(
		context.Background(),
		nil,
		true,
	) != nil {
		t.Fatal("nil input without span stays nil")
	}
}
