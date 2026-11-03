package timedactor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordTimerSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
	})

	return recorder
}

func schedulerContext(t *testing.T) (context.Context, trace.SpanContext) {
	t.Helper()

	ctx, span := otel.Tracer("test").Start(
		context.Background(),
		"scheduler",
	)
	t.Cleanup(func() {
		span.End()
	})

	member, err := baggage.NewMember(
		"pl.tx_id",
		"tx-77",
	)
	if err != nil {
		t.Fatalf("baggage member: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("baggage: %v", err)
	}

	return baggage.ContextWithBaggage(
		ctx,
		bag,
	), span.SpanContext()
}

func addedPayload(t *testing.T, ctx context.Context) payload[testMeta] {
	t.Helper()

	var captured []byte
	actor, cancel := newTestActor(&mockKV{
		putFn: func(_ context.Context, _ string, value []byte) (uint64, error) {
			captured = value
			return 1, nil
		},
	})
	defer cancel()

	if err := actor.Add(
		ctx,
		"key",
		testMeta{Type: "T"},
		time.Minute,
	); err != nil {
		t.Fatalf("Add: %v", err)
	}

	stored, err := unmarshalPayload[testMeta](captured)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	return stored
}

func TestTimerSpanLinksScheduler(t *testing.T) {
	recorder := recordTimerSpans(t)
	ctx, scheduler := schedulerContext(t)

	stored := addedPayload(
		t,
		ctx,
	)
	if len(stored.Trace) == 0 {
		t.Fatalf("Add must store trace")
	}

	actor, cancel := newTestActor(&mockKV{})
	defer cancel()

	var callbackSpan trace.SpanContext
	var txID string
	actor.subscribers = []subscriber[testMeta]{
		{
			Callback: func(cbCtx context.Context, _ string, _ testMeta) {
				callbackSpan = trace.SpanContextFromContext(cbCtx)
				txID = baggage.FromContext(cbCtx).Member("pl.tx_id").Value()
			},
		},
	}
	actor.rememberTrace(
		"key",
		stored.Trace,
	)
	actor.fireMatchingCallbacks(
		"key",
		testMeta{},
	)

	var fired sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "timer.fire" {
			fired = span
		}
	}
	if fired == nil {
		t.Fatalf("no timer.fire span")
	}
	if fired.Parent().IsValid() {
		t.Fatalf("timer.fire must be a root span")
	}
	if len(fired.Links()) != 1 || fired.Links()[0].SpanContext.SpanID() != scheduler.SpanID() {
		t.Fatalf("link must point to scheduler span: %v", fired.Links())
	}
	if callbackSpan.SpanID() != fired.SpanContext().SpanID() {
		t.Fatalf("callback ctx must carry timer.fire span")
	}
	if txID != "tx-77" {
		t.Fatalf("baggage pl.tx_id: want tx-77 got %q", txID)
	}

	found := false
	for _, kv := range fired.Attributes() {
		if string(kv.Key) == "timer.key" && kv.Value.AsString() == "key" {
			found = true
		}
	}
	if !found {
		t.Fatalf("timer.key attribute missing")
	}
}

func TestTimerSpanOldPayload(t *testing.T) {
	recorder := recordTimerSpans(t)

	_, span := startTimerSpan(
		"key",
		nil,
	)
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	if spans[0].Parent().IsValid() || len(spans[0].Links()) != 0 {
		t.Fatalf("old payload must give a root span without links")
	}
}

func TestPayloadJSONBackwardCompatible(t *testing.T) {
	old := []byte(`{"e":5,"m":{"type":"X"}}`)

	stored, err := unmarshalPayload[testMeta](old)
	if err != nil {
		t.Fatalf("old json: %v", err)
	}
	if stored.Metadata.Type != "X" || stored.Trace != nil {
		t.Fatalf("unexpected: %+v", stored)
	}

	data, err := json.Marshal(stored)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(data) != `{"e":5,"m":{"type":"X"}}` {
		t.Fatalf("empty trace must be omitted: %s", data)
	}
}
