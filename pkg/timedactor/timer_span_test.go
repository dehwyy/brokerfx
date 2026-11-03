package timedactor

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"github.com/nats-io/nats.go/jetstream"
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
		t.Fatalf(
			"baggage member: %v",
			err,
		)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf(
			"baggage: %v",
			err,
		)
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
		t.Fatalf(
			"Add: %v",
			err,
		)
	}

	stored, err := unmarshalPayload[testMeta](captured)
	if err != nil {
		t.Fatalf(
			"unmarshal: %v",
			err,
		)
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
	actor.fireMatchingCallbacks(
		"key",
		testMeta{},
		stored.Trace,
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
		t.Fatalf(
			"link must point to scheduler span: %v",
			fired.Links(),
		)
	}
	if callbackSpan.SpanID() != fired.SpanContext().SpanID() {
		t.Fatalf("callback ctx must carry timer.fire span")
	}
	if txID != "tx-77" {
		t.Fatalf(
			"baggage pl.tx_id: want tx-77 got %q",
			txID,
		)
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
		t.Fatalf(
			"want 1 span, got %d",
			len(spans),
		)
	}
	if spans[0].Parent().IsValid() || len(spans[0].Links()) != 0 {
		t.Fatalf("old payload must give a root span without links")
	}
}

func TestPayloadJSONBackwardCompatible(t *testing.T) {
	old := []byte(`{"e":5,"m":{"type":"X"}}`)

	stored, err := unmarshalPayload[testMeta](old)
	if err != nil {
		t.Fatalf(
			"old json: %v",
			err,
		)
	}
	if stored.Metadata.Type != "X" || stored.Trace != nil {
		t.Fatalf(
			"unexpected: %+v",
			stored,
		)
	}

	data, err := json.Marshal(stored)
	if err != nil {
		t.Fatalf(
			"marshal: %v",
			err,
		)
	}
	if string(data) != `{"e":5,"m":{"type":"X"}}` {
		t.Fatalf(
			"empty trace must be omitted: %s",
			data,
		)
	}
}

func putEntry(t *testing.T, stored map[string]string) *mockEntry {
	t.Helper()

	data, err := marshalPayload(
		payload[testMeta]{
			ExpiresAt: time.Now().UnixNano(),
			Metadata:  testMeta{Type: "T"},
			Trace:     stored,
		},
	)
	if err != nil {
		t.Fatalf(
			"marshal: %v",
			err,
		)
	}

	return &mockEntry{
		key:       "order.1",
		value:     data,
		revision:  3,
		operation: jetstream.KeyValuePut,
	}
}

func storedTrace(t *testing.T) (map[string]string, trace.SpanContext) {
	t.Helper()

	ctx, scheduler := schedulerContext(t)
	stored := map[string]string{}
	tracenats.InjectMap(
		ctx,
		stored,
	)

	return stored, scheduler
}

func TestTimerSpanFromWatchedPayload(t *testing.T) {
	recorder := recordTimerSpans(t)
	stored, scheduler := storedTrace(t)

	actor, cancel := newTestActor(&mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return nil
		},
	})
	defer cancel()

	type fired struct {
		span trace.SpanContext
		txID string
	}
	results := make(
		chan fired,
		1,
	)
	actor.subscribers = []subscriber[testMeta]{
		{
			Callback: func(cbCtx context.Context, _ string, _ testMeta) {
				results <- fired{
					span: trace.SpanContextFromContext(cbCtx),
					txID: baggage.FromContext(cbCtx).Member("pl.tx_id").Value(),
				}
			},
		},
	}

	actor.handleWatchEntry(putEntry(
		t,
		stored,
	))

	var got fired
	select {
	case got = <-results:
	case <-time.After(5 * time.Second):
		t.Fatalf("callback did not fire")
	}
	if got.txID != "tx-77" {
		t.Fatalf(
			"baggage pl.tx_id: want tx-77 got %q",
			got.txID,
		)
	}

	var span sdktrace.ReadOnlySpan
	for _, ended := range recorder.Ended() {
		if ended.Name() == "timer.fire" {
			span = ended
		}
	}
	if span == nil {
		t.Fatalf("no timer.fire span")
	}
	if len(span.Links()) != 1 || span.Links()[0].SpanContext.SpanID() != scheduler.SpanID() {
		t.Fatalf(
			"link must point to scheduler span: %v",
			span.Links(),
		)
	}
	if got.span.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("callback ctx must carry timer.fire span")
	}
}

func syncMapEntries(actor *TimedActor[testMeta]) int {
	count := 0
	value := reflect.ValueOf(actor).Elem()
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if field.Type() != reflect.TypeOf(sync.Map{}) {
			continue
		}

		stored := reflect.NewAt(
			field.Type(),
			unsafe.Pointer(field.UnsafeAddr()),
		).Interface().(*sync.Map)
		stored.Range(func(_, _ any) bool {
			count++
			return true
		})
	}

	return count
}

func TestTimerHoldsNoStateAfterLostRace(t *testing.T) {
	recordTimerSpans(t)
	stored, _ := storedTrace(t)

	attempted := make(
		chan struct{},
		1,
	)
	actor, cancel := newTestActor(&mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			attempted <- struct{}{}
			return jetstream.ErrKeyNotFound
		},
	})
	defer cancel()

	actor.handleWatchEntry(putEntry(
		t,
		stored,
	))

	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatalf("timer did not fire")
	}
	time.Sleep(50 * time.Millisecond)

	if count := syncMapEntries(actor); count != 0 {
		t.Fatalf(
			"trace state outlived the timer: %d entries",
			count,
		)
	}
}

func TestTimerHoldsNoStateAfterClear(t *testing.T) {
	recordTimerSpans(t)
	stored, _ := storedTrace(t)

	actor, cancel := newTestActor(&mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return nil
		},
	})
	defer cancel()

	entry := putEntry(
		t,
		stored,
	)
	var payloadValue payload[testMeta]
	if err := json.Unmarshal(
		entry.value,
		&payloadValue,
	); err != nil {
		t.Fatalf(
			"unmarshal: %v",
			err,
		)
	}
	payloadValue.ExpiresAt = time.Now().Add(time.Hour).UnixNano()
	data, err := marshalPayload(payloadValue)
	if err != nil {
		t.Fatalf(
			"marshal: %v",
			err,
		)
	}
	entry.value = data

	actor.handleWatchEntry(entry)

	if err := actor.Clear(
		context.Background(),
		"order.1",
	); err != nil {
		t.Fatalf(
			"Clear: %v",
			err,
		)
	}

	if count := syncMapEntries(actor); count != 0 {
		t.Fatalf(
			"trace state outlived the cleared timer: %d entries",
			count,
		)
	}
}
