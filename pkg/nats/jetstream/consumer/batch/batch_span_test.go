package batch

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordBatchSpans(t *testing.T) *tracetest.SpanRecorder {
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

func withTraceparent(msg *fakeMsg, traceID string, spanID string) *fakeMsg {
	msg.headers.Set(
		"traceparent",
		"00-"+traceID+"-"+spanID+"-01",
	)

	return msg
}

func TestBatchSpanLinksMessages(t *testing.T) {
	recorder := recordBatchSpans(t)
	first := withTraceparent(
		newMsg(
			"a",
			1,
		),
		"0af7651916cd43dd8448eb211c80319c",
		"b7ad6b7169203331",
	)
	second := newMsg(
		"b",
		1,
	)
	third := withTraceparent(
		newMsg(
			"c",
			1,
		),
		"1af7651916cd43dd8448eb211c80319d",
		"c7ad6b7169203332",
	)

	var handlerSpan trace.SpanContext
	consumer := testConsumer(
		func(ctx context.Context, msgs []jetstream.Msg) []Result {
			handlerSpan = trace.SpanContextFromContext(ctx)
			return []Result{Ack(), Ack(), Ack()}
		},
		5,
		nil,
	)
	consumer.processBatch(
		context.Background(),
		asMsgs(
			first,
			second,
			third,
		),
	)

	var batchSpan sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "nats.consume.batch" {
			batchSpan = span
		}
	}
	if batchSpan == nil {
		t.Fatal("nats.consume.batch span not recorded")
	}
	if batchSpan.Parent().IsValid() {
		t.Fatalf(
			"want root span, parent %v",
			batchSpan.Parent(),
		)
	}
	if batchSpan.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf(
			"kind: %v",
			batchSpan.SpanKind(),
		)
	}
	if len(batchSpan.Links()) != 2 {
		t.Fatalf(
			"want 2 links, got %d",
			len(batchSpan.Links()),
		)
	}
	if batchSpan.Links()[0].SpanContext.SpanID().String() != "b7ad6b7169203331" {
		t.Fatalf(
			"first link: %v",
			batchSpan.Links()[0].SpanContext,
		)
	}
	if handlerSpan.SpanID() != batchSpan.SpanContext().SpanID() {
		t.Fatal("handler ctx must carry the batch span")
	}

	count := int64(-1)
	for _, kv := range batchSpan.Attributes() {
		if string(kv.Key) == "messaging.batch.message_count" {
			count = kv.Value.AsInt64()
		}
	}
	if count != 3 {
		t.Fatalf(
			"messaging.batch.message_count: want 3 got %d",
			count,
		)
	}
	if batchSpan.Status().Code == codes.Error {
		t.Fatalf(
			"unexpected error status: %v",
			batchSpan.Status(),
		)
	}
}

type panicAckMsg struct {
	*fakeMsg
}

func (panicAckMsg) Ack() error { panic("ack exploded") }

func TestBatchSpanEndsOnceWhenSettlePanics(t *testing.T) {
	recorder := recordBatchSpans(t)
	msg := panicAckMsg{newMsg(
		"a",
		1,
	)}

	consumer := testConsumer(
		func(_ context.Context, msgs []jetstream.Msg) []Result {
			return []Result{{Verdict: VerdictAck}}
		},
		3,
		nil,
	)

	func() {
		defer func() {
			if recover() == nil {
				t.Error("settle panic expected")
			}
		}()
		consumer.processBatch(
			context.Background(),
			[]jetstream.Msg{msg},
		)
	}()

	var ended int
	for _, span := range recorder.Ended() {
		if span.Name() != "nats.consume.batch" {
			continue
		}
		ended++
		if span.Status().Code != codes.Error || span.Status().Description != "panic" {
			t.Fatalf(
				"status: %v",
				span.Status(),
			)
		}
	}
	if ended != 1 {
		t.Fatalf(
			"want 1 ended batch span, got %d",
			ended,
		)
	}
}

func spansNamed(recorder *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var found []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == name {
			found = append(
				found,
				span,
			)
		}
	}

	return found
}

func TestBatchCreatesConsumeSpanPerMessage(t *testing.T) {
	recorder := recordBatchSpans(t)
	first := withTraceparent(
		newMsg(
			"a",
			1,
		),
		"0af7651916cd43dd8448eb211c80319c",
		"b7ad6b7169203331",
	)
	first.headers.Set(
		"Nats-Msg-Id",
		"id-1",
	)
	second := withTraceparent(
		newMsg(
			"b",
			1,
		),
		"1af7651916cd43dd8448eb211c80319d",
		"c7ad6b7169203332",
	)
	third := newMsg(
		"c",
		1,
	)

	consumer := testConsumer(
		resultsHandler(Ack(), Ack(), Ack()),
		5,
		nil,
	)
	consumer.processBatch(
		context.Background(),
		asMsgs(
			first,
			second,
			third,
		),
	)

	batchSpans := spansNamed(
		recorder,
		"nats.consume.batch",
	)
	if len(batchSpans) != 1 {
		t.Fatalf(
			"want one batch span, got %d",
			len(batchSpans),
		)
	}

	items := spansNamed(
		recorder,
		"nats.consume",
	)
	if len(items) != 3 {
		t.Fatalf(
			"want 3 message spans, got %d",
			len(items),
		)
	}

	wantTrace := []string{
		"0af7651916cd43dd8448eb211c80319c",
		"1af7651916cd43dd8448eb211c80319d",
		"",
	}
	wantParent := []string{
		"b7ad6b7169203331",
		"c7ad6b7169203332",
		"",
	}
	for i, item := range items {
		if item.SpanKind() != trace.SpanKindConsumer {
			t.Fatalf(
				"kind: %v",
				item.SpanKind(),
			)
		}
		if wantTrace[i] == "" {
			if item.Parent().IsValid() {
				t.Fatalf(
					"message without context must be a root, parent %v",
					item.Parent(),
				)
			}
		} else if item.Parent().TraceID().String() != wantTrace[i] || item.Parent().SpanID().String() != wantParent[i] {
			t.Fatalf(
				"message %d parent: %v",
				i,
				item.Parent(),
			)
		}
		if len(item.Links()) != 1 || item.Links()[0].SpanContext.SpanID() != batchSpans[0].SpanContext().SpanID() {
			t.Fatalf(
				"message %d must link to the batch span: %v",
				i,
				item.Links(),
			)
		}
		if item.Status().Code == codes.Error {
			t.Fatalf(
				"acked message span must not be Error: %v",
				item.Status(),
			)
		}
	}

	ids := map[string]string{}
	for _, kv := range items[0].Attributes() {
		ids[string(kv.Key)] = kv.Value.Emit()
	}
	if ids["messaging.message.id"] != "id-1" || ids["messaging.system"] != "nats" {
		t.Fatalf(
			"attributes: %v",
			ids,
		)
	}
}

func TestBatchNakMarksMessageAndBatchSpansError(t *testing.T) {
	recorder := recordBatchSpans(t)
	first := newMsg(
		"a",
		1,
	)
	second := newMsg(
		"b",
		1,
	)

	consumer := testConsumer(
		resultsHandler(Ack(), Nak(errBusiness)),
		5,
		nil,
	)
	consumer.processBatch(
		context.Background(),
		asMsgs(
			first,
			second,
		),
	)

	items := spansNamed(
		recorder,
		"nats.consume",
	)
	if len(items) != 2 {
		t.Fatalf(
			"want 2 message spans, got %d",
			len(items),
		)
	}
	if items[0].Status().Code == codes.Error {
		t.Fatalf(
			"acked message span must be clean: %v",
			items[0].Status(),
		)
	}
	if items[1].Status().Code != codes.Error || items[1].Status().Description != "nak" {
		t.Fatalf(
			"nak message span: %v",
			items[1].Status(),
		)
	}

	batchSpans := spansNamed(
		recorder,
		"nats.consume.batch",
	)
	if len(batchSpans) != 1 {
		t.Fatalf(
			"want one batch span, got %d",
			len(batchSpans),
		)
	}
	if batchSpans[0].Status().Code != codes.Error || batchSpans[0].Status().Description != "nack" {
		t.Fatalf(
			"batch status: %v",
			batchSpans[0].Status(),
		)
	}

	failed := int64(-1)
	for _, kv := range batchSpans[0].Attributes() {
		if string(kv.Key) == "messaging.batch.failed_count" {
			failed = kv.Value.AsInt64()
		}
	}
	if failed != 1 {
		t.Fatalf(
			"messaging.batch.failed_count: want 1 got %d",
			failed,
		)
	}
}

func TestBatchTermMarksMessageSpanError(t *testing.T) {
	recorder := recordBatchSpans(t)
	poison := &poisonRecorder{}

	consumer := testConsumer(
		resultsHandler(Term(errBusiness)),
		5,
		poison,
	)
	consumer.processBatch(
		context.Background(),
		asMsgs(newMsg(
			"a",
			1,
		)),
	)

	items := spansNamed(
		recorder,
		"nats.consume",
	)
	if len(items) != 1 || items[0].Status().Code != codes.Error || items[0].Status().Description != "term" {
		t.Fatalf(
			"term message span: %v",
			items,
		)
	}
}

func TestBatchIsolatedMessageSpansEndWithVerdict(t *testing.T) {
	recorder := recordBatchSpans(t)

	calls := 0
	consumer := testConsumer(
		func(_ context.Context, msgs []jetstream.Msg) []Result {
			calls++
			if calls == 1 {
				panic("boom")
			}
			return []Result{Ack()}
		},
		5,
		nil,
	)
	consumer.processBatch(
		context.Background(),
		asMsgs(
			newMsg(
				"a",
				1,
			),
			newMsg(
				"b",
				1,
			),
		),
	)

	items := spansNamed(
		recorder,
		"nats.consume",
	)
	if len(items) != 2 {
		t.Fatalf(
			"want 2 message spans, got %d",
			len(items),
		)
	}
	for _, item := range items {
		if item.Status().Code == codes.Error {
			t.Fatalf(
				"isolated acked message must be clean: %v",
				item.Status(),
			)
		}
	}
}

func TestItemContextCarriesFilteredBaggage(t *testing.T) {
	msg := newMsg(
		"a",
		1,
	)
	msg.headers.Set(
		"traceparent",
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	)
	msg.headers.Set(
		"baggage",
		"pl.tx_id=tx-7,other=1",
	)

	ctx := ItemContext(
		context.Background(),
		msg,
	)

	members := baggage.FromContext(ctx)
	if members.Member("pl.tx_id").Value() != "tx-7" {
		t.Fatalf(
			"pl.tx_id missing: %v",
			members,
		)
	}
	if members.Member("other").Key() != "" {
		t.Fatal("non-pl baggage must be filtered")
	}
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("ItemContext must not change the span")
	}
}
