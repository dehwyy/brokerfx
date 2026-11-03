package batch

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
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
	msg := panicAckMsg{newMsg("a", 1)}

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
