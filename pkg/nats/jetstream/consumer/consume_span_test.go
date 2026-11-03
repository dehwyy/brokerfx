package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	testTraceID  = "0af7651916cd43dd8448eb211c80319c"
	testParentID = "b7ad6b7169203331"
)

type traceMsg struct {
	*settleMsg

	headers   nats.Header
	delivered uint64
}

func newTraceMsg(headers nats.Header, delivered uint64) *traceMsg {
	return &traceMsg{
		settleMsg: &settleMsg{},
		headers:   headers,
		delivered: delivered,
	}
}

func (m *traceMsg) Headers() nats.Header { return m.headers }

func (m *traceMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		Stream:       "ORDERS",
		Consumer:     "orders-worker",
		NumDelivered: m.delivered,
		Sequence: jetstream.SequencePair{
			Stream: 42,
		},
	}, nil
}

func tracedHeaders() nats.Header {
	return nats.Header{
		"traceparent": []string{"00-" + testTraceID + "-" + testParentID + "-01"},
		"baggage":     []string{"pl.tx_id=tx-77"},
		"Nats-Msg-Id": []string{"msg-1"},
	}
}

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
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

func consumeSpans(recorder *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "nats.consume" {
			out = append(
				out,
				span,
			)
		}
	}

	return out
}

func oneConsumeSpan(t *testing.T, recorder *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()

	spans := consumeSpans(recorder)
	if len(spans) != 1 {
		t.Fatalf(
			"want 1 nats.consume span, got %d",
			len(spans),
		)
	}

	return spans[0]
}

func spanAttribute(span sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}

	return attribute.Value{}, false
}

func TestProcessContinuesTraceFromHeaders(t *testing.T) {
	recorder := recordSpans(t)
	msg := newTraceMsg(
		tracedHeaders(),
		1,
	)

	var handlerSpan trace.SpanContext
	var txID string
	process(msg, handlerOf2(func(ctx context.Context) error {
		handlerSpan = trace.SpanContextFromContext(ctx)
		txID = baggage.FromContext(ctx).Member("pl.tx_id").Value()
		return nil
	}))

	span := oneConsumeSpan(
		t,
		recorder,
	)
	if span.Parent().TraceID().String() != testTraceID {
		t.Fatalf(
			"parent trace: want %s got %s",
			testTraceID,
			span.Parent().TraceID(),
		)
	}
	if span.Parent().SpanID().String() != testParentID {
		t.Fatalf(
			"parent span: want %s got %s",
			testParentID,
			span.Parent().SpanID(),
		)
	}
	if handlerSpan.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("handler ctx must carry nats.consume span")
	}
	if txID != "tx-77" {
		t.Fatalf(
			"baggage pl.tx_id: want tx-77 got %q",
			txID,
		)
	}
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf(
			"kind: %v",
			span.SpanKind(),
		)
	}
	if value, ok := spanAttribute(
		span,
		"messaging.message.id",
	); !ok || value.AsString() != "msg-1" {
		t.Fatalf(
			"messaging.message.id: %v %v",
			value,
			ok,
		)
	}
	if value, ok := spanAttribute(
		span,
		"messaging.nats.stream",
	); !ok || value.AsString() != "ORDERS" {
		t.Fatalf(
			"messaging.nats.stream: %v %v",
			value,
			ok,
		)
	}
	if value, ok := spanAttribute(
		span,
		"messaging.consumer.group.name",
	); !ok || value.AsString() != "orders-worker" {
		t.Fatalf(
			"messaging.consumer.group.name: %v %v",
			value,
			ok,
		)
	}
	if value, ok := spanAttribute(
		span,
		"messaging.nats.sequence.stream",
	); !ok || value.AsInt64() != 42 {
		t.Fatalf(
			"messaging.nats.sequence.stream: %v %v",
			value,
			ok,
		)
	}
	if msg.count("ack") != 1 {
		t.Fatalf(
			"ack: %d",
			msg.count("ack"),
		)
	}
}

func TestProcessRootWithoutHeaders(t *testing.T) {
	recorder := recordSpans(t)
	process(newTraceMsg(
		nats.Header{},
		1,
	), handlerOf2(func(context.Context) error { return nil }))

	span := oneConsumeSpan(
		t,
		recorder,
	)
	if span.Parent().IsValid() {
		t.Fatalf(
			"want root span, parent %v",
			span.Parent(),
		)
	}
}

func TestProcessRootOnMalformedTraceparent(t *testing.T) {
	recorder := recordSpans(t)
	headers := nats.Header{"traceparent": []string{"garbage"}}
	process(newTraceMsg(
		headers,
		1,
	), handlerOf2(func(context.Context) error { return nil }))

	span := oneConsumeSpan(
		t,
		recorder,
	)
	if span.Parent().IsValid() {
		t.Fatalf(
			"want root span, parent %v",
			span.Parent(),
		)
	}
}

func TestProcessKeepsSpanFromMiddleware(t *testing.T) {
	recorder := recordSpans(t)
	provider := otel.GetTracerProvider()
	var mwSpan trace.SpanContext

	opts := handlerOf2(func(context.Context) error { return nil })
	opts.BeforeHandlerMiddleware = append(
		opts.BeforeHandlerMiddleware,
		func(ctx context.Context, _ jetstream.Msg) (context.Context, error) {
			ctx, span := provider.Tracer("mw").Start(
				ctx,
				"mw",
			)
			mwSpan = span.SpanContext()
			span.End()
			return ctx, nil
		},
	)
	process(
		newTraceMsg(
			tracedHeaders(),
			1,
		),
		opts,
	)

	span := oneConsumeSpan(
		t,
		recorder,
	)
	if span.Parent().SpanID() != mwSpan.SpanID() {
		t.Fatalf(
			"parent: want middleware span %s got %s",
			mwSpan.SpanID(),
			span.Parent().SpanID(),
		)
	}
	if span.SpanContext().TraceID() != mwSpan.TraceID() {
		t.Fatalf("trace id must follow middleware span")
	}
}

func TestProcessHandlerErrorOnSpan(t *testing.T) {
	recorder := recordSpans(t)
	msg := newTraceMsg(
		tracedHeaders(),
		3,
	)
	process(msg, handlerOf2(func(context.Context) error { return errors.New("boom") }))

	span := oneConsumeSpan(
		t,
		recorder,
	)
	if span.Status().Code != codes.Error {
		t.Fatalf(
			"status: %v",
			span.Status(),
		)
	}
	if value, ok := spanAttribute(
		span,
		"messaging.nats.num_delivered",
	); !ok || value.AsInt64() != 3 {
		t.Fatalf(
			"messaging.nats.num_delivered: %v %v",
			value,
			ok,
		)
	}
	if msg.count("nak") != 1 {
		t.Fatalf(
			"nak: %d",
			msg.count("nak"),
		)
	}
}

func TestProcessPanicMarksSpan(t *testing.T) {
	recorder := recordSpans(t)
	msg := newTraceMsg(
		tracedHeaders(),
		1,
	)
	process(msg, handlerOf2(func(context.Context) error { panic("kaboom") }))

	span := oneConsumeSpan(
		t,
		recorder,
	)
	if span.Status().Code != codes.Error {
		t.Fatalf(
			"status: %v",
			span.Status(),
		)
	}
	if msg.count("nak") != 1 {
		t.Fatalf(
			"nak: %d",
			msg.count("nak"),
		)
	}
}

func TestProcessSecondMessageDoesNotInheritFirstSpan(t *testing.T) {
	recorder := recordSpans(t)
	first := newTraceMsg(
		nats.Header{},
		1,
	)
	second := newTraceMsg(
		nats.Header{},
		1,
	)
	opts := handlerOf2(func(context.Context) error { return nil })

	process(
		first,
		opts,
	)
	process(
		second,
		opts,
	)

	spans := consumeSpans(recorder)
	if len(spans) != 2 {
		t.Fatalf(
			"want 2 spans, got %d",
			len(spans),
		)
	}
	if spans[1].Parent().IsValid() {
		t.Fatalf(
			"second message must be a root, parent %v",
			spans[1].Parent(),
		)
	}
	if spans[0].SpanContext().TraceID() == spans[1].SpanContext().TraceID() {
		t.Fatalf("messages must not share a trace")
	}
}

func handlerOf2(fn func(ctx context.Context) error) Opts {
	return Opts{HandlerFunc: func(ctx context.Context, _ jetstream.Msg) error { return fn(ctx) }}
}
