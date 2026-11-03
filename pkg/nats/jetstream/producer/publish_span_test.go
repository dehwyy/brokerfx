package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type publishJS struct {
	jetstream.JetStream

	published *nats.Msg
	err       error
}

func (j *publishJS) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	j.published = msg
	return &jetstream.PubAck{}, j.err
}

type asyncAck struct {
	ok  chan *jetstream.PubAck
	err chan error
	msg *nats.Msg
}

func (a *asyncAck) Ok() <-chan *jetstream.PubAck { return a.ok }

func (a *asyncAck) Err() <-chan error { return a.err }

func (a *asyncAck) Msg() *nats.Msg { return a.msg }

func (j *publishJS) PublishMsgAsync(msg *nats.Msg, _ ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	j.published = msg
	if j.err != nil {
		return nil, j.err
	}

	ack := &asyncAck{
		ok: make(
			chan *jetstream.PubAck,
			1,
		),
		err: make(
			chan error,
			1,
		),
		msg: msg,
	}
	ack.ok <- &jetstream.PubAck{}

	return ack, nil
}

type testEvent struct{}

func (testEvent) Subject() string { return "orders.created" }
func (testEvent) Data() any       { return map[string]string{"a": "b"} }

func recordPublishSpans(t *testing.T) *tracetest.SpanRecorder {
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

func publishSpan(t *testing.T, recorder *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()

	for _, span := range recorder.Ended() {
		if span.Name() == "nats.publish" {
			return span
		}
	}
	t.Fatalf("no nats.publish span")

	return nil
}

func TestProducePublishSpanAndHeader(t *testing.T) {
	recorder := recordPublishSpans(t)
	js := &publishJS{}
	prod := New(Opts{JetStream: js})

	if err := prod.ProduceSync(
		context.Background(),
		testEvent{},
	); err != nil {
		t.Fatalf(
			"ProduceSync: %v",
			err,
		)
	}

	span := publishSpan(
		t,
		recorder,
	)
	if span.SpanKind() != trace.SpanKindProducer {
		t.Fatalf(
			"kind: %v",
			span.SpanKind(),
		)
	}

	want := span.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String()
	got := js.published.Header.Get("traceparent")
	if len(got) < 55 || got[3:3+len(want)] != want {
		t.Fatalf(
			"traceparent %q must carry span %s",
			got,
			want,
		)
	}
}

func TestProducePublishSpanError(t *testing.T) {
	recorder := recordPublishSpans(t)
	js := &publishJS{err: errors.New("boom")}
	prod := New(Opts{JetStream: js})

	if err := prod.ProduceSync(
		context.Background(),
		testEvent{},
	); err == nil {
		t.Fatalf("want error")
	}

	if publishSpan(
		t,
		recorder,
	).Status().Code != codes.Error {
		t.Fatalf("span status must be Error")
	}
}

func TestProduceKeepsMiddlewareOverride(t *testing.T) {
	recordPublishSpans(t)
	js := &publishJS{}
	prod := New(Opts{
		JetStream: js,
		BeforePublishMiddleware: []BeforePublishMiddleware{
			func(_ context.Context, msg *nats.Msg) {
				msg.Header.Set(
					"traceparent",
					"custom",
				)
			},
		},
	})

	if err := prod.ProduceSync(
		context.Background(),
		testEvent{},
	); err != nil {
		t.Fatalf(
			"ProduceSync: %v",
			err,
		)
	}

	if js.published.Header.Get("traceparent") != "custom" {
		t.Fatalf(
			"middleware override lost: %q",
			js.published.Header.Get("traceparent"),
		)
	}
}

func TestProduceAsyncPublishSpanAndHeader(t *testing.T) {
	recorder := recordPublishSpans(t)
	js := &publishJS{}
	prod := New(Opts{JetStream: js})

	if err := prod.Produce(
		context.Background(),
		testEvent{},
	); err != nil {
		t.Fatalf(
			"Produce: %v",
			err,
		)
	}

	span := publishSpan(
		t,
		recorder,
	)
	if span.SpanKind() != trace.SpanKindProducer {
		t.Fatalf(
			"kind: %v",
			span.SpanKind(),
		)
	}

	want := span.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String()
	got := js.published.Header.Get("traceparent")
	if len(got) < 55 || got[3:3+len(want)] != want {
		t.Fatalf(
			"traceparent %q must carry span %s",
			got,
			want,
		)
	}
}

func TestProduceAsyncPublishSpanError(t *testing.T) {
	recorder := recordPublishSpans(t)
	js := &publishJS{err: errors.New("boom")}
	prod := New(Opts{JetStream: js})

	if err := prod.Produce(
		context.Background(),
		testEvent{},
	); err == nil {
		t.Fatalf("want error")
	}

	if publishSpan(
		t,
		recorder,
	).Status().Code != codes.Error {
		t.Fatalf("span status must be Error")
	}
}

func TestPublishSpanCarriesMessageIDWhenSet(t *testing.T) {
	for _, async := range []bool{false, true} {
		recorder := recordPublishSpans(t)
		prod := New(Opts{JetStream: &publishJS{}})

		var err error
		if async {
			err = prod.Produce(
				context.Background(),
				testEvent{},
				WithMsgID("msg-42"),
			)
		} else {
			err = prod.ProduceSync(
				context.Background(),
				testEvent{},
				WithMsgID("msg-42"),
			)
		}
		if err != nil {
			t.Fatalf(
				"publish: %v",
				err,
			)
		}

		got := ""
		for _, kv := range publishSpan(
			t,
			recorder,
		).Attributes() {
			if string(kv.Key) == "messaging.message.id" {
				got = kv.Value.AsString()
			}
		}
		if got != "msg-42" {
			t.Fatalf(
				"async=%v messaging.message.id: %q",
				async,
				got,
			)
		}
	}
}

func TestPublishSpanWithoutMessageIDHasNoAttribute(t *testing.T) {
	recorder := recordPublishSpans(t)
	prod := New(Opts{JetStream: &publishJS{}})

	if err := prod.ProduceSync(
		context.Background(),
		testEvent{},
	); err != nil {
		t.Fatalf(
			"ProduceSync: %v",
			err,
		)
	}

	for _, kv := range publishSpan(
		t,
		recorder,
	).Attributes() {
		if string(kv.Key) == "messaging.message.id" {
			t.Fatalf(
				"unexpected messaging.message.id: %v",
				kv.Value,
			)
		}
	}
}
