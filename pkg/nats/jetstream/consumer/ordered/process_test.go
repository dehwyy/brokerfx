package ordered

import (
	"context"
	"errors"
	"testing"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/middleware"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	testTraceID  = "0af7651916cd43dd8448eb211c80319c"
	testParentID = "b7ad6b7169203331"
)

type fakeMsg struct {
	jetstream.Msg

	headers nats.Header
}

func (m *fakeMsg) Subject() string { return "snap.a" }

func (m *fakeMsg) Headers() nats.Header { return m.headers }

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, errors.New("no metadata")
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

func TestOrderedSpanOnlyWithContext(t *testing.T) {
	recorder := recordSpans(t)

	calls := 0
	opts := Opts{
		HandlerFunc: func(_ context.Context, _ jetstream.Msg) error {
			calls++
			return nil
		},
	}

	process(
		context.Background(),
		&fakeMsg{headers: nats.Header{}},
		opts,
	)
	if calls != 1 {
		t.Fatalf(
			"handler must run without context, calls=%d",
			calls,
		)
	}
	if len(recorder.Ended()) != 0 {
		t.Fatalf(
			"no span expected without traceparent, got %d",
			len(recorder.Ended()),
		)
	}

	process(
		context.Background(),
		&fakeMsg{
			headers: nats.Header{
				"traceparent": []string{"00-" + testTraceID + "-" + testParentID + "-01"},
			},
		},
		opts,
	)
	if calls != 2 {
		t.Fatalf(
			"handler must run with context, calls=%d",
			calls,
		)
	}

	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "nats.consume" {
		t.Fatalf(
			"want one nats.consume span, got %d",
			len(spans),
		)
	}
	if spans[0].Parent().TraceID().String() != testTraceID || spans[0].Parent().SpanID().String() != testParentID {
		t.Fatalf(
			"parent from header expected, got %v",
			spans[0].Parent(),
		)
	}
}

func TestOrderedHandlerErrorContinues(t *testing.T) {
	recordSpans(t)

	calls := 0
	opts := Opts{
		HandlerFunc: func(_ context.Context, _ jetstream.Msg) error {
			calls++
			return errors.New("boom")
		},
	}

	for range 3 {
		process(
			context.Background(),
			&fakeMsg{headers: nats.Header{}},
			opts,
		)
	}
	if calls != 3 {
		t.Fatalf(
			"every message must reach the handler, calls=%d",
			calls,
		)
	}
}

func TestOrderedMiddlewareErrorSkipsMessage(t *testing.T) {
	calls := 0
	opts := Opts{
		HandlerFunc: func(_ context.Context, _ jetstream.Msg) error {
			calls++
			return nil
		},
		BeforeHandlerMiddleware: []middleware.Middleware{
			func(ctx context.Context, _ jetstream.Msg) (context.Context, error) {
				return ctx, errors.New("mw")
			},
		},
	}

	process(
		context.Background(),
		&fakeMsg{headers: nats.Header{}},
		opts,
	)
	if calls != 0 {
		t.Fatalf("handler must not run after middleware error")
	}
}

func TestOrderedRecoversHandlerPanic(t *testing.T) {
	recorder := recordSpans(t)

	calls := 0
	opts := Opts{
		HandlerFunc: func(_ context.Context, _ jetstream.Msg) error {
			calls++
			if calls == 1 {
				panic("kaboom")
			}
			return nil
		},
	}
	msg := &fakeMsg{
		headers: nats.Header{
			"traceparent": []string{"00-" + testTraceID + "-" + testParentID + "-01"},
		},
	}

	process(
		context.Background(),
		msg,
		opts,
	)
	process(
		context.Background(),
		msg,
		opts,
	)
	if calls != 2 {
		t.Fatalf(
			"stream must continue after panic, calls=%d",
			calls,
		)
	}

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf(
			"want 2 spans, got %d",
			len(spans),
		)
	}
	if spans[0].Status().Code != codes.Error || spans[0].Status().Description != "panic" {
		t.Fatalf(
			"panic span status: %v",
			spans[0].Status(),
		)
	}
	if spans[1].Status().Code == codes.Error {
		t.Fatalf("healthy span must not be Error")
	}
}

func TestOrderedSpanCarriesStreamAttributes(t *testing.T) {
	recorder := recordSpans(t)

	process(
		context.Background(),
		&metaMsg{
			fakeMsg: fakeMsg{
				headers: nats.Header{
					"traceparent": []string{"00-" + testTraceID + "-" + testParentID + "-01"},
				},
			},
		},
		Opts{
			HandlerFunc: func(_ context.Context, _ jetstream.Msg) error {
				return nil
			},
		},
	)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf(
			"want 1 span, got %d",
			len(spans),
		)
	}

	got := map[string]string{}
	for _, kv := range spans[0].Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	if got["messaging.nats.stream"] != "SNAP" || got["messaging.nats.sequence.stream"] != "42" {
		t.Fatalf(
			"stream attributes missing: %v",
			got,
		)
	}
}

type metaMsg struct {
	fakeMsg
}

func (m *metaMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		Stream: "SNAP",
		Sequence: jetstream.SequencePair{
			Stream: 42,
		},
	}, nil
}
