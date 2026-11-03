package batch

import (
	"context"
	"testing"

	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	traceA = "0af7651916cd43dd8448eb211c80319c"
	traceB = "1af7651916cd43dd8448eb211c80319d"
)

func workSpans(
	recorder interface {
		Ended() []sdktrace.ReadOnlySpan
	},
) []sdktrace.ReadOnlySpan {
	var found []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "work" {
			found = append(
				found,
				span,
			)
		}
	}

	return found
}

func startWork(ctx context.Context, msg jetstream.Msg) {
	itemCtx := ItemContext(
		ctx,
		msg,
	)
	_, span := otel.Tracer("test").Start(
		itemCtx,
		"work",
	)
	span.End()
}

func TestItemContextContinuesMessageTrace(t *testing.T) {
	recorder := recordBatchSpans(t)
	first := withTraceparent(
		newMsg(
			"a",
			1,
		),
		traceA,
		"b7ad6b7169203331",
	)
	first.headers.Set(
		"baggage",
		"pl.tx_id=tx-1",
	)
	second := withTraceparent(
		newMsg(
			"b",
			1,
		),
		traceB,
		"c7ad6b7169203332",
	)

	var firstBaggage baggage.Baggage
	consumer := testConsumer(
		func(ctx context.Context, msgs []jetstream.Msg) []Result {
			firstBaggage = baggage.FromContext(ItemContext(
				ctx,
				msgs[0],
			))
			for _, msg := range msgs {
				startWork(
					ctx,
					msg,
				)
			}

			return []Result{Ack(), Ack()}
		},
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
	works := workSpans(recorder)
	if len(items) != 2 || len(works) != 2 {
		t.Fatalf(
			"want 2 items and 2 work spans, got %d and %d",
			len(items),
			len(works),
		)
	}
	for i, traceID := range []string{traceA, traceB} {
		work := works[i]
		item := items[i]
		if work.Parent().SpanID() != item.SpanContext().SpanID() {
			t.Fatalf(
				"work %d must be a child of its item span: parent %s item %s",
				i,
				work.Parent().SpanID(),
				item.SpanContext().SpanID(),
			)
		}
		if work.SpanContext().TraceID().String() != traceID {
			t.Fatalf(
				"work %d trace %s want %s",
				i,
				work.SpanContext().TraceID(),
				traceID,
			)
		}
	}
	if works[0].Parent().SpanID() == works[1].Parent().SpanID() {
		t.Fatal("two messages must have two different parents")
	}
	if firstBaggage.Member("pl.tx_id").Value() != "tx-1" {
		t.Fatalf(
			"baggage missing: %v",
			firstBaggage,
		)
	}
}

func TestItemContextFrameElementsHaveOwnParents(t *testing.T) {
	recorder := recordBatchSpans(t)
	traces := []string{traceA, traceB}
	spanIDs := []string{"b7ad6b7169203331", "c7ad6b7169203332"}
	encoded := make(
		[][]byte,
		0,
		len(traces),
	)
	ids := make(
		[]string,
		0,
		len(traces),
	)
	for i := range traces {
		id := "id-" + spanIDs[i]
		element, err := frame.Marshal(frame.Element{
			Subject: "test.subject",
			Headers: map[string][]string{
				jetstream.MsgIDHeader: {id},
				"traceparent":         {"00-" + traces[i] + "-" + spanIDs[i] + "-01"},
			},
			Payload: []byte(id),
		})
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(
			encoded,
			element,
		)
		ids = append(
			ids,
			id,
		)
	}
	framed := newMsg(
		"",
		1,
	)
	framed.data = frame.Join(encoded)
	for key, value := range frame.WireHeaders(ids) {
		framed.headers.Set(
			key,
			value,
		)
	}

	consumer := testConsumer(
		func(ctx context.Context, msgs []jetstream.Msg) []Result {
			for _, msg := range msgs {
				startWork(
					ctx,
					msg,
				)
			}

			return []Result{Ack(), Ack()}
		},
		5,
		nil,
	)
	consumer.processBatch(
		context.Background(),
		consumer.unpack(
			context.Background(),
			asMsgs(framed),
		),
	)

	items := spansNamed(
		recorder,
		"nats.consume",
	)
	works := workSpans(recorder)
	if len(items) != 2 || len(works) != 2 {
		t.Fatalf(
			"want 2 items and 2 work spans, got %d and %d",
			len(items),
			len(works),
		)
	}
	for i := range works {
		if works[i].Parent().SpanID() != items[i].SpanContext().SpanID() {
			t.Fatalf(
				"element %d work span not under its own item span",
				i,
			)
		}
		if works[i].SpanContext().TraceID().String() != traces[i] {
			t.Fatalf(
				"element %d trace %s want %s",
				i,
				works[i].SpanContext().TraceID(),
				traces[i],
			)
		}
	}
}

func TestItemContextMessageWithoutContextIsSafe(t *testing.T) {
	recordBatchSpans(t)
	plain := newMsg(
		"a",
		1,
	)

	var inside baggage.Baggage
	var foreign trace.SpanContext
	consumer := testConsumer(
		func(ctx context.Context, msgs []jetstream.Msg) []Result {
			inside = baggage.FromContext(ItemContext(
				ctx,
				msgs[0],
			))
			foreign = trace.SpanContextFromContext(ItemContext(
				context.Background(),
				newMsg(
					"other",
					1,
				),
			))

			return []Result{Ack()}
		},
		5,
		nil,
	)
	consumer.processBatch(
		context.Background(),
		asMsgs(plain),
	)

	if inside.Len() != 0 {
		t.Fatalf(
			"want no baggage, got %v",
			inside,
		)
	}
	if foreign.IsValid() {
		t.Fatal("message outside the batch must not get a span")
	}
}

func TestIsolateSelfSettledMessageSpanEndsWithRealVerdict(t *testing.T) {
	cases := []struct {
		name    string
		settle  func(*testing.T, jetstream.Msg)
		wantErr bool
	}{
		{
			name: "nak",
			settle: func(t *testing.T, msg jetstream.Msg) {
				mustSettle(
					t,
					msg.Nak(),
				)
			},
			wantErr: true,
		},
		{
			name: "term",
			settle: func(t *testing.T, msg jetstream.Msg) {
				mustSettle(
					t,
					msg.Term(),
				)
			},
			wantErr: true,
		},
		{
			name: "ack",
			settle: func(t *testing.T, msg jetstream.Msg) {
				mustSettle(
					t,
					msg.Ack(),
				)
			},
			wantErr: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := recordBatchSpans(t)
			consumer := testConsumer(
				func(_ context.Context, msgs []jetstream.Msg) []Result {
					tc.settle(
						t,
						msgs[0],
					)
					panic("boom")
				},
				5,
				nil,
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
			if len(items) != 1 {
				t.Fatalf(
					"want 1 item span, got %d",
					len(items),
				)
			}
			if got := items[0].Status().Code == codes.Error; got != tc.wantErr {
				t.Fatalf(
					"error status = %v want %v: %v",
					got,
					tc.wantErr,
					items[0].Status(),
				)
			}
		})
	}
}

func mustSettle(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Error(err)
	}
}
