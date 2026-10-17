package batch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type frameRecorder struct {
	noopObserver
	events []FrameEvent
}

func (r *frameRecorder) OnFrame(event FrameEvent) { r.events = append(r.events, event) }

func frameMsg(t *testing.T, delivered uint64, payloads ...string) *fakeMsg {
	t.Helper()
	encoded := make([][]byte, 0, len(payloads))
	ids := make([]string, 0, len(payloads))
	for _, payload := range payloads {
		element, err := frame.Marshal(frame.Element{
			Subject: "test.subject",
			Headers: map[string][]string{jetstream.MsgIDHeader: {"id-" + payload}},
			Payload: []byte(payload),
		})
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, element)
		ids = append(ids, "id-"+payload)
	}
	msg := newMsg("", delivered)
	msg.data = frame.Join(encoded)
	for key, value := range frame.WireHeaders(ids) {
		msg.headers.Set(key, value)
	}
	return msg
}

func TestFrameElementsGetTheirOwnVerdicts(t *testing.T) {
	poison := &poisonRecorder{}
	observer := &frameRecorder{}
	plain := newMsg("plain", 1)
	framed := frameMsg(t, 1, "a", "b", "c", "d")
	var seen []string
	handler := func(_ context.Context, msgs []jetstream.Msg) []Result {
		for _, msg := range msgs {
			seen = append(seen, string(msg.Data()))
		}
		return []Result{Ack(), Ack(), Nak(errBusiness), NakDelay(3*time.Second, errBusiness), Term(errBusiness)}
	}
	c := testConsumer(handler, 5, poison)
	c.observer = observer
	c.heartbeatEvery = time.Hour

	c.processBatch(context.Background(), c.unpack(context.Background(), asMsgs(plain, framed)))

	if len(seen) != 5 || seen[0] != "plain" || seen[4] != "d" {
		t.Fatalf("handler saw %v", seen)
	}
	expectSettles(t, plain, "ack")
	expectSettles(t, framed, "nakdelay:3s")
	calls := poison.snapshot()
	if len(calls) != 1 || string(calls[0].msg.Data()) != "d" || calls[0].msg.Headers().Get(jetstream.MsgIDHeader) != "id-d" {
		t.Fatalf("poison must get the element, got %+v", calls)
	}
	if len(observer.events) != 1 || observer.events[0].Elements != 4 || observer.events[0].Err != nil {
		t.Fatalf("frame events %+v", observer.events)
	}
}

func TestFrameAllAckOrTermAcksTheFrame(t *testing.T) {
	framed := frameMsg(t, 1, "a", "b")
	c := testConsumer(resultsHandler(Ack(), Term(errBusiness)), 5, &poisonRecorder{})

	c.processBatch(context.Background(), c.unpack(context.Background(), asMsgs(framed)))

	expectSettles(t, framed, "ack")
}

func TestFrameElementPoisonedAtMaxAttempts(t *testing.T) {
	poison := &poisonRecorder{}
	framed := frameMsg(t, 5, "a", "b")
	c := testConsumer(resultsHandler(Ack(), Nak(errBusiness)), 5, poison)

	c.processBatch(context.Background(), c.unpack(context.Background(), asMsgs(framed)))

	expectSettles(t, framed, "ack")
	calls := poison.snapshot()
	if len(calls) != 1 || string(calls[0].msg.Data()) != "b" || !errors.Is(calls[0].err, errBusiness) {
		t.Fatalf("poison calls %+v", calls)
	}
}

func TestBrokenFrameGoesWholeToPoison(t *testing.T) {
	poison := &poisonRecorder{}
	observer := &frameRecorder{}
	broken := newMsg("garbage", 1)
	broken.headers = nats.Header{}
	broken.headers.Set(frame.HeaderVersion, "9")
	c := testConsumer(resultsHandler(), 5, poison)
	c.observer = observer

	if got := c.unpack(context.Background(), asMsgs(broken)); len(got) != 0 {
		t.Fatalf("broken frame must not reach the handler: %d", len(got))
	}
	expectSettles(t, broken, "term")
	calls := poison.snapshot()
	if len(calls) != 1 || calls[0].msg != broken || !errors.Is(calls[0].err, frame.ErrUnknownVersion) {
		t.Fatalf("poison calls %+v", calls)
	}
	if len(observer.events) != 1 || !errors.Is(observer.events[0].Err, frame.ErrUnknownVersion) {
		t.Fatalf("frame events %+v", observer.events)
	}
}

func TestFrameHandlerPanicIsolatesElements(t *testing.T) {
	framed := frameMsg(t, 1, "a", "b", "c")
	calls := 0
	handler := func(_ context.Context, msgs []jetstream.Msg) []Result {
		calls++
		for _, msg := range msgs {
			if string(msg.Data()) == "b" {
				panic("boom")
			}
		}
		return ackAllResults(len(msgs))
	}
	c := testConsumer(handler, 5, &poisonRecorder{})

	c.processBatch(context.Background(), c.unpack(context.Background(), asMsgs(framed)))

	expectSettles(t, framed, "nakdelay:1s")
}

func ackAllResults(n int) []Result {
	results := make([]Result, n)
	for i := range results {
		results[i] = Ack()
	}
	return results
}
