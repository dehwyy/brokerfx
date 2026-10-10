package batch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var errBusiness = errors.New("business failure")

func TestProcessAppliesVerdictsPerMessage(t *testing.T) {
	poison := &poisonRecorder{}
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1), newMsg("c", 1), newMsg("d", 1)}
	c := testConsumer(resultsHandler(Ack(), Nak(errBusiness), NakDelay(2*time.Second, errBusiness), Term(errBusiness)), 5, poison)

	c.processBatch(context.Background(), asMsgs(msgs...))

	expectSettles(t, msgs[0], "ack")
	expectSettles(t, msgs[1], "nakdelay:1s")
	expectSettles(t, msgs[2], "nakdelay:2s")
	expectSettles(t, msgs[3], "term")
	calls := poison.snapshot()
	if len(calls) != 1 || calls[0].msg != msgs[3] || !errors.Is(calls[0].err, errBusiness) {
		t.Fatalf("poison calls %+v", calls)
	}
}

func TestPoisonInsideBatchOnlyPoisonTerminated(t *testing.T) {
	poison := &poisonRecorder{}
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1), newMsg("c", 1), newMsg("d", 1), newMsg("e", 1)}
	c := testConsumer(resultsHandler(Ack(), Ack(), Term(errBusiness), Ack(), Ack()), 5, poison)

	c.processBatch(context.Background(), asMsgs(msgs...))

	for i, msg := range msgs {
		if i == 2 {
			expectSettles(t, msg, "term")
			continue
		}
		expectSettles(t, msg, "ack")
	}
	calls := poison.snapshot()
	if len(calls) != 1 || calls[0].msg != msgs[2] {
		t.Fatalf("poison calls %+v", calls)
	}
}

func TestShortResultsNakRest(t *testing.T) {
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1), newMsg("c", 1)}
	c := testConsumer(resultsHandler(Ack()), 5, nil)

	c.processBatch(context.Background(), asMsgs(msgs...))

	expectSettles(t, msgs[0], "ack")
	expectSettles(t, msgs[1], "nakdelay:1s")
	expectSettles(t, msgs[2], "nakdelay:1s")

	poison := &poisonRecorder{}
	last := []*fakeMsg{newMsg("a", 1), newMsg("b", 1)}
	c = testConsumer(resultsHandler(Ack()), 1, poison)

	c.processBatch(context.Background(), asMsgs(last...))

	calls := poison.snapshot()
	if len(calls) != 1 || !errors.Is(calls[0].err, ErrMissingResult) {
		t.Fatalf("poison calls %+v", calls)
	}
}

func TestProcessExtraResultsIgnored(t *testing.T) {
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1)}
	c := testConsumer(resultsHandler(Ack(), Ack(), Ack(), Ack()), 5, nil)

	c.processBatch(context.Background(), asMsgs(msgs...))

	expectSettles(t, msgs[0], "ack")
	expectSettles(t, msgs[1], "ack")
}

func TestZeroResultIsNak(t *testing.T) {
	msgs := []*fakeMsg{newMsg("a", 1)}
	c := testConsumer(resultsHandler(Result{}), 5, nil)

	c.processBatch(context.Background(), asMsgs(msgs...))

	expectSettles(t, msgs[0], "nakdelay:1s")
}

func panicOnBoom(calls *atomic.Int32) Handler {
	return func(_ context.Context, msgs []jetstream.Msg) []Result {
		calls.Add(1)
		results := make([]Result, len(msgs))
		for i, msg := range msgs {
			if string(msg.Data()) == "boom" {
				panic("boom")
			}
			results[i] = Ack()
		}
		return results
	}
}

func TestHandlerPanicIsolatesMessages(t *testing.T) {
	var calls atomic.Int32
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("boom", 1), newMsg("c", 1)}
	c := testConsumer(panicOnBoom(&calls), 5, nil)

	c.processBatch(context.Background(), asMsgs(msgs...))

	if calls.Load() != 4 {
		t.Fatalf("handler calls %d", calls.Load())
	}
	expectSettles(t, msgs[0], "ack")
	expectSettles(t, msgs[1], "nakdelay:1s")
	expectSettles(t, msgs[2], "ack")
}

func TestPanicOnLastDeliveryIsPoison(t *testing.T) {
	var calls atomic.Int32
	poison := &poisonRecorder{}
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("boom", 3)}
	c := testConsumer(panicOnBoom(&calls), 3, poison)

	c.processBatch(context.Background(), asMsgs(msgs...))

	expectSettles(t, msgs[0], "ack")
	expectSettles(t, msgs[1], "term")
	pc := poison.snapshot()
	if len(pc) != 1 || !errors.Is(pc[0].err, ErrHandlerPanic) {
		t.Fatalf("poison calls %+v", pc)
	}
}

func TestNakOnLastDeliveryBecomesPoison(t *testing.T) {
	poison := &poisonRecorder{}
	msgs := []*fakeMsg{newMsg("a", 2)}
	c := testConsumer(resultsHandler(Nak(errBusiness)), 2, poison)

	c.processBatch(context.Background(), asMsgs(msgs...))

	expectSettles(t, msgs[0], "term")
	if len(poison.snapshot()) != 1 {
		t.Fatalf("poison calls %+v", poison.snapshot())
	}
}

func TestNakUsesLadderByDelivery(t *testing.T) {
	cases := []struct {
		delays    []time.Duration
		delivered uint64
		want      string
	}{
		{delivered: 1, want: "nakdelay:1s"},
		{delivered: 2, want: "nakdelay:5s"},
		{delivered: 3, want: "nakdelay:30s"},
		{delivered: 4, want: "nakdelay:2m0s"},
		{delivered: 5, want: "nakdelay:5m0s"},
		{delivered: 6, want: "nakdelay:5m0s"},
		{delivered: 9, want: "nakdelay:5m0s"},
		{delays: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}, delivered: 1, want: "nakdelay:10ms"},
		{delays: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}, delivered: 2, want: "nakdelay:20ms"},
		{delays: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}, delivered: 3, want: "nakdelay:20ms"},
	}

	for _, tc := range cases {
		msg := newMsg("a", tc.delivered)
		c := testConsumer(resultsHandler(Nak(errBusiness)), 10, nil)
		if tc.delays != nil {
			c.delays = tc.delays
		}

		c.processBatch(context.Background(), asMsgs(msg))

		expectSettles(t, msg, tc.want)
	}
}

func TestNakAtMaxAttemptsGoesToPoison(t *testing.T) {
	poison := &poisonRecorder{}
	msg := newMsg("a", 2)
	c := testConsumer(resultsHandler(Nak(errBusiness)), 3, poison)
	c.processBatch(context.Background(), asMsgs(msg))
	expectSettles(t, msg, "nakdelay:5s")
	if len(poison.snapshot()) != 0 {
		t.Fatalf("poison calls %+v", poison.snapshot())
	}

	msg = newMsg("a", 3)
	c.processBatch(context.Background(), asMsgs(msg))
	expectSettles(t, msg, "term")
	calls := poison.snapshot()
	if len(calls) != 1 || !errors.Is(calls[0].err, errBusiness) {
		t.Fatalf("poison calls %+v", calls)
	}

	msg = newMsg("a", 50)
	c = testConsumer(resultsHandler(Nak(errBusiness)), -1, poison)
	c.processBatch(context.Background(), asMsgs(msg))
	expectSettles(t, msg, "nakdelay:5m0s")
}

func TestNakWithoutErrorAtMaxAttemptsGetsExhaustedReason(t *testing.T) {
	poison := &poisonRecorder{}
	msg := newMsg("a", 3)
	c := testConsumer(resultsHandler(Nak(nil)), 3, poison)

	c.processBatch(context.Background(), asMsgs(msg))

	calls := poison.snapshot()
	if len(calls) != 1 || !errors.Is(calls[0].err, ErrAttemptsExhausted) {
		t.Fatalf("poison calls %+v", calls)
	}
}

func TestPoisonErrorSkipsRetries(t *testing.T) {
	poison := &poisonRecorder{}
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1)}
	c := testConsumer(resultsHandler(Nak(Poison(errBusiness)), NakDelay(time.Second, Poison(errBusiness))), 5, poison)

	c.processBatch(context.Background(), asMsgs(msgs...))

	expectSettles(t, msgs[0], "term")
	expectSettles(t, msgs[1], "term")
	calls := poison.snapshot()
	if len(calls) != 2 {
		t.Fatalf("poison calls %+v", calls)
	}
	for _, call := range calls {
		if !errors.Is(call.err, ErrPoison) || !errors.Is(call.err, errBusiness) {
			t.Fatalf("poison err %v", call.err)
		}
	}
}

func TestOnPoisonErrorNaksWithDelay(t *testing.T) {
	poison := &poisonRecorder{err: errors.New("dlq down")}
	msg := newMsg("a", 1)
	c := testConsumer(resultsHandler(Term(errBusiness)), 5, poison)

	c.processBatch(context.Background(), asMsgs(msg))

	expectSettles(t, msg, "nakdelay:1s")
}

func TestOnPoisonErrorRetriedOnRedelivery(t *testing.T) {
	poison := &poisonRecorder{err: errors.New("dlq down")}
	c := testConsumer(resultsHandler(Nak(errBusiness)), 3, poison)

	first := newMsg("a", 3)
	c.processBatch(context.Background(), asMsgs(first))
	expectSettles(t, first, "nakdelay:1s")

	poison.mu.Lock()
	poison.err = nil
	poison.mu.Unlock()

	redelivered := newMsg("a", 4)
	c.processBatch(context.Background(), asMsgs(redelivered))
	expectSettles(t, redelivered, "term")
	if len(poison.snapshot()) != 2 {
		t.Fatalf("poison calls %+v", poison.snapshot())
	}
}

func TestOnPoisonPanicNaksWithDelay(t *testing.T) {
	msg := newMsg("a", 1)
	c := testConsumer(resultsHandler(Term(errBusiness)), 5, nil)
	c.onPoison = func(context.Context, jetstream.Msg, error) error {
		panic("dlq panic")
	}

	c.processBatch(context.Background(), asMsgs(msg))

	expectSettles(t, msg, "nakdelay:1s")
}

func TestTermWithoutOnPoison(t *testing.T) {
	msg := newMsg("a", 1)
	c := testConsumer(resultsHandler(Term(errBusiness)), 5, nil)

	c.processBatch(context.Background(), asMsgs(msg))

	expectSettles(t, msg, "term")
}

func TestNoSettleBeforeHandlerReturns(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1)}
	c := testConsumer(func(_ context.Context, batch []jetstream.Msg) []Result {
		close(entered)
		<-release
		return []Result{Ack(), Ack()}
	}, 5, nil)

	done := make(chan struct{})
	go func() {
		c.processBatch(context.Background(), asMsgs(msgs...))
		close(done)
	}()

	<-entered
	time.Sleep(20 * time.Millisecond)
	for _, msg := range msgs {
		if len(msg.settles()) != 0 {
			t.Fatalf("settled before handler returned: %v", msg.settles())
		}
	}
	close(release)
	<-done
	expectSettles(t, msgs[0], "ack")
	expectSettles(t, msgs[1], "ack")
}

func TestHandlerSettledItselfIsFine(t *testing.T) {
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1)}
	c := testConsumer(func(_ context.Context, batch []jetstream.Msg) []Result {
		if err := batch[0].Ack(); err != nil {
			t.Errorf("ack %v", err)
		}
		return []Result{Ack(), Ack()}
	}, 5, nil)

	c.processBatch(context.Background(), asMsgs(msgs...))

	expectSettles(t, msgs[0], "ack", "ack")
	expectSettles(t, msgs[1], "ack")
}
