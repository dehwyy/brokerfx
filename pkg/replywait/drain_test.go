package replywait

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeListenerStopper struct {
	calls int
}

func (f *fakeListenerStopper) stop() {
	f.calls++
}

func testDrainWaiterConfig() Config {
	cfg := testWaiterConfig()
	cfg.DrainTimeout = time.Second
	return cfg
}

func TestReplyWaiterDrainingFalseBeforeBeginDrain(t *testing.T) {
	rw := newReplyWaiter(testDrainWaiterConfig(), newRegistry(), nil)

	if rw.Draining() {
		t.Fatalf("expected Draining to be false before BeginDrain")
	}
}

func TestReplyWaiterBeginDrainSetsDraining(t *testing.T) {
	rw := newReplyWaiter(testDrainWaiterConfig(), newRegistry(), nil)

	rw.BeginDrain()

	if !rw.Draining() {
		t.Fatalf("expected Draining to be true after BeginDrain")
	}
}

func TestReplyWaiterRegisterDuringDrainReturnsErrDraining(t *testing.T) {
	reg := newRegistry()
	rw := newReplyWaiter(testDrainWaiterConfig(), reg, nil)

	rw.BeginDrain()

	_, err := rw.Register("corr-1")
	if !errors.Is(err, ErrDraining) {
		t.Fatalf("expected ErrDraining, got %v", err)
	}
	if got := reg.inflight(); got != 0 {
		t.Fatalf("expected no entry registered during drain, inflight=%d", got)
	}
}

func TestReplyWaiterRequestDuringDrainReturnsErrDrainingWithoutPublish(t *testing.T) {
	reg := newRegistry()
	rw := newReplyWaiter(testDrainWaiterConfig(), reg, nil)

	rw.BeginDrain()

	published := false
	_, err := rw.Request(
		context.Background(),
		"corr-1",
		func(context.Context) error {
			published = true
			return nil
		},
	)
	if !errors.Is(err, ErrDraining) {
		t.Fatalf("expected ErrDraining, got %v", err)
	}
	if published {
		t.Fatalf("expected publish not to be called during drain")
	}
}

func TestReplyWaiterDrainWaitsForInflightThenSucceeds(t *testing.T) {
	reg := newRegistry()
	rw := newReplyWaiter(testDrainWaiterConfig(), reg, nil)

	waiter, err := rw.Register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	waitResult := make(chan error, 1)
	go func() {
		_, waitErr := waiter.Wait(context.Background())
		waitResult <- waitErr
	}()

	go func() {
		time.Sleep(20 * time.Millisecond)
		reg.resolve("corr-1", &fakeMsg{subject: "corr-1"})
	}()

	rw.BeginDrain()

	if err := rw.Drain(context.Background()); err != nil {
		t.Fatalf("unexpected drain error: %v", err)
	}

	select {
	case waitErr := <-waitResult:
		if waitErr != nil {
			t.Fatalf("expected in-flight waiter to receive its reply, got error: %v", waitErr)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for in-flight waiter to resolve")
	}

	if got := reg.inflight(); got != 0 {
		t.Fatalf("expected inflight=0 after drain, got %d", got)
	}
}

func TestReplyWaiterDrainTimesOutAndFailsRemaining(t *testing.T) {
	reg := newRegistry()
	cfg := testDrainWaiterConfig()
	cfg.DrainTimeout = 30 * time.Millisecond
	rw := newReplyWaiter(cfg, reg, nil)

	waiter, err := rw.Register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	rw.BeginDrain()

	err = rw.Drain(context.Background())
	if !errors.Is(err, ErrDrained) {
		t.Fatalf("expected ErrDrained, got %v", err)
	}

	if got := reg.inflight(); got != 0 {
		t.Fatalf("expected inflight=0 after drain timeout, got %d", got)
	}

	_, waitErr := waiter.Wait(context.Background())
	if !errors.Is(waitErr, ErrDrained) {
		t.Fatalf("expected remaining waiter to receive ErrDrained, got %v", waitErr)
	}
}

func TestReplyWaiterDrainRespectsShorterCtxDeadlineThanDrainTimeout(t *testing.T) {
	reg := newRegistry()
	cfg := testDrainWaiterConfig()
	cfg.DrainTimeout = time.Minute
	rw := newReplyWaiter(cfg, reg, nil)

	if _, err := rw.Register("corr-1"); err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	rw.BeginDrain()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := rw.Drain(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrDrained) {
		t.Fatalf("expected ErrDrained, got %v", err)
	}
	if elapsed >= cfg.DrainTimeout {
		t.Fatalf("expected ctx deadline (shorter than DrainTimeout) to win, waited %v", elapsed)
	}
}

func TestReplyWaiterDrainStopsListener(t *testing.T) {
	reg := newRegistry()
	rw := newReplyWaiter(testDrainWaiterConfig(), reg, nil)

	stopper := &fakeListenerStopper{}
	rw.attachListener(stopper)

	rw.BeginDrain()

	if err := rw.Drain(context.Background()); err != nil {
		t.Fatalf("unexpected drain error: %v", err)
	}

	if stopper.calls != 1 {
		t.Fatalf("expected listener stop called exactly once, got %d", stopper.calls)
	}
}

func TestReplyWaiterDrainSecondCallIsNoOp(t *testing.T) {
	reg := newRegistry()
	rw := newReplyWaiter(testDrainWaiterConfig(), reg, nil)

	stopper := &fakeListenerStopper{}
	rw.attachListener(stopper)

	rw.BeginDrain()

	firstErr := rw.Drain(context.Background())
	secondErr := rw.Drain(context.Background())

	if !errors.Is(secondErr, firstErr) && secondErr != firstErr {
		t.Fatalf("expected repeat Drain to return same result, first=%v second=%v", firstErr, secondErr)
	}
	if stopper.calls != 1 {
		t.Fatalf("expected listener stop called exactly once across repeat drains, got %d", stopper.calls)
	}
}
