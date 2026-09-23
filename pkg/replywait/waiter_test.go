package replywait

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeWaker struct {
	calls int
}

func (w *fakeWaker) WakeupRelay() {
	w.calls++
}

func testWaiterConfig() Config {
	return Config{
		Stream:            "BALANCE_REPLY",
		ReplySubject:      "balance.result.balanceapi.pod-1",
		CorrelationFunc:   func(msg jetstream.Msg) (string, error) { return msg.Subject(), nil },
		DefaultTimeout:    time.Second,
		InactiveThreshold: time.Minute,
	}
}

func TestReplyWaiterRequestResolvedDuringPublish(t *testing.T) {
	reg := newRegistry()
	waker := &fakeWaker{}
	rw := newReplyWaiter(testWaiterConfig(), reg, waker)

	msg := &fakeMsg{subject: "corr-1"}

	got, err := rw.Request(
		context.Background(),
		"corr-1",
		func(context.Context) error {
			if ok := reg.resolve("corr-1", msg); !ok {
				t.Fatalf("expected resolve to find registered waiter")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected request error: %v", err)
	}
	if got.Subject() != msg.Subject() {
		t.Fatalf("expected msg %q, got %q", msg.Subject(), got.Subject())
	}
	if waker.calls != 1 {
		t.Fatalf("expected wake exactly once, got %d", waker.calls)
	}
}

func TestReplyWaiterRequestPublishErrorRemovesEntryAndSkipsWake(t *testing.T) {
	reg := newRegistry()
	waker := &fakeWaker{}
	rw := newReplyWaiter(testWaiterConfig(), reg, waker)

	wantErr := errors.New("publish boom")

	_, err := rw.Request(
		context.Background(),
		"corr-1",
		func(context.Context) error { return wantErr },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected publish error, got %v", err)
	}
	if got := reg.inflight(); got != 0 {
		t.Fatalf("expected entry removed after publish error, inflight=%d", got)
	}
	if waker.calls != 0 {
		t.Fatalf("expected wake not called on publish error, got %d calls", waker.calls)
	}
}

func TestReplyWaiterRequestSuccessWakesExactlyOnce(t *testing.T) {
	reg := newRegistry()
	waker := &fakeWaker{}
	rw := newReplyWaiter(testWaiterConfig(), reg, waker)

	msg := &fakeMsg{subject: "corr-1"}

	_, err := rw.Request(
		context.Background(),
		"corr-1",
		func(context.Context) error {
			go reg.resolve("corr-1", msg)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected request error: %v", err)
	}
	if waker.calls != 1 {
		t.Fatalf("expected wake exactly once, got %d", waker.calls)
	}
}

func TestReplyWaiterRequestNoDeadlineUsesDefaultTimeout(t *testing.T) {
	reg := newRegistry()
	cfg := testWaiterConfig()
	cfg.DefaultTimeout = 20 * time.Millisecond
	rw := newReplyWaiter(cfg, reg, nil)

	start := time.Now()
	_, err := rw.Request(
		context.Background(),
		"corr-1",
		func(context.Context) error { return nil },
	)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if elapsed < cfg.DefaultTimeout {
		t.Fatalf("expected wait to last at least default timeout %v, got %v", cfg.DefaultTimeout, elapsed)
	}
	if elapsed > cfg.DefaultTimeout+500*time.Millisecond {
		t.Fatalf("expected wait to stop near default timeout %v, got %v", cfg.DefaultTimeout, elapsed)
	}
}

func TestReplyWaiterRequestShorterCtxDeadlineWins(t *testing.T) {
	reg := newRegistry()
	cfg := testWaiterConfig()
	cfg.DefaultTimeout = time.Minute
	rw := newReplyWaiter(cfg, reg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := rw.Request(
		ctx,
		"corr-1",
		func(context.Context) error { return nil },
	)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if elapsed >= cfg.DefaultTimeout {
		t.Fatalf("expected ctx deadline (shorter than default) to win, waited %v", elapsed)
	}
}

func TestReplyWaiterRequestExpiredCtxReturnsTimeoutImmediately(t *testing.T) {
	reg := newRegistry()
	rw := newReplyWaiter(testWaiterConfig(), reg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	start := time.Now()
	_, err := rw.Request(
		ctx,
		"corr-1",
		func(context.Context) error { return nil },
	)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout for already expired ctx, got %v", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected immediate return for expired ctx, took %v", elapsed)
	}
	if got := reg.inflight(); got != 0 {
		t.Fatalf("expected entry removed after expired ctx, inflight=%d", got)
	}
}

func TestReplyWaiterReplySubjectReturnsConfiguredSubject(t *testing.T) {
	reg := newRegistry()
	cfg := testWaiterConfig()
	rw := newReplyWaiter(cfg, reg, nil)

	if got := rw.ReplySubject(); got != cfg.ReplySubject {
		t.Fatalf("expected reply subject %q, got %q", cfg.ReplySubject, got)
	}
}

func TestReplyWaiterRegisterAllowsSeparatePublishAndWait(t *testing.T) {
	reg := newRegistry()
	rw := newReplyWaiter(testWaiterConfig(), reg, nil)

	w, err := rw.Register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	msg := &fakeMsg{subject: "corr-1"}
	if ok := reg.resolve("corr-1", msg); !ok {
		t.Fatalf("expected resolve to find registered waiter")
	}

	got, err := w.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if got.Subject() != msg.Subject() {
		t.Fatalf("expected msg %q, got %q", msg.Subject(), got.Subject())
	}
}
