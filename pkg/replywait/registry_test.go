package replywait

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeMsg struct {
	jetstream.Msg

	subject string
}

func (m *fakeMsg) Subject() string { return m.subject }

func TestRegistryRegisterResolveWait(t *testing.T) {
	r := newRegistry()

	w, err := r.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	msg := &fakeMsg{subject: "balance.result.balanceapi.pod-1"}

	if ok := r.resolve("corr-1", msg); !ok {
		t.Fatalf("expected resolve to succeed")
	}

	got, err := w.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if got.Subject() != msg.Subject() {
		t.Fatalf("expected msg %q, got %q", msg.Subject(), got.Subject())
	}
}

func TestRegistryResolveBeforeWaitNotLost(t *testing.T) {
	r := newRegistry()

	w, err := r.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	msg := &fakeMsg{subject: "balance.result.balanceapi.pod-1"}
	r.resolve("corr-1", msg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := w.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if got.Subject() != msg.Subject() {
		t.Fatalf("expected msg %q, got %q", msg.Subject(), got.Subject())
	}
}

func TestRegistrySecondResolveReturnsFalse(t *testing.T) {
	r := newRegistry()

	if _, err := r.register("corr-1"); err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	msg := &fakeMsg{subject: "s1"}
	if ok := r.resolve("corr-1", msg); !ok {
		t.Fatalf("expected first resolve to succeed")
	}
	if ok := r.resolve("corr-1", msg); ok {
		t.Fatalf("expected second resolve to fail")
	}
}

func TestRegistryResolveUnknownIDReturnsFalse(t *testing.T) {
	r := newRegistry()

	if ok := r.resolve("unknown", &fakeMsg{}); ok {
		t.Fatalf("expected resolve of unknown id to fail")
	}
}

func TestRegistryRegisterDuplicateLiveID(t *testing.T) {
	r := newRegistry()

	if _, err := r.register("corr-1"); err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	_, err := r.register("corr-1")
	if err == nil {
		t.Fatalf("expected error for duplicate live correlation id")
	}
	if !errors.Is(err, ErrDuplicateCorrelation) {
		t.Fatalf("expected error to wrap ErrDuplicateCorrelation, got %v", err)
	}
}

func TestRegistryWaitDeadlineExceeded(t *testing.T) {
	r := newRegistry()

	w, err := r.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = w.Wait(ctx)
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected error to wrap ErrTimeout, got %v", err)
	}

	if got := r.inflight(); got != 0 {
		t.Fatalf("expected entry removed after timeout, inflight=%d", got)
	}
}

func TestRegistryWaitCanceled(t *testing.T) {
	r := newRegistry()

	w, err := r.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = w.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if got := r.inflight(); got != 0 {
		t.Fatalf("expected entry removed after cancel, inflight=%d", got)
	}
}

func TestRegistryFailAll(t *testing.T) {
	r := newRegistry()

	w1, err := r.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}
	w2, err := r.register("corr-2")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	r.failAll(ErrDrained)

	if got := r.inflight(); got != 0 {
		t.Fatalf("expected inflight=0 after failAll, got %d", got)
	}

	for _, w := range []*Waiter{w1, w2} {
		_, err := w.Wait(context.Background())
		if !errors.Is(err, ErrDrained) {
			t.Fatalf("expected ErrDrained, got %v", err)
		}
	}
}

func TestRegistryReportsWaitStartedAndResolved(t *testing.T) {
	r := newRegistry()
	obs := &fakeObserver{}
	r.attachObserver(obs)

	w, err := r.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	if got := obs.snapshotStarted(); len(got) != 1 || got[0] != "corr-1" {
		t.Fatalf("expected WaitStarted(corr-1), got %v", got)
	}
	if got := obs.lastInflight(); got != 1 {
		t.Fatalf("expected inflight=1 after register, got %d", got)
	}

	r.resolve("corr-1", &fakeMsg{subject: "corr-1"})
	if _, err := w.Wait(context.Background()); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	if got := obs.snapshotResolved(); len(got) != 1 || got[0] != "corr-1" {
		t.Fatalf("expected WaitResolved(corr-1), got %v", got)
	}
	if got := obs.lastInflight(); got != 0 {
		t.Fatalf("expected inflight=0 after resolve, got %d", got)
	}
}

func TestRegistryReportsWaitTimedOut(t *testing.T) {
	r := newRegistry()
	obs := &fakeObserver{}
	r.attachObserver(obs)

	w, err := r.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if _, err := w.Wait(ctx); !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}

	if got := obs.snapshotTimedOut(); len(got) != 1 || got[0] != "corr-1" {
		t.Fatalf("expected WaitTimedOut(corr-1), got %v", got)
	}
}

func TestRegistryReportsWaitDrained(t *testing.T) {
	r := newRegistry()
	obs := &fakeObserver{}
	r.attachObserver(obs)

	w1, err := r.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}
	w2, err := r.register("corr-2")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	r.failAll(ErrDrained)

	for _, w := range []*Waiter{w1, w2} {
		if _, err := w.Wait(context.Background()); !errors.Is(err, ErrDrained) {
			t.Fatalf("expected ErrDrained, got %v", err)
		}
	}

	got := obs.snapshotDrained()
	if len(got) != 2 {
		t.Fatalf("expected 2 WaitDrained calls, got %v", got)
	}
	if got := obs.lastInflight(); got != 0 {
		t.Fatalf("expected inflight=0 after failAll, got %d", got)
	}
}

func TestRegistryConcurrentPairsLeaveNoEntries(t *testing.T) {
	r := newRegistry()

	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()

			id := fmt.Sprintf("corr-%d", i)
			w, err := r.register(id)
			if err != nil {
				t.Errorf("unexpected register error for %s: %v", id, err)
				return
			}

			msg := &fakeMsg{subject: id}
			go r.resolve(id, msg)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			got, err := w.Wait(ctx)
			if err != nil {
				t.Errorf("unexpected wait error for %s: %v", id, err)
				return
			}
			if got.Subject() != id {
				t.Errorf("expected subject %q, got %q", id, got.Subject())
			}
		}(i)
	}

	wg.Wait()

	if got := r.inflight(); got != 0 {
		t.Fatalf("expected no leftover entries, inflight=%d", got)
	}
}
