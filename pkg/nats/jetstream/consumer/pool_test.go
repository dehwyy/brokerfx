package consumer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolWaitCoversLateAcquire(t *testing.T) {
	var violations atomic.Int32
	for range 3000 {
		inFlight := newPool(1)
		if !inFlight.tryAcquire() {
			t.Fatal("the only slot is busy")
		}
		var running atomic.Int32
		running.Store(1)
		var drained atomic.Bool
		go func() {
			time.Sleep(50 * time.Microsecond)
			running.Add(-1)
			inFlight.release()
		}()
		late := make(chan struct{})
		go func() {
			defer close(late)
			if !inFlight.acquire() {
				return
			}
			if drained.Load() {
				violations.Add(1)
			}
			running.Add(1)
			time.Sleep(10 * time.Microsecond)
			running.Add(-1)
			inFlight.release()
		}()
		time.Sleep(5 * time.Microsecond)
		ctx, cancel := context.WithTimeout(
			context.Background(),
			2*time.Second,
		)
		err := inFlight.wait(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if running.Load() != 0 {
			violations.Add(1)
		}
		drained.Store(true)
		<-late
	}
	if violations.Load() != 0 {
		t.Fatalf(
			"wait returned while a handler could still run: %d times",
			violations.Load(),
		)
	}
}

func TestPoolWaitStopsWaitingAcquire(t *testing.T) {
	inFlight := newPool(1)
	if !inFlight.tryAcquire() {
		t.Fatal("the only slot is busy")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Millisecond,
	)
	defer cancel()
	if err := inFlight.wait(ctx); err == nil {
		t.Fatal("wait returned nil while the slot was busy")
	}
	if inFlight.acquire() {
		t.Fatal("acquire succeeded after the pool was stopped")
	}
	inFlight.release()

	ctx, cancel = context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()
	if err := inFlight.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if inFlight.tryAcquire() {
		t.Fatal("tryAcquire succeeded after the pool was drained")
	}
}
