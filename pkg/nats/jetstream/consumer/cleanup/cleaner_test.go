package cleanup

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
)

func TestMain(m *testing.M) {
	log.Logger = zerolog.Nop()
	m.Run()
}

type deleteCall struct {
	before time.Time
	limit  int
}

func stubDelete(t *testing.T, fn func(ctx context.Context, call deleteCall) (int64, error)) *[]deleteCall {
	t.Helper()
	var mu sync.Mutex
	calls := &[]deleteCall{}
	prev := deleteBatch
	deleteBatch = func(ctx context.Context, _ *gorm.DB, before time.Time, limit int) (int64, error) {
		call := deleteCall{before: before, limit: limit}
		mu.Lock()
		*calls = append(*calls, call)
		mu.Unlock()
		return fn(ctx, call)
	}
	t.Cleanup(func() { deleteBatch = prev })
	return calls
}

func testCleaner(t *testing.T, opts Opts) *Cleaner {
	t.Helper()
	opts.DB = &gorm.DB{}
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	c.pause = time.Millisecond
	return c
}

func TestRunOnceLoopsUntilShortBatch(t *testing.T) {
	answers := []int64{5000, 5000, 120}
	calls := stubDelete(t, func(context.Context, deleteCall) (int64, error) {
		answer := answers[0]
		answers = answers[1:]
		return answer, nil
	})
	c := testCleaner(t, Opts{})
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }

	total, err := c.RunOnce(context.Background())

	if err != nil || total != 10120 || len(*calls) != 3 {
		t.Fatalf("total %d err %v calls %d", total, err, len(*calls))
	}
	for _, call := range *calls {
		if !call.before.Equal(now.Add(-48*time.Hour)) || call.limit != 5000 {
			t.Fatalf("call %+v", call)
		}
	}
}

func TestRunOnceStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stubDelete(t, func(context.Context, deleteCall) (int64, error) {
		cancel()
		return 5000, nil
	})
	c := testCleaner(t, Opts{})
	c.pause = time.Second

	total, err := c.RunOnce(ctx)

	if !errors.Is(err, context.Canceled) || total != 5000 {
		t.Fatalf("total %d err %v", total, err)
	}
}

func TestStartRunsPeriodicallyAndStopWaits(t *testing.T) {
	var passes atomic.Int32
	var inPass atomic.Bool
	stubDelete(t, func(context.Context, deleteCall) (int64, error) {
		inPass.Store(true)
		passes.Add(1)
		time.Sleep(5 * time.Millisecond)
		inPass.Store(false)
		return 0, nil
	})
	c := testCleaner(t, Opts{Interval: 20 * time.Millisecond})

	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(95 * time.Millisecond)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if passes.Load() < 3 || inPass.Load() {
		t.Fatalf("passes %d in pass %v", passes.Load(), inPass.Load())
	}
	after := passes.Load()
	time.Sleep(50 * time.Millisecond)
	if passes.Load() != after {
		t.Fatal("passes continued after stop")
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestErrorGoesToOnErrorAndLoopLives(t *testing.T) {
	var passes atomic.Int32
	stubDelete(t, func(context.Context, deleteCall) (int64, error) {
		if passes.Add(1) == 1 {
			return 0, errors.New("db down")
		}
		return 0, nil
	})
	var errorsSeen atomic.Int32
	c := testCleaner(t, Opts{
		Interval: 10 * time.Millisecond,
		OnError:  func(error) { errorsSeen.Add(1) },
	})

	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for passes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if errorsSeen.Load() != 1 || passes.Load() < 2 {
		t.Fatalf("errors %d passes %d", errorsSeen.Load(), passes.Load())
	}
}

func TestStopWithoutStart(t *testing.T) {
	c := testCleaner(t, Opts{})
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
