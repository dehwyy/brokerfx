package batch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeBatch struct {
	msgs chan jetstream.Msg
	err  error
}

func (b *fakeBatch) Messages() <-chan jetstream.Msg { return b.msgs }
func (b *fakeBatch) Error() error                   { return b.err }

func batchOf(msgs []*fakeMsg, err error) *fakeBatch {
	ch := make(chan jetstream.Msg, len(msgs))
	for _, msg := range msgs {
		ch <- msg
	}
	close(ch)
	return &fakeBatch{msgs: ch, err: err}
}

type fetchCall struct {
	count int
}

type scriptedFetcher struct {
	mu     sync.Mutex
	calls  []fetchCall
	script []func(count int) (jetstream.MessageBatch, error)
	block  chan struct{}
}

func (f *scriptedFetcher) Fetch(count int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fetchCall{count: count})
	var step func(count int) (jetstream.MessageBatch, error)
	if len(f.script) > 0 {
		step = f.script[0]
		f.script = f.script[1:]
	}
	f.mu.Unlock()

	if step != nil {
		return step(count)
	}
	<-f.block
	return batchOf(nil, context.Canceled), nil
}

func (f *scriptedFetcher) snapshot() []fetchCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fetchCall(nil), f.calls...)
}

func loopConsumer(handler Handler, f *scriptedFetcher) *Consumer {
	c := testConsumer(handler, 5, nil)
	c.max = 8
	c.linger = 5 * time.Millisecond
	c.pipeline = 1
	c.fetcher = f
	return c
}

func ackAll(_ context.Context, msgs []jetstream.Msg) []Result {
	results := make([]Result, len(msgs))
	for i := range results {
		results[i] = Ack()
	}
	return results
}

func TestFetchIdleThenLinger(t *testing.T) {
	first := []*fakeMsg{newMsg("a", 1)}
	rest := []*fakeMsg{newMsg("b", 1), newMsg("c", 1), newMsg("d", 1), newMsg("e", 1), newMsg("f", 1)}
	f := &scriptedFetcher{
		block: make(chan struct{}),
		script: []func(int) (jetstream.MessageBatch, error){
			func(int) (jetstream.MessageBatch, error) { return batchOf(first, nil), nil },
			func(int) (jetstream.MessageBatch, error) { return batchOf(rest, nil), nil },
		},
	}
	var mu sync.Mutex
	var sizes []int
	handled := make(chan struct{})
	c := loopConsumer(func(ctx context.Context, msgs []jetstream.Msg) []Result {
		mu.Lock()
		sizes = append(sizes, len(msgs))
		mu.Unlock()
		close(handled)
		return ackAll(ctx, msgs)
	}, f)

	c.start()
	<-handled
	close(f.block)
	if err := c.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}

	calls := f.snapshot()
	if len(calls) < 2 || calls[0].count != 1 || calls[1].count != 7 {
		t.Fatalf("fetch calls %+v", calls)
	}
	if len(sizes) != 1 || sizes[0] != 6 {
		t.Fatalf("batch sizes %v", sizes)
	}
	for _, msg := range append(first, rest...) {
		expectSettles(t, msg, "ack")
	}
}

func TestDrainNaksFetchedNotStarted(t *testing.T) {
	msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1), newMsg("c", 1)}
	var calls atomic.Int32
	f := &scriptedFetcher{block: make(chan struct{})}
	c := loopConsumer(func(ctx context.Context, batch []jetstream.Msg) []Result {
		calls.Add(1)
		return ackAll(ctx, batch)
	}, f)
	f.script = []func(int) (jetstream.MessageBatch, error){
		func(int) (jetstream.MessageBatch, error) {
			c.cancel()
			return batchOf(msgs, nil), nil
		},
	}
	close(f.block)

	c.start()
	<-c.stopped
	if err := c.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}

	if calls.Load() != 0 {
		t.Fatalf("handler called %d", calls.Load())
	}
	for _, msg := range msgs {
		expectSettles(t, msg, "nak")
	}
}

func TestDrainWaitsInFlightBatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		wantErr error
	}{
		{name: "waits", timeout: 2 * time.Second},
		{name: "deadline", timeout: 50 * time.Millisecond, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msgs := []*fakeMsg{newMsg("a", 1), newMsg("b", 1)}
			release := make(chan struct{})
			entered := make(chan struct{})
			f := &scriptedFetcher{
				block: make(chan struct{}),
				script: []func(int) (jetstream.MessageBatch, error){
					func(int) (jetstream.MessageBatch, error) { return batchOf(msgs, nil), nil },
				},
			}
			close(f.block)
			c := loopConsumer(func(ctx context.Context, batch []jetstream.Msg) []Result {
				close(entered)
				<-release
				return ackAll(ctx, batch)
			}, f)
			c.max = 2

			c.start()
			<-entered

			drained := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
				defer cancel()
				drained <- c.Drain(ctx)
			}()

			if tc.wantErr != nil {
				err := <-drained
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v got %v", tc.wantErr, err)
				}
				close(release)
				if err := c.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
				return
			}

			select {
			case err := <-drained:
				t.Fatalf("drain returned early: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			if err := <-drained; err != nil {
				t.Fatal(err)
			}
			for _, msg := range msgs {
				expectSettles(t, msg, "ack")
			}
			if err := c.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFatalFetchErrorCallsOnFatal(t *testing.T) {
	f := &scriptedFetcher{
		block: make(chan struct{}),
		script: []func(int) (jetstream.MessageBatch, error){
			func(int) (jetstream.MessageBatch, error) { return nil, jetstream.ErrConsumerDeleted },
		},
	}
	var fatals atomic.Int32
	c := loopConsumer(ackAll, f)
	c.onFatal = func(err error) {
		if !errors.Is(err, jetstream.ErrConsumerDeleted) {
			t.Errorf("fatal err %v", err)
		}
		fatals.Add(1)
	}

	c.start()
	select {
	case <-c.stopped:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop")
	}

	if fatals.Load() != 1 {
		t.Fatalf("fatal calls %d", fatals.Load())
	}
}

func TestFatalBatchErrorNaksFetched(t *testing.T) {
	msgs := []*fakeMsg{newMsg("a", 1)}
	f := &scriptedFetcher{
		block: make(chan struct{}),
		script: []func(int) (jetstream.MessageBatch, error){
			func(int) (jetstream.MessageBatch, error) { return batchOf(msgs, jetstream.ErrConsumerNotFound), nil },
		},
	}
	var fatals atomic.Int32
	c := loopConsumer(ackAll, f)
	c.onFatal = func(error) { fatals.Add(1) }

	c.start()
	<-c.stopped

	if fatals.Load() != 1 {
		t.Fatalf("fatal calls %d", fatals.Load())
	}
	expectSettles(t, msgs[0], "nak")
}

func TestTransientFetchErrorKeepsLoopAlive(t *testing.T) {
	msgs := []*fakeMsg{newMsg("a", 1)}
	handled := make(chan struct{})
	f := &scriptedFetcher{
		block: make(chan struct{}),
		script: []func(int) (jetstream.MessageBatch, error){
			func(int) (jetstream.MessageBatch, error) { return nil, errors.New("connection reset") },
			func(int) (jetstream.MessageBatch, error) { return batchOf(msgs, nil), nil },
			func(int) (jetstream.MessageBatch, error) { return batchOf(nil, nil), nil },
		},
	}
	var fatals atomic.Int32
	c := loopConsumer(func(ctx context.Context, batch []jetstream.Msg) []Result {
		close(handled)
		return ackAll(ctx, batch)
	}, f)
	c.onFatal = func(error) { fatals.Add(1) }

	c.start()
	select {
	case <-handled:
	case <-time.After(3 * time.Second):
		t.Fatal("message not handled after transient error")
	}
	close(f.block)
	if err := c.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}

	if fatals.Load() != 0 {
		t.Fatalf("fatal calls %d", fatals.Load())
	}
	expectSettles(t, msgs[0], "ack")
}
