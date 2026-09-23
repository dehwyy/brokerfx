package replywait

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type Waiter struct {
	id        string
	reg       *registry
	observer  Observer
	startedAt time.Time
	result    chan jetstream.Msg
	fail      chan error
}

func (w *Waiter) Wait(ctx context.Context) (jetstream.Msg, error) {
	select {
	case msg := <-w.result:
		w.observer.WaitResolved(w.id, time.Since(w.startedAt))
		return msg, nil
	case err := <-w.fail:
		if errors.Is(err, ErrDrained) {
			w.observer.WaitDrained(w.id)
		}
		return nil, err
	case <-ctx.Done():
		w.reg.remove(w.id)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			w.observer.WaitTimedOut(w.id)
			return nil, fmt.Errorf("%w: correlation %s", ErrTimeout, w.id)
		}
		return nil, ctx.Err()
	}
}

func (w *Waiter) Cancel() {
	w.reg.remove(w.id)
}

type registry struct {
	mu       sync.Mutex
	entries  map[string]*Waiter
	observer Observer
}

func newRegistry() *registry {
	return &registry{
		entries:  make(map[string]*Waiter),
		observer: NopObserver{},
	}
}

func (r *registry) attachObserver(o Observer) {
	if o == nil {
		return
	}

	r.mu.Lock()
	r.observer = o
	r.mu.Unlock()
}

func (r *registry) register(id string) (*Waiter, error) {
	r.mu.Lock()

	if _, exists := r.entries[id]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrDuplicateCorrelation, id)
	}

	w := &Waiter{
		id:        id,
		reg:       r,
		observer:  r.observer,
		startedAt: time.Now(),
		result:    make(chan jetstream.Msg, 1),
		fail:      make(chan error, 1),
	}
	r.entries[id] = w
	count := len(r.entries)
	observer := r.observer
	r.mu.Unlock()

	observer.WaitStarted(id)
	observer.Inflight(count)

	return w, nil
}

func (r *registry) resolve(id string, msg jetstream.Msg) bool {
	r.mu.Lock()
	w, ok := r.entries[id]
	if ok {
		delete(r.entries, id)
	}
	count := len(r.entries)
	observer := r.observer
	r.mu.Unlock()

	if !ok {
		return false
	}

	observer.Inflight(count)
	w.result <- msg
	return true
}

func (r *registry) failAll(err error) {
	r.mu.Lock()
	entries := r.entries
	r.entries = make(map[string]*Waiter)
	observer := r.observer
	r.mu.Unlock()

	observer.Inflight(0)

	for _, w := range entries {
		w.fail <- err
	}
}

func (r *registry) inflight() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.entries)
}

func (r *registry) remove(id string) {
	r.mu.Lock()
	_, existed := r.entries[id]
	delete(r.entries, id)
	count := len(r.entries)
	observer := r.observer
	r.mu.Unlock()

	if existed {
		observer.Inflight(count)
	}
}
