package replywait

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

type Waiter struct {
	id     string
	reg    *registry
	result chan jetstream.Msg
	fail   chan error
}

func (w *Waiter) Wait(ctx context.Context) (jetstream.Msg, error) {
	select {
	case msg := <-w.result:
		return msg, nil
	case err := <-w.fail:
		return nil, err
	case <-ctx.Done():
		w.reg.remove(w.id)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: correlation %s", ErrTimeout, w.id)
		}
		return nil, ctx.Err()
	}
}

func (w *Waiter) Cancel() {
	w.reg.remove(w.id)
}

type registry struct {
	mu      sync.Mutex
	entries map[string]*Waiter
}

func newRegistry() *registry {
	return &registry{
		entries: make(map[string]*Waiter),
	}
}

func (r *registry) register(id string) (*Waiter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.entries[id]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateCorrelation, id)
	}

	w := &Waiter{
		id:     id,
		reg:    r,
		result: make(chan jetstream.Msg, 1),
		fail:   make(chan error, 1),
	}
	r.entries[id] = w

	return w, nil
}

func (r *registry) resolve(id string, msg jetstream.Msg) bool {
	r.mu.Lock()
	w, ok := r.entries[id]
	if ok {
		delete(r.entries, id)
	}
	r.mu.Unlock()

	if !ok {
		return false
	}

	w.result <- msg
	return true
}

func (r *registry) failAll(err error) {
	r.mu.Lock()
	entries := r.entries
	r.entries = make(map[string]*Waiter)
	r.mu.Unlock()

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
	delete(r.entries, id)
	r.mu.Unlock()
}
