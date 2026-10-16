package consumer

import (
	"context"
	"sync"
)

type pool struct {
	slots   chan struct{}
	stopped chan struct{}
	stop    sync.Once
	drain   sync.Mutex
	held    int
}

func newPool(size int) *pool {
	if size <= 0 {
		return nil
	}

	return &pool{
		slots: make(
			chan struct{},
			size,
		),
		stopped: make(chan struct{}),
	}
}

func (p *pool) acquire() bool {
	select {
	case p.slots <- struct{}{}:
		return true
	case <-p.stopped:
		return false
	}
}

func (p *pool) tryAcquire() bool {
	select {
	case p.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (p *pool) release() {
	<-p.slots
}

func (p *pool) wait(ctx context.Context) error {
	if p == nil {
		return nil
	}

	p.stop.Do(func() {
		close(p.stopped)
	})

	p.drain.Lock()
	defer p.drain.Unlock()
	for p.held < cap(p.slots) {
		select {
		case p.slots <- struct{}{}:
			p.held++
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
