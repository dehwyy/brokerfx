package consumer

import (
	"context"
	"sync"
)

type pool struct {
	slots chan struct{}
	busy  sync.WaitGroup
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
	}
}

func (p *pool) acquire() {
	p.slots <- struct{}{}
	p.busy.Add(1)
}

func (p *pool) release() {
	p.busy.Done()
	<-p.slots
}

func (p *pool) wait(ctx context.Context) error {
	if p == nil {
		return nil
	}

	idle := make(chan struct{})
	go func() {
		p.busy.Wait()
		close(idle)
	}()

	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
