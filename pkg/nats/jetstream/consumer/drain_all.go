package consumer

import (
	"context"
	"errors"
	"sync"
)

func (d *Drainer) drainAll(ctx context.Context) error {
	failures := make(
		chan error,
		len(d.consumers),
	)

	var wg sync.WaitGroup
	for _, consumer := range d.consumers {
		wg.Add(1)
		go func(consumer Consumer) {
			defer wg.Done()
			failures <- consumer.drain(ctx)
		}(consumer)
	}
	wg.Wait()
	close(failures)

	var joined error
	for err := range failures {
		joined = errors.Join(
			joined,
			err,
		)
	}

	return joined
}

func (c Consumer) drain(ctx context.Context) error {
	c.consumeCtx.Drain()

	select {
	case <-c.consumeCtx.Closed():
	case <-ctx.Done():
		return ctx.Err()
	}

	return c.inFlight.wait(ctx)
}
