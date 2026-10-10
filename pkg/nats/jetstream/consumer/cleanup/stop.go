package cleanup

import "context"

func (c *Cleaner) Stop(ctx context.Context) error {
	c.mu.Lock()
	cancel := c.cancel
	stopped := c.stopped
	c.mu.Unlock()

	if cancel == nil {
		return nil
	}
	cancel()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
