package batch

import "context"

func (c *Consumer) Drain(ctx context.Context) error {
	c.cancel()

	select {
	case <-c.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
