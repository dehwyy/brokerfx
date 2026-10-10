package cleanup

import (
	"context"
	"time"
)

func (c *Cleaner) RunOnce(ctx context.Context) (int64, error) {
	before := c.now().Add(-c.retention)

	var total int64
	for {
		deleted, err := deleteBatch(
			ctx,
			c.db,
			before,
			c.batchLimit,
		)
		total += deleted
		if err != nil {
			return total, err
		}
		if deleted < int64(c.batchLimit) {
			return total, nil
		}

		timer := time.NewTimer(c.pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return total, ctx.Err()
		case <-timer.C:
		}
	}
}
