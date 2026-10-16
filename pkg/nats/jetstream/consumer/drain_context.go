package consumer

import "context"

func (d *Drainer) DrainContext(ctx context.Context) error {
	return d.drainAll(ctx)
}
