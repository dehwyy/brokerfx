package outbox

import (
	"context"
	"time"
)

func (r *OutboxRelay) lingerAfterWakeup(ctx context.Context) {
	if !r.config.Batched || r.config.Linger <= 0 {
		return
	}

	timer := time.NewTimer(r.config.Linger)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
