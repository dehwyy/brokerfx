package outbox

import (
	"context"
	"time"
)

func (r *OutboxRelay) pendingGaugeTicker() (<-chan time.Time, func()) {
	if r.config.PendingGaugeInterval <= 0 {
		return nil, func() {}
	}

	if _, ok := r.observer.(PendingObserver); !ok {
		r.logger.Warn().
			Dur("interval", r.config.PendingGaugeInterval).
			Msg("outbox pending gauge is set but the observer does not implement PendingObserver, gauge off")
		return nil, func() {}
	}

	ticker := time.NewTicker(r.config.PendingGaugeInterval)

	return ticker.C, ticker.Stop
}

func (r *OutboxRelay) sendPending(ctx context.Context) {
	observer, ok := r.observer.(PendingObserver)
	if !ok {
		return
	}

	stats, err := r.store.pendingStats(ctx)
	if err != nil {
		r.logger.Error().Err(err).Msg("failed to compute outbox pending gauge")
		return
	}

	observer.OnPending(stats)
}
