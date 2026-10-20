package outbox

import (
	"math/rand/v2"
	"time"
)

func (r *OutboxRelay) retryDelay(attempts int) time.Duration {
	return r.jittered(nextAttemptDelay(
		attempts,
		r.config.RetryBackoffBase,
		r.config.RetryBackoffMax,
	))
}

func (r *OutboxRelay) unknownRetryDelay() time.Duration {
	return r.jittered(max(
		r.config.TxDrainPublishTimeout,
		txDrainUnknownRetryFloor,
	))
}

func (r *OutboxRelay) jittered(delay time.Duration) time.Duration {
	if !r.config.RetryJitter {
		return delay
	}

	spread := int64(delay / 2)
	if spread <= 0 {
		return delay
	}

	extended := delay + time.Duration(r.jitterUpTo(spread))
	if extended < delay {
		return delay
	}

	return extended
}

func (r *OutboxRelay) jitterUpTo(n int64) int64 {
	if r.jitter == nil {
		return rand.Int64N(n)
	}

	return r.jitter(n)
}
