package outbox

import "time"

type PendingObserver interface {
	OnPending(stats PendingStats)
}

type PendingStats struct {
	Pending          int64
	Capped           bool
	OldestPendingAge time.Duration
}
