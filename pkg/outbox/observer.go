package outbox

import "time"

type Observer interface {
	OnPublished(n int)
	OnFailed(n int)
	OnParked(eventID, subject, lastError string)
	OnStats(stats Stats)
}

type Stats struct {
	Pending          int64
	InFlight         int64
	Done             int64
	Parked           int64
	OldestPendingAge time.Duration
}
