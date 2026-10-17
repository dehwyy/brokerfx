package pgtimer

import "time"

type Observer interface {
	OnClaim(event ClaimEvent)
	OnFire(event FireEvent)
}

type ClaimEvent struct {
	Limit    int
	Claimed  int
	Duration time.Duration
	Err      error
}

type FireEvent struct {
	Name     string
	Lag      time.Duration
	Duration time.Duration
	Deleted  bool
	Err      error
}

type noopObserver struct{}

func (noopObserver) OnClaim(ClaimEvent) {}

func (noopObserver) OnFire(FireEvent) {}

func observerOrNoop(observer Observer) Observer {
	if observer == nil {
		return noopObserver{}
	}

	return observer
}
