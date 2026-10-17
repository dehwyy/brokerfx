package batch

import "time"

type Observer interface {
	OnFetch(event FetchEvent)
	OnApply(event ApplyEvent)
	OnBisect(event BisectEvent)
	OnSettle(event SettleEvent)
}

type FetchEvent struct {
	Messages int
	Max      int
	Duration time.Duration
}

type ApplyEvent struct {
	Messages int
	Fresh    int
	Duration time.Duration
	Err      error
}

type BisectEvent struct {
	Messages int
	Groups   int
	Err      error
}

type SettleEvent struct {
	Ack     int
	Skip    int
	Retry   int
	Poison  int
	Blocked int
}

type FrameObserver interface {
	OnFrame(event FrameEvent)
}

type FrameEvent struct {
	Elements int
	Err      error
}
