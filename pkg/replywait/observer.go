package replywait

import "time"

type Observer interface {
	WaitStarted(correlationID string)
	WaitResolved(correlationID string, duration time.Duration)
	WaitTimedOut(correlationID string)
	WaitDrained(correlationID string)
	ReplyLate(correlationID string)
	ReplyRejected(err error)
	Inflight(count int)
}

type NopObserver struct{}

func (NopObserver) WaitStarted(string) {}

func (NopObserver) WaitResolved(string, time.Duration) {}

func (NopObserver) WaitTimedOut(string) {}

func (NopObserver) WaitDrained(string) {}

func (NopObserver) ReplyLate(string) {}

func (NopObserver) ReplyRejected(error) {}

func (NopObserver) Inflight(int) {}
