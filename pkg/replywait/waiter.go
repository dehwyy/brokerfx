package replywait

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

type Waker interface {
	WakeupRelay()
}

type ReplyWaiter struct {
	cfg   Config
	reg   *registry
	waker Waker
}

func newReplyWaiter(cfg Config, reg *registry, waker Waker) *ReplyWaiter {
	return &ReplyWaiter{
		cfg:   cfg,
		reg:   reg,
		waker: waker,
	}
}

func (rw *ReplyWaiter) ReplySubject() string {
	return rw.cfg.ReplySubject
}

func (rw *ReplyWaiter) Register(correlationID string) (*Waiter, error) {
	return rw.reg.register(correlationID)
}

func (rw *ReplyWaiter) Request(
	ctx context.Context,
	correlationID string,
	publish func(ctx context.Context) error,
) (jetstream.Msg, error) {
	waiter, err := rw.reg.register(correlationID)
	if err != nil {
		return nil, err
	}

	if err := publish(ctx); err != nil {
		waiter.Cancel()
		return nil, err
	}

	if rw.waker != nil {
		rw.waker.WakeupRelay()
	}

	waitCtx, cancel := context.WithTimeout(ctx, rw.cfg.DefaultTimeout)
	defer cancel()

	return waiter.Wait(waitCtx)
}
