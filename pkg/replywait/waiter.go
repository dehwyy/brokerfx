package replywait

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrDraining = errors.New("replywait: draining, no new requests accepted")

const drainPollInterval = 5 * time.Millisecond

type Waker interface {
	WakeupRelay()
}

type stopper interface {
	stop()
}

type ReplyWaiter struct {
	cfg   Config
	reg   *registry
	waker Waker

	mu       sync.Mutex
	draining bool
	drained  bool
	drainErr error
	listener stopper
}

func newReplyWaiter(cfg Config, reg *registry, waker Waker) *ReplyWaiter {
	return &ReplyWaiter{
		cfg:   cfg,
		reg:   reg,
		waker: waker,
	}
}

func (rw *ReplyWaiter) attachListener(l stopper) {
	rw.mu.Lock()
	rw.listener = l
	rw.mu.Unlock()
}

func (rw *ReplyWaiter) ReplySubject() string {
	return rw.cfg.ReplySubject
}

func (rw *ReplyWaiter) Draining() bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	return rw.draining
}

func (rw *ReplyWaiter) BeginDrain() {
	rw.mu.Lock()
	rw.draining = true
	rw.mu.Unlock()
}

func (rw *ReplyWaiter) Drain(ctx context.Context) error {
	rw.mu.Lock()
	if rw.drained {
		err := rw.drainErr
		rw.mu.Unlock()
		return err
	}
	rw.draining = true
	rw.mu.Unlock()

	deadlineCtx := ctx
	if rw.cfg.DrainTimeout > 0 {
		var cancel context.CancelFunc
		deadlineCtx, cancel = context.WithTimeout(ctx, rw.cfg.DrainTimeout)
		defer cancel()
	}

	drainErr := rw.waitForInflight(deadlineCtx)

	rw.reg.failAll(ErrDrained)

	rw.mu.Lock()
	listener := rw.listener
	rw.mu.Unlock()

	if listener != nil {
		listener.stop()
	}

	rw.mu.Lock()
	rw.drained = true
	rw.drainErr = drainErr
	rw.mu.Unlock()

	return drainErr
}

func (rw *ReplyWaiter) waitForInflight(deadlineCtx context.Context) error {
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()

	for rw.reg.inflight() > 0 {
		select {
		case <-ticker.C:
		case <-deadlineCtx.Done():
			return fmt.Errorf("%w: inflight replies remaining", ErrDrained)
		}
	}

	return nil
}

func (rw *ReplyWaiter) Register(correlationID string) (*Waiter, error) {
	if rw.Draining() {
		return nil, fmt.Errorf("%w: correlation %s", ErrDraining, correlationID)
	}

	return rw.reg.register(correlationID)
}

func (rw *ReplyWaiter) Request(
	ctx context.Context,
	correlationID string,
	publish func(ctx context.Context) error,
) (jetstream.Msg, error) {
	waiter, err := rw.Register(correlationID)
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
