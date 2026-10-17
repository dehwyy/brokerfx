package frame

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type frameState struct {
	mu       sync.Mutex
	msg      jetstream.Msg
	pending  int
	failed   bool
	delay    time.Duration
	throttle time.Duration
	lastBeat time.Time
}

type outcome struct {
	nak   bool
	delay time.Duration
}

func (f *frameState) settle(
	element *elementMsg,
	result outcome,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if element.done {
		return false, jetstream.ErrMsgAlreadyAckd
	}
	element.done = true
	f.pending--
	if result.nak {
		f.failed = true
		f.delay = max(
			f.delay,
			result.delay,
		)
	}

	return f.pending == 0, nil
}

func (f *frameState) finish(
	ctx context.Context,
	double bool,
) error {
	f.mu.Lock()
	failed := f.failed
	delay := f.delay
	f.mu.Unlock()

	switch {
	case failed && delay > 0:
		return f.msg.NakWithDelay(delay)
	case failed:
		return f.msg.Nak()
	case double:
		return f.msg.DoubleAck(ctx)
	}

	return f.msg.Ack()
}

func (f *frameState) resolve(
	ctx context.Context,
	element *elementMsg,
	result outcome,
	double bool,
) error {
	last, err := f.settle(
		element,
		result,
	)
	if err != nil || !last {
		return err
	}

	return f.finish(
		ctx,
		double,
	)
}

func (f *frameState) progress(element *elementMsg) error {
	f.mu.Lock()
	if element.done {
		f.mu.Unlock()
		return jetstream.ErrMsgAlreadyAckd
	}
	now := time.Now()
	if f.throttle > 0 && !f.lastBeat.IsZero() && now.Sub(f.lastBeat) < f.throttle {
		f.mu.Unlock()
		return nil
	}
	f.lastBeat = now
	f.mu.Unlock()

	return f.msg.InProgress()
}
