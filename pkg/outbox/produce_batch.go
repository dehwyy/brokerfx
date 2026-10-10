package outbox

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func (p *JetStreamProducer) ProduceBatch(
	ctx context.Context,
	events []ProducerEvent,
) []error {
	errs := make(
		[]error,
		len(events),
	)
	futures := make(
		[]jetstream.PubAckFuture,
		len(events),
	)

	for i, event := range events {
		future, err := p.js.PublishMsgAsync(toNatsMsg(event))
		if err != nil {
			errs[i] = err
			continue
		}
		futures[i] = future
	}

	timer := time.NewTimer(p.timeout)
	defer timer.Stop()

	expired := false
	for i, future := range futures {
		if future == nil {
			continue
		}
		if expired {
			errs[i] = settledFuture(future)
			continue
		}

		select {
		case <-future.Ok():
		case err := <-future.Err():
			errs[i] = err
		case <-timer.C:
			expired = true
			errs[i] = settledFuture(future)
		case <-ctx.Done():
			expired = true
			errs[i] = ctx.Err()
		}
	}

	return errs
}

func settledFuture(future jetstream.PubAckFuture) error {
	select {
	case <-future.Ok():
		return nil
	case err := <-future.Err():
		return err
	default:
		return context.DeadlineExceeded
	}
}
