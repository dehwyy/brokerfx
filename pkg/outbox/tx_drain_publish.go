package outbox

import (
	"context"
	"time"
)

type indexedErr struct {
	index int
	err   error
}

func (r *OutboxRelay) publishWithin(
	ctx context.Context,
	events []ProducerEvent,
	timeout time.Duration,
) []error {
	errs := make(
		[]error,
		len(events),
	)
	for i := range errs {
		errs[i] = context.DeadlineExceeded
	}

	pubCtx, cancel := context.WithTimeout(
		ctx,
		timeout,
	)
	defer cancel()

	if batchProducer, ok := r.producer.(BatchProducer); ok {
		done := make(
			chan []error,
			1,
		)
		go func() {
			done <- batchProducer.ProduceBatch(
				pubCtx,
				events,
			)
		}()

		select {
		case got := <-done:
			for i := range errs {
				if i < len(got) {
					errs[i] = got[i]
				}
			}
		case <-pubCtx.Done():
		}

		return errs
	}

	producer := r.producer
	results := make(
		chan indexedErr,
		len(events),
	)
	for i, event := range events {
		go func() {
			results <- indexedErr{
				index: i,
				err: producer.Produce(
					pubCtx,
					event,
				),
			}
		}()
	}

	for remaining := len(events); remaining > 0; remaining-- {
		select {
		case result := <-results:
			errs[result.index] = result.err
		case <-pubCtx.Done():
			return errs
		}
	}

	return errs
}

func (r *OutboxRelay) drainPublish(
	ctx context.Context,
	events []relayEvent,
) []pubResult {
	outcomes := make(
		[]pubResult,
		len(events),
	)
	latest := r.latestIndexes(events)

	pending := make(
		[]int,
		0,
		len(events),
	)
	producerEvents := make(
		[]ProducerEvent,
		0,
		len(events),
	)
	for i, event := range events {
		outcomes[i] = pubResult{
			id: event.ID,
		}
		if index, ok := latest[event.Topic]; ok && index != i {
			continue
		}

		producerEvent, err := r.producerEvent(event)
		if err != nil {
			outcomes[i].err = err
			continue
		}
		pending = append(
			pending,
			i,
		)
		producerEvents = append(
			producerEvents,
			producerEvent,
		)
	}

	if len(producerEvents) == 0 {
		return outcomes
	}

	errs := r.publishWithin(
		ctx,
		producerEvents,
		r.config.TxDrainPublishTimeout,
	)
	for position, index := range pending {
		outcomes[index].err = errs[position]
	}

	return outcomes
}
