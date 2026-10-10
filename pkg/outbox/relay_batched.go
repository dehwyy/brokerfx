package outbox

import (
	"context"
	"sync"
)

func (r *OutboxRelay) process(ctx context.Context) {
	if !r.config.Batched {
		r.processBatch(ctx)
		return
	}

	work := context.WithoutCancel(ctx)
	caps := r.ensureSchemaCaps(work)
	if !caps.V2 {
		r.processBatch(ctx)
		return
	}

	for {
		claimed := r.processBatched(
			work,
			caps,
		)
		if claimed < r.config.BatchSize || ctx.Err() != nil || r.Paused() {
			return
		}
	}
}

func (r *OutboxRelay) processBatched(
	ctx context.Context,
	caps schemaCaps,
) int {
	if r.Paused() {
		return 0
	}

	events, err := r.claimBatch(ctx)
	if err != nil {
		r.logger.Error().Err(err).Msg("failed to claim outbox batch")
		return 0
	}
	if len(events) == 0 {
		return 0
	}

	outcomes := r.publishBatched(
		ctx,
		events,
	)

	var successIDs []string
	var failedOutcomes []pubResult
	for _, outcome := range outcomes {
		if outcome.err != nil {
			r.logger.Error().
				Err(outcome.err).
				Str(
					"event_id",
					outcome.id,
				).
				Msg("failed to publish outbox event")
			failedOutcomes = append(
				failedOutcomes,
				outcome,
			)
			continue
		}
		successIDs = append(
			successIDs,
			outcome.id,
		)
	}

	r.settleOutcomes(
		ctx,
		r.store.DB(),
		caps,
		events,
		successIDs,
		failedOutcomes,
	)

	return len(events)
}

func (r *OutboxRelay) publishBatched(
	ctx context.Context,
	events []relayEvent,
) []pubResult {
	outcomes := make(
		[]pubResult,
		len(events),
	)
	kvLatestIndex := kvLatestIndexes(events)

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
		if latest, ok := kvLatestIndex[event.Topic]; ok && latest != i {
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

	errs := r.produceAll(
		ctx,
		producerEvents,
	)
	for position, index := range pending {
		if position < len(errs) {
			outcomes[index].err = errs[position]
		}
	}

	return outcomes
}

func (r *OutboxRelay) produceAll(
	ctx context.Context,
	events []ProducerEvent,
) []error {
	if batchProducer, ok := r.producer.(BatchProducer); ok {
		return batchProducer.ProduceBatch(
			ctx,
			events,
		)
	}

	errs := make(
		[]error,
		len(events),
	)
	var wg sync.WaitGroup
	for i, event := range events {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = r.producer.Produce(
				ctx,
				event,
			)
		}()
	}
	wg.Wait()

	return errs
}
