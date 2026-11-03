package batch

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func (c *Consumer) processBatch(
	ctx context.Context,
	msgs []jetstream.Msg,
) {
	stopHeartbeat := c.heartbeat(msgs)
	defer stopHeartbeat()

	spanCtx, span := startBatchSpan(
		ctx,
		msgs,
	)
	spans := startItemSpans(
		span,
		msgs,
	)
	settleCtx := withItemSpans(
		spanCtx,
		spans,
	)
	healthy := false
	var tally SettleEvent
	defer func() {
		spans.endUnsettled()
		endBatchSpan(
			span,
			healthy,
			tally.failed(),
		)
	}()

	results, completed := c.runHandler(
		spanCtx,
		msgs,
	)
	if !completed {
		tally = c.isolate(
			settleCtx,
			msgs,
		)
		return
	}

	tally = c.settle(
		settleCtx,
		msgs,
		results,
	)
	healthy = true
}

func (c *Consumer) runHandler(
	ctx context.Context,
	msgs []jetstream.Msg,
) (results []Result, completed bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Error().
				Any(
					"panic",
					recovered,
				).
				Int(
					"batch",
					len(msgs),
				).
				Msg("batch handler panic, isolating messages")
			results = nil
			completed = false
		}
	}()

	return c.handler(
		ctx,
		msgs,
	), true
}
