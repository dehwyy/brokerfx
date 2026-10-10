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

	results, completed := c.runHandler(
		ctx,
		msgs,
	)
	if !completed {
		c.isolate(
			ctx,
			msgs,
		)
		return
	}

	c.settle(
		ctx,
		msgs,
		results,
	)
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
