package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func (c *Consumer) hold(
	ctx context.Context,
	msg jetstream.Msg,
	result Result,
	floor time.Duration,
) (settleKind, time.Duration) {
	delivered := numDelivered(msg)
	if c.maxDeliver > 0 && delivered >= uint64(c.maxDeliver) {
		c.sendPoison(
			ctx,
			msg,
			fmt.Errorf(
				"%w: %w",
				ErrAttemptsExhausted,
				result.Err,
			),
		)
		return settledPoison, 0
	}

	delay := c.delayForDelivery(delivered)
	if result.Delay > 0 {
		delay = result.Delay
	}
	delay = max(
		delay,
		floor,
	)

	log.Debug().
		Str(
			"subject",
			msg.Subject(),
		).
		Dur(
			"delay",
			delay,
		).
		Msg("batch message held behind a failed message with the same order key")

	logSettleErr(
		msg,
		"nak",
		msg.NakWithDelay(delay),
	)

	return settledBlocked, delay
}
