package batch

import (
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func (c *Consumer) hold(
	msg jetstream.Msg,
	result Result,
) settleKind {
	delay := c.delayForDelivery(numDelivered(msg))
	if result.Delay > 0 {
		delay = result.Delay
	}

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

	return settledBlocked
}
