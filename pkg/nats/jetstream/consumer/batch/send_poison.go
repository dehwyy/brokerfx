package batch

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func (c *Consumer) sendPoison(
	ctx context.Context,
	msg jetstream.Msg,
	reason error,
) {
	if c.onPoison == nil {
		log.Error().
			Err(reason).
			Str("subject", msg.Subject()).
			Msg("batch poison message terminated without OnPoison")
		logSettleErr(
			msg,
			"term",
			msg.Term(),
		)
		return
	}

	if err := c.callOnPoison(
		ctx,
		msg,
		reason,
	); err != nil {
		log.Error().
			Err(err).
			AnErr("reason", reason).
			Str("subject", msg.Subject()).
			Msg("batch OnPoison failed, message redelivered later")
		logSettleErr(
			msg,
			"nak",
			msg.NakWithDelay(c.firstDelay()),
		)
		return
	}

	log.Error().
		Err(reason).
		Str("subject", msg.Subject()).
		Msg("batch poison message handed to OnPoison")
	logSettleErr(
		msg,
		"term",
		msg.Term(),
	)
}

func (c *Consumer) callOnPoison(
	ctx context.Context,
	msg jetstream.Msg,
	reason error,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"%w: OnPoison: %v",
				ErrHandlerPanic,
				recovered,
			)
		}
	}()

	return c.onPoison(
		ctx,
		msg,
		reason,
	)
}
