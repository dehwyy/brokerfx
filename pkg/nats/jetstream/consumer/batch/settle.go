package batch

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func (c *Consumer) settle(
	ctx context.Context,
	msgs []jetstream.Msg,
	results []Result,
) {
	for i, msg := range msgs {
		messageResult := Nak(ErrMissingResult)
		if i < len(results) {
			messageResult = results[i]
		}

		c.apply(
			ctx,
			msg,
			messageResult,
		)
	}
}

func (c *Consumer) apply(
	ctx context.Context,
	msg jetstream.Msg,
	result Result,
) {
	switch result.Verdict {
	case VerdictAck:
		logSettleErr(
			msg,
			"ack",
			msg.Ack(),
		)
	case VerdictTerm:
		c.sendPoison(
			ctx,
			msg,
			result.Err,
		)
	default:
		c.retry(
			ctx,
			msg,
			result,
		)
	}
}

func (c *Consumer) retry(
	ctx context.Context,
	msg jetstream.Msg,
	result Result,
) {
	if errors.Is(
		result.Err,
		ErrPoison,
	) {
		c.sendPoison(
			ctx,
			msg,
			result.Err,
		)
		return
	}

	delivered := numDelivered(msg)
	if c.maxAttempts > 0 && delivered >= uint64(c.maxAttempts) {
		reason := result.Err
		if reason == nil {
			reason = ErrAttemptsExhausted
		}
		c.sendPoison(
			ctx,
			msg,
			reason,
		)
		return
	}

	delay := c.delayForDelivery(delivered)
	if result.Verdict == VerdictNakDelay && result.Delay > 0 {
		delay = result.Delay
	}

	log.Warn().
		Err(result.Err).
		Str(
			"subject",
			msg.Subject(),
		).
		Uint64(
			"num_delivered",
			delivered,
		).
		Dur(
			"delay",
			delay,
		).
		Msg("batch message nak with delay")

	logSettleErr(
		msg,
		"nak",
		msg.NakWithDelay(delay),
	)
}

func numDelivered(msg jetstream.Msg) uint64 {
	metadata, err := msg.Metadata()
	if err != nil || metadata == nil {
		log.Warn().
			Err(err).
			Str(
				"subject",
				msg.Subject(),
			).
			Msg("batch message metadata unavailable, treating as first delivery")
		return 1
	}

	return metadata.NumDelivered
}

func logSettleErr(
	msg jetstream.Msg,
	action string,
	err error,
) {
	if err == nil {
		return
	}

	if errors.Is(
		err,
		jetstream.ErrMsgAlreadyAckd,
	) {
		log.Debug().
			Str(
				"subject",
				msg.Subject(),
			).
			Str(
				"action",
				action,
			).
			Msg("batch message already settled by handler")
		return
	}

	log.Error().
		Err(err).
		Str(
			"subject",
			msg.Subject(),
		).
		Str(
			"action",
			action,
		).
		Msg("batch message settle failed")
}

func (c *Consumer) firstDelay() time.Duration {
	return c.delays[0]
}
