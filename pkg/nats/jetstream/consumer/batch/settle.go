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
	var tally SettleEvent
	returns := make(
		[]time.Duration,
		len(msgs),
	)
	for i, msg := range msgs {
		messageResult := Nak(ErrMissingResult)
		if i < len(results) {
			messageResult = results[i]
		}

		floor := returnFloor(
			returns,
			i,
			messageResult,
		)
		kind, delay := c.apply(
			ctx,
			msg,
			messageResult,
			floor,
		)
		returns[i] = max(
			delay,
			floor,
		)
		tally = tally.with(kind)
	}

	c.watch().OnSettle(tally)
}

func returnFloor(
	returns []time.Duration,
	index int,
	result Result,
) time.Duration {
	previous := result.after - 1
	if previous < 0 || previous >= index {
		return 0
	}

	return returns[previous]
}

func (c *Consumer) apply(
	ctx context.Context,
	msg jetstream.Msg,
	result Result,
	floor time.Duration,
) (settleKind, time.Duration) {
	switch result.Verdict {
	case VerdictAck, VerdictSkip:
		logSettleErr(
			msg,
			"ack",
			msg.Ack(),
		)
		if result.Verdict == VerdictSkip {
			return settledSkip, 0
		}
		return settledAck, 0
	case VerdictTerm:
		c.sendPoison(
			ctx,
			msg,
			result.Err,
		)
		return settledPoison, 0
	default:
		return c.retry(
			ctx,
			msg,
			result,
			floor,
		)
	}
}

func (c *Consumer) retry(
	ctx context.Context,
	msg jetstream.Msg,
	result Result,
	floor time.Duration,
) (settleKind, time.Duration) {
	if errors.Is(
		result.Err,
		ErrOrderBlocked,
	) {
		return c.hold(
			ctx,
			msg,
			result,
			floor,
		)
	}

	if errors.Is(
		result.Err,
		ErrPoison,
	) {
		c.sendPoison(
			ctx,
			msg,
			result.Err,
		)
		return settledPoison, 0
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
		return settledPoison, 0
	}

	delay := c.delayForDelivery(delivered)
	if result.Verdict == VerdictNakDelay && result.Delay > 0 {
		delay = result.Delay
	}
	delay = max(
		delay,
		floor,
	)

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

	return settledRetry, delay
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
