package batch

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

func (c *Consumer) isolate(
	ctx context.Context,
	msgs []jetstream.Msg,
) SettleEvent {
	var tally SettleEvent
	spans := itemSpansFrom(ctx)
	for i, msg := range msgs {
		if isSettled(msg) {
			spans.end(
				i,
				settledAck,
			)
			continue
		}

		single := []jetstream.Msg{msg}
		results, completed := c.runHandler(
			ctx,
			single,
		)
		if !completed {
			kind, _ := c.apply(
				ctx,
				msg,
				Nak(ErrHandlerPanic),
				0,
			)
			event := SettleEvent{}.with(kind)
			c.watch().OnSettle(event)
			tally = tally.plus(event)
			spans.end(
				i,
				kind,
			)
			continue
		}

		tally = tally.plus(c.settle(
			withItemSpans(
				ctx,
				spans.slice(
					i,
					i+1,
				),
			),
			single,
			results,
		))
	}

	return tally
}

func isSettled(msg jetstream.Msg) bool {
	return errors.Is(
		msg.InProgress(),
		jetstream.ErrMsgAlreadyAckd,
	)
}
