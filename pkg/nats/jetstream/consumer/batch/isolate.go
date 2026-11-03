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
	wrapped := itemMsgsFrom(ctx)
	for i, msg := range msgs {
		handlerMsg := msg
		selfKind := settledAck
		if i < len(wrapped) {
			handlerMsg = wrapped[i]
			selfKind = wrapped[i].verdict()
		}

		if isSettled(msg) {
			spans.end(
				i,
				selfKind,
			)
			continue
		}

		single := []jetstream.Msg{msg}
		results, completed := c.runHandler(
			ctx,
			[]jetstream.Msg{handlerMsg},
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
