package batch

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

func (c *Consumer) isolate(
	ctx context.Context,
	msgs []jetstream.Msg,
) {
	for _, msg := range msgs {
		if isSettled(msg) {
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
			c.watch().OnSettle(SettleEvent{}.with(kind))
			continue
		}

		c.settle(
			ctx,
			single,
			results,
		)
	}
}

func isSettled(msg jetstream.Msg) bool {
	return errors.Is(
		msg.InProgress(),
		jetstream.ErrMsgAlreadyAckd,
	)
}
