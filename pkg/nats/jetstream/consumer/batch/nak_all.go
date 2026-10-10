package batch

import "github.com/nats-io/nats.go/jetstream"

func (c *Consumer) nakAll(msgs []jetstream.Msg) {
	for _, msg := range msgs {
		logSettleErr(
			msg,
			"nak",
			msg.Nak(),
		)
	}
}
