package batch

import (
	"context"

	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func (c *Consumer) unpack(
	ctx context.Context,
	msgs []jetstream.Msg,
) []jetstream.Msg {
	unpacked := make(
		[]jetstream.Msg,
		0,
		len(msgs),
	)
	for _, msg := range msgs {
		if !frame.IsFrame(msg) {
			unpacked = append(
				unpacked,
				msg,
			)
			continue
		}

		elements, err := frame.Unpack(
			msg,
			c.heartbeatEvery,
		)
		c.watchFrame(FrameEvent{
			Elements: len(elements),
			Err:      err,
		})
		if err != nil {
			log.Error().
				Err(err).
				Str(
					"subject",
					msg.Subject(),
				).
				Msg("batch frame cannot be unpacked, poisoning the whole frame")
			c.sendPoison(
				ctx,
				msg,
				err,
			)
			continue
		}

		unpacked = append(
			unpacked,
			elements...,
		)
	}

	return unpacked
}
