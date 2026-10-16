package consumer

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func nak(
	msg jetstream.Msg,
	delays []time.Duration,
) error {
	if len(delays) == 0 {
		return msg.Nak()
	}

	return msg.NakWithDelay(
		nakDelay(
			msg,
			delays,
		),
	)
}

func nakDelay(
	msg jetstream.Msg,
	delays []time.Duration,
) time.Duration {
	metadata, err := msg.Metadata()
	if err != nil || metadata.NumDelivered <= 1 {
		return delays[0]
	}

	index := metadata.NumDelivered - 1
	if index >= uint64(len(delays)) {
		return delays[len(delays)-1]
	}

	return delays[index]
}
