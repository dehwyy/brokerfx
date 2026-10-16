package consumer

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func ackWaitOf(consumer jetstream.Consumer) time.Duration {
	info := consumer.CachedInfo()
	if info == nil {
		return 0
	}

	return info.Config.AckWait
}
