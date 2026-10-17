package consumeroptsbuilder

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type Partitions struct {
	Base      jetstream.ConsumerConfig
	Count     int
	Filters   func(partition int) []string
	Group     string
	PinnedTTL time.Duration
}
