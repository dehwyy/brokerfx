package consumeroptsbuilder

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func (b *ConsumerOptsBuilder) WithPinnedGroup(
	group string,
	ttl time.Duration,
) *ConsumerOptsBuilder {
	b.config.PriorityPolicy = jetstream.PriorityPolicyPinned
	b.config.PriorityGroups = []string{group}
	b.config.PinnedTTL = ttl
	return b
}
