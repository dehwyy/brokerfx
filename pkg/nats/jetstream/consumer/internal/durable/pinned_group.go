package durable

import "github.com/nats-io/nats.go/jetstream"

func PinnedGroup(cfg jetstream.ConsumerConfig) string {
	if cfg.PriorityPolicy != jetstream.PriorityPolicyPinned || len(cfg.PriorityGroups) != 1 {
		return ""
	}

	return cfg.PriorityGroups[0]
}
