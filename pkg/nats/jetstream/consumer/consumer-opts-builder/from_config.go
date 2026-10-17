package consumeroptsbuilder

import "github.com/nats-io/nats.go/jetstream"

func FromConfig(cfg jetstream.ConsumerConfig) *ConsumerOptsBuilder {
	return &ConsumerOptsBuilder{
		config: cfg,
	}
}
