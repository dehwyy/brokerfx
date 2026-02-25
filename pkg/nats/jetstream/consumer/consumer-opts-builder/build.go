package consumeroptsbuilder

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type ConsumerOptsBuilder struct {
	config jetstream.ConsumerConfig
}

func NewDefault() *ConsumerOptsBuilder {
	return &ConsumerOptsBuilder{
		config: jetstream.ConsumerConfig{
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       10 * time.Second,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	}
}

func (b *ConsumerOptsBuilder) Build() jetstream.ConsumerConfig {
	return b.config
}

// REQUIRED
func (b *ConsumerOptsBuilder) WithName(
	name string,
	durable ...bool,
) *ConsumerOptsBuilder {
	b.config.Name = name
	if len(durable) > 0 {
		b.config.Durable = name
	}

	return b
}
func (b *ConsumerOptsBuilder) WithFilterSubject(
	filterSubject string,
) *ConsumerOptsBuilder {
	b.config.FilterSubject = filterSubject
	return b
}

// -----------

// OPTIONAL
func (b *ConsumerOptsBuilder) WithDescription(
	description string,
) *ConsumerOptsBuilder {
	b.config.Description = description
	return b
}

func (b *ConsumerOptsBuilder) WithAckWait(
	ackWait time.Duration,
) *ConsumerOptsBuilder {
	b.config.AckWait = ackWait
	return b
}

func (b *ConsumerOptsBuilder) WithMaxWaiting(
	maxWaiting int,
) *ConsumerOptsBuilder {
	b.config.MaxWaiting = maxWaiting
	return b
}

func (b *ConsumerOptsBuilder) WithMaxDeliver(
	maxDeliver int,
) *ConsumerOptsBuilder {
	b.config.MaxDeliver = maxDeliver
	return b
}

func (b *ConsumerOptsBuilder) WithDeliverPolicy(
	deliverPolicy jetstream.DeliverPolicy,
) *ConsumerOptsBuilder {
	b.config.DeliverPolicy = deliverPolicy
	return b
}

func (b *ConsumerOptsBuilder) WithAckPolicy(
	ackPolicy jetstream.AckPolicy,
) *ConsumerOptsBuilder {
	b.config.AckPolicy = ackPolicy
	return b
}
