package consumeroptsbuilder

import "github.com/nats-io/nats.go/jetstream"

func (b *ConsumerOptsBuilder) WithDeliverByStartSequence(sequence uint64) *ConsumerOptsBuilder {
	if sequence == 0 {
		b.config.DeliverPolicy = jetstream.DeliverAllPolicy
		b.config.OptStartSeq = 0
		return b
	}

	b.config.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
	b.config.OptStartSeq = sequence
	return b
}
