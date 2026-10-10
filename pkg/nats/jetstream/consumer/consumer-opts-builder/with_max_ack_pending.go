package consumeroptsbuilder

func (b *ConsumerOptsBuilder) WithMaxAckPending(maxAckPending int) *ConsumerOptsBuilder {
	b.config.MaxAckPending = maxAckPending
	return b
}
