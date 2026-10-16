package consumeroptsbuilder

func (b *ConsumerOptsBuilder) WithReplicas(replicas int) *ConsumerOptsBuilder {
	b.config.Replicas = replicas
	return b
}
