package durable

import "github.com/nats-io/nats.go/jetstream"

func Name(cfg jetstream.ConsumerConfig) string {
	if cfg.Durable != "" {
		return cfg.Durable
	}

	return cfg.Name
}
