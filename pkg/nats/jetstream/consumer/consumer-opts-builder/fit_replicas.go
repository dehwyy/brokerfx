package consumeroptsbuilder

import (
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func FitReplicas(
	cfg jetstream.ConsumerConfig,
	stream jetstream.StreamConfig,
) jetstream.ConsumerConfig {
	if cfg.Replicas == 0 {
		return cfg
	}

	fits := cfg.Replicas > 0 && cfg.Replicas <= stream.Replicas
	if stream.Retention != jetstream.LimitsPolicy {
		fits = cfg.Replicas == stream.Replicas
	}
	if fits {
		return cfg
	}

	log.Warn().
		Str(
			"stream",
			stream.Name,
		).
		Str(
			"consumer",
			consumerName(cfg),
		).
		Int(
			"requested_replicas",
			cfg.Replicas,
		).
		Int(
			"stream_replicas",
			stream.Replicas,
		).
		Str(
			"retention",
			stream.Retention.String(),
		).
		Msg("consumer replicas do not fit the stream, inheriting stream replicas")
	cfg.Replicas = 0

	return cfg
}

func consumerName(cfg jetstream.ConsumerConfig) string {
	if cfg.Durable != "" {
		return cfg.Durable
	}

	return cfg.Name
}
