package streamoptsbuilder

import (
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func FitDuplicates(cfg jetstream.StreamConfig) jetstream.StreamConfig {
	if cfg.MaxAge <= 0 || cfg.Duplicates <= cfg.MaxAge {
		return cfg
	}

	log.Warn().
		Str(
			"stream",
			cfg.Name,
		).
		Dur(
			"requested_duplicates",
			cfg.Duplicates,
		).
		Dur(
			"max_age",
			cfg.MaxAge,
		).
		Msg("stream duplicates window exceeds max age, clamping to max age")
	cfg.Duplicates = cfg.MaxAge

	return cfg
}
