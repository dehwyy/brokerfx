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
		Dur(
			"outbox_max_stall_threshold",
			cfg.MaxAge/2,
		).
		Msg("stream duplicates window exceeds max age, clamping to max age: an outbox publishing here needs BROKERFX_OUTBOX_STALL_THRESHOLD at most outbox_max_stall_threshold")
	cfg.Duplicates = cfg.MaxAge

	return cfg
}
