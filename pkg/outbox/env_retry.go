package outbox

import (
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	envRetryBackoffBase = "BROKERFX_OUTBOX_RETRY_BACKOFF_BASE"
	envRetryBackoffMax  = "BROKERFX_OUTBOX_RETRY_BACKOFF_MAX"
	envRetryJitter      = "BROKERFX_OUTBOX_RETRY_JITTER"

	defaultEnvRetryBackoffMax = 5 * time.Second
)

func resolveRetry(cfg Config) Config {
	if envRetryJitterOn() {
		cfg.RetryJitter = true
	}

	base, hasBase := envPositiveDuration(envRetryBackoffBase)
	maxDelay, hasMax := envPositiveDuration(envRetryBackoffMax)
	if !hasBase {
		if hasMax {
			log.Warn().
				Str(
					"env",
					envRetryBackoffMax,
				).
				Msg("outbox: retry backoff max without base is ignored")
		}

		return cfg
	}

	if cfg.RetryBackoffBase != 0 {
		log.Warn().
			Dur(
				"config_base",
				cfg.RetryBackoffBase,
			).
			Dur(
				"config_max",
				cfg.RetryBackoffMax,
			).
			Msg("outbox: retry backoff env is ignored, the config sets its own backoff")

		return cfg
	}

	if !hasMax {
		maxDelay = defaultEnvRetryBackoffMax
	}
	cfg.RetryBackoffBase = base
	cfg.RetryBackoffMax = max(
		maxDelay,
		base,
	)

	return cfg
}

func envRetryJitterOn() bool {
	value := strings.TrimSpace(os.Getenv(envRetryJitter))
	if value == "" || value == "0" || strings.EqualFold(
		value,
		"false",
	) {
		return false
	}
	if envTrue(envRetryJitter) {
		return true
	}

	log.Warn().
		Str(
			"env",
			envRetryJitter,
		).
		Str(
			"value",
			value,
		).
		Msg("outbox: ignoring invalid retry jitter env")

	return false
}
