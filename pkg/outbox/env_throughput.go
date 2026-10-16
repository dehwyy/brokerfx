package outbox

import (
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const envPendingGaugeInterval = "BROKERFX_OUTBOX_PENDING_GAUGE_INTERVAL"

func envNonNegativeDuration(name string) (time.Duration, bool) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, false
	}

	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		log.Warn().
			Str("env", name).
			Str("value", value).
			Msg("outbox: ignoring invalid duration env")
		return 0, false
	}

	return parsed, true
}

func resolveThroughput(cfg Config) Config {
	if value, ok := envNonNegativeDuration(envPendingGaugeInterval); ok {
		cfg.PendingGaugeInterval = value
	}

	return cfg
}
