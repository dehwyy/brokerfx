package outbox

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	envPendingGaugeInterval = "BROKERFX_OUTBOX_PENDING_GAUGE_INTERVAL"
	envCleanupBatch         = "BROKERFX_OUTBOX_CLEANUP_BATCH"
	envLatestOnlyPrefixes   = "BROKERFX_OUTBOX_LATEST_ONLY_PREFIXES"
)

func splitList(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	return strings.Split(
		value,
		",",
	)
}

func envNonNegativeInt(name string) (int, bool) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, false
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		log.Warn().
			Str("env", name).
			Str("value", value).
			Msg("outbox: ignoring invalid integer env")
		return 0, false
	}

	return parsed, true
}

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
	if value, ok := envNonNegativeInt(envCleanupBatch); ok {
		cfg.CleanupBatch = value
	}

	return cfg
}
