package outbox

import (
	"errors"
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
	envTxDrain              = "BROKERFX_OUTBOX_TX_DRAIN"
	envTxDrainTimeout       = "BROKERFX_OUTBOX_TX_DRAIN_PUBLISH_TIMEOUT"

	defaultTxDrainPublishTimeout = 2 * time.Second
	maxTxDrainPublishTimeout     = 25 * time.Second
)

var ErrTxDrainPublishTimeout = errors.New("outbox: tx-drain publish timeout must be below 25s")

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
	if envTrue(envTxDrain) {
		cfg.TxDrain = true
	}
	if value, ok := envPositiveDuration(envTxDrainTimeout); ok {
		cfg.TxDrainPublishTimeout = value
	}
	if cfg.TxDrain && cfg.TxDrainPublishTimeout <= 0 {
		cfg.TxDrainPublishTimeout = defaultTxDrainPublishTimeout
	}

	return cfg
}

func envTrue(name string) bool {
	value := strings.TrimSpace(os.Getenv(name))
	return strings.EqualFold(
		value,
		"true",
	) || value == "1"
}

func validateThroughput(cfg Config) error {
	if cfg.TxDrain && cfg.TxDrainPublishTimeout >= maxTxDrainPublishTimeout {
		return ErrTxDrainPublishTimeout
	}

	return nil
}
