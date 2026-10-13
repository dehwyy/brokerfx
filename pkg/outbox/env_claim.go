package outbox

import (
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	envClaimV2                  = "BROKERFX_OUTBOX_CLAIM_V2"
	envClaimTimeout             = "BROKERFX_OUTBOX_CLAIM_TIMEOUT"
	envStallThreshold           = "BROKERFX_OUTBOX_STALL_THRESHOLD"
	envStallReclaimInterval     = "BROKERFX_OUTBOX_STALL_RECLAIM_INTERVAL"
	defaultClaimTimeout         = 5 * time.Second
	defaultStallReclaimInterval = time.Minute
)

func envClaimV2Enabled() bool {
	value := strings.TrimSpace(os.Getenv(envClaimV2))
	return strings.EqualFold(
		value,
		"true",
	) || value == "1"
}

func envPositiveDuration(name string) (time.Duration, bool) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, false
	}

	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		log.Warn().
			Str("env", name).
			Str("value", value).
			Msg("outbox: ignoring invalid duration env")
		return 0, false
	}

	return parsed, true
}

func resolveClaim(cfg Config) Config {
	if value, ok := envPositiveDuration(envClaimTimeout); ok {
		cfg.ClaimTimeout = value
	}
	if value, ok := envPositiveDuration(envStallThreshold); ok {
		cfg.StallThreshold = value
	}
	if value, ok := envPositiveDuration(envStallReclaimInterval); ok {
		cfg.StallReclaimInterval = value
	}
	if envClaimV2Enabled() {
		cfg.ClaimV2 = true
	}
	if cfg.ClaimTimeout <= 0 {
		cfg.ClaimTimeout = defaultClaimTimeout
	}
	if cfg.StallReclaimInterval <= 0 {
		cfg.StallReclaimInterval = defaultStallReclaimInterval
		if cfg.StallThreshold > 0 && cfg.StallThreshold < cfg.StallReclaimInterval {
			cfg.StallReclaimInterval = cfg.StallThreshold
		}
	}

	return cfg
}
