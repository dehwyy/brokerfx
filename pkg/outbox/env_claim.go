package outbox

import (
	"os"
	"strings"
	"time"
)

const (
	envClaimV2                  = "BROKERFX_OUTBOX_CLAIM_V2"
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

func resolveClaim(cfg Config) Config {
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
