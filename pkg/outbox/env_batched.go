package outbox

import (
	"os"
	"strings"
	"time"
)

const (
	envRelayBatched    = "BROKERFX_OUTBOX_RELAY_BATCHED"
	defaultRelayLinger = 5 * time.Millisecond
)

func envBatched() bool {
	value := strings.TrimSpace(os.Getenv(envRelayBatched))
	return strings.EqualFold(
		value,
		"true",
	) || value == "1"
}

func resolveBatched(cfg Config) Config {
	if envBatched() {
		cfg.Batched = true
	}
	if cfg.Batched && cfg.Linger <= 0 {
		cfg.Linger = defaultRelayLinger
	}

	return cfg
}
