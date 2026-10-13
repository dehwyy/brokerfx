package outbox

import (
	"testing"
	"time"
)

func TestNewRelayClaimEnvOverrides(t *testing.T) {
	t.Setenv(
		envClaimTimeout,
		"2s",
	)
	t.Setenv(
		envStallThreshold,
		"60s",
	)
	t.Setenv(
		envStallReclaimInterval,
		"15s",
	)

	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if r.config.ClaimTimeout != 2*time.Second || r.config.StallThreshold != time.Minute || r.config.StallReclaimInterval != 15*time.Second {
		t.Fatalf("got timeout %s stall %s interval %s", r.config.ClaimTimeout, r.config.StallThreshold, r.config.StallReclaimInterval)
	}
}

func TestNewRelayClaimEnvInvalidKeepsDefaults(t *testing.T) {
	for _, value := range []string{"abc", "-1s", "0"} {
		t.Run(
			value,
			func(t *testing.T) {
				t.Setenv(
					envClaimTimeout,
					value,
				)
				t.Setenv(
					envStallThreshold,
					value,
				)
				t.Setenv(
					envStallReclaimInterval,
					value,
				)

				r := NewRelay(RelayDeps{
					Store: &OutboxStore{},
					Config: Config{
						Mode: ModeDeleteAfterSend,
					},
				})

				if r.config.ClaimTimeout != defaultClaimTimeout || r.config.StallThreshold != 5*time.Minute || r.config.StallReclaimInterval != defaultStallReclaimInterval {
					t.Fatalf("value %q changed config: %+v", value, r.config)
				}
			},
		)
	}
}

func TestNewRelayStallThresholdEnvClampsDefaultInterval(t *testing.T) {
	t.Setenv(
		envStallThreshold,
		"30s",
	)

	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if r.config.StallReclaimInterval != 30*time.Second {
		t.Fatalf("interval %s, want 30s", r.config.StallReclaimInterval)
	}
}
