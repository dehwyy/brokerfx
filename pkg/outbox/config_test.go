package outbox

import (
	"testing"
	"time"

	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
)

func TestDedupWindowInvariant(t *testing.T) {
	cfg := DefaultConfig()

	stream := streamoptsbuilder.
		NewDefault().
		WithName("test").
		WithSubjects([]string{"test.subject"}).
		Build()

	if cfg.StallThreshold <= 0 {
		t.Fatalf("default StallThreshold must be positive, got %s", cfg.StallThreshold)
	}

	if stream.Duplicates < 2*cfg.StallThreshold {
		t.Fatalf(
			"dedup invariant violated: Duplicates(%s) must be >= 2*StallThreshold(%s)",
			stream.Duplicates,
			cfg.StallThreshold,
		)
	}

	if stream.Duplicates >= stream.MaxAge {
		t.Fatalf(
			"Duplicates(%s) must stay below MaxAge(%s)",
			stream.Duplicates,
			stream.MaxAge,
		)
	}
}

func TestNewRelayUsesConfiguredStallThreshold(t *testing.T) {
	configured := 90 * time.Second

	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode:           ModeDeleteAfterSend,
			StallThreshold: configured,
		},
	})

	if r.config.StallThreshold != configured {
		t.Fatalf("expected StallThreshold %s, got %s", configured, r.config.StallThreshold)
	}
}

func TestNewRelayFillsZeroStallThreshold(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if r.config.StallThreshold != DefaultConfig().StallThreshold {
		t.Fatalf(
			"expected default StallThreshold %s, got %s",
			DefaultConfig().StallThreshold,
			r.config.StallThreshold,
		)
	}
}

func TestDefaultConfigMatchesV019(t *testing.T) {
	want := Config{
		Mode:            ModeUpdateAfterSend,
		BatchSize:       100,
		TickInterval:    2 * time.Second,
		DeleteOlderThan: 1 * time.Hour,
		StallThreshold:  5 * time.Minute,
	}

	got := DefaultConfig()
	if got != want {
		t.Fatalf("DefaultConfig() regressed from v0.1.9 baseline: got %+v, want %+v", got, want)
	}
}

func TestRecommendedConfigDedupWindowInvariant(t *testing.T) {
	cfg := RecommendedConfig()

	stream := streamoptsbuilder.
		NewDefault().
		WithName("test").
		WithSubjects([]string{"test.subject"}).
		Build()

	if cfg.StallThreshold <= 0 {
		t.Fatalf("RecommendedConfig StallThreshold must be positive, got %s", cfg.StallThreshold)
	}

	if stream.Duplicates < 2*cfg.StallThreshold {
		t.Fatalf(
			"dedup invariant violated: Duplicates(%s) must be >= 2*StallThreshold(%s)",
			stream.Duplicates,
			cfg.StallThreshold,
		)
	}
}

func TestRecommendedConfigValues(t *testing.T) {
	cfg := RecommendedConfig()
	base := DefaultConfig()

	if cfg.Mode != base.Mode {
		t.Fatalf("expected Mode unchanged from DefaultConfig, got %s", cfg.Mode)
	}
	if cfg.BatchSize != base.BatchSize {
		t.Fatalf("expected BatchSize unchanged from DefaultConfig, got %d", cfg.BatchSize)
	}
	if cfg.TickInterval != base.TickInterval {
		t.Fatalf("expected TickInterval unchanged from DefaultConfig, got %s", cfg.TickInterval)
	}
	if cfg.StallThreshold != base.StallThreshold {
		t.Fatalf("expected StallThreshold unchanged from DefaultConfig, got %s", cfg.StallThreshold)
	}
	if cfg.CleanupInterval != 24*time.Hour {
		t.Fatalf("expected CleanupInterval 24h, got %s", cfg.CleanupInterval)
	}
	if cfg.DeleteOlderThan != 7*24*time.Hour {
		t.Fatalf("expected DeleteOlderThan 7d, got %s", cfg.DeleteOlderThan)
	}
	if cfg.RetainParked != 30*24*time.Hour {
		t.Fatalf("expected RetainParked 30d, got %s", cfg.RetainParked)
	}
	if cfg.MaxAttempts != 20 {
		t.Fatalf("expected MaxAttempts 20, got %d", cfg.MaxAttempts)
	}
	if cfg.RetryBackoffBase != 2*time.Second {
		t.Fatalf("expected RetryBackoffBase 2s, got %s", cfg.RetryBackoffBase)
	}
	if cfg.RetryBackoffMax != 5*time.Minute {
		t.Fatalf("expected RetryBackoffMax 5m, got %s", cfg.RetryBackoffMax)
	}
	if cfg.StatsInterval != time.Minute {
		t.Fatalf("expected StatsInterval 1m, got %s", cfg.StatsInterval)
	}
}
