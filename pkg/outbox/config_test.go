package outbox

import (
	"testing"
	"time"

	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
)

// TestDedupWindowInvariant asserts the core D1 invariant: the stream Duplicates
// window must be at least twice the relay StallThreshold so a re-published stalled
// row always lands inside a live dedup window.
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

// TestNewRelayUsesConfiguredStallThreshold verifies the relay keeps a caller's
// StallThreshold and only fills the default when it is unset.
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

// TestNewRelayFillsZeroStallThreshold guards the zero-value fallback so a caller
// that sets only Mode does not get a zero threshold that re-picks every row.
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

// TestDefaultConfigMatchesV019 is a regression guard: DefaultConfig must stay
// byte-equal to the v0.1.9 baseline so opt-in via RecommendedConfig never
// changes behavior for a caller that does not touch config at all.
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

// TestRecommendedConfigDedupWindowInvariant re-runs the D1 invariant against
// RecommendedConfig, since it changes StallThreshold-adjacent fields nowhere
// but must still respect the dedup window relationship established by
// DefaultConfig's StallThreshold.
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

// TestRecommendedConfigValues pins the D-18/D-25 recommended values so a
// future edit cannot silently drift from the documented recommendation.
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
