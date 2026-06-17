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
