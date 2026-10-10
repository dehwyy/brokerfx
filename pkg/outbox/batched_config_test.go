package outbox

import (
	"testing"
	"time"
)

func TestDefaultConfigNotBatched(t *testing.T) {
	if DefaultConfig().Batched || RecommendedConfig().Batched {
		t.Fatal("defaults must not be batched")
	}
	if DefaultConfig().Linger != 0 || RecommendedConfig().Linger != 0 {
		t.Fatal("defaults must not linger")
	}
}

func TestNewRelayBatchedByEnv(t *testing.T) {
	for _, value := range []string{"1", "true", "TRUE"} {
		t.Setenv(envRelayBatched, value)

		r := NewRelay(RelayDeps{
			Store: &OutboxStore{},
			Config: Config{
				Mode: ModeDeleteAfterSend,
			},
		})

		if !r.config.Batched || r.config.Linger != 5*time.Millisecond {
			t.Fatalf("env %q: batched %v linger %s", value, r.config.Batched, r.config.Linger)
		}
	}
}

func TestNewRelayBatchedDefaultLinger(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode:    ModeDeleteAfterSend,
			Batched: true,
		},
	})

	if !r.config.Batched || r.config.Linger != 5*time.Millisecond {
		t.Fatalf("batched %v linger %s", r.config.Batched, r.config.Linger)
	}
}

func TestNewRelayBatchedKeepsExplicitLinger(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode:    ModeDeleteAfterSend,
			Batched: true,
			Linger:  20 * time.Millisecond,
		},
	})

	if r.config.Linger != 20*time.Millisecond {
		t.Fatalf("linger %s", r.config.Linger)
	}
}

func TestNewRelayBatchedWithoutModeKeepsBatched(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Batched: true,
		},
	})

	if !r.config.Batched || r.config.Mode != ModeUpdateAfterSend {
		t.Fatalf("batched %v mode %s", r.config.Batched, r.config.Mode)
	}
}

func TestNewRelayNotBatchedKeepsLingerZero(t *testing.T) {
	t.Setenv(envRelayBatched, "")

	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if r.config.Batched || r.config.Linger != 0 {
		t.Fatalf("batched %v linger %s", r.config.Batched, r.config.Linger)
	}
}
