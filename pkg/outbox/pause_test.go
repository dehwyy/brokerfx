package outbox

import (
	"context"
	"testing"
)

func TestNewRelayNotPausedByDefault(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if r.Paused() {
		t.Fatalf("expected relay not paused by default")
	}
}

func TestNewRelayPausedByConfig(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode:   ModeDeleteAfterSend,
			Paused: true,
		},
	})

	if !r.Paused() {
		t.Fatalf("expected relay paused when Config.Paused is true")
	}
}

func TestNewRelayPausedByEnvTrue(t *testing.T) {
	t.Setenv(envRelayPaused, "true")

	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if !r.Paused() {
		t.Fatalf("expected relay paused when env %s=true", envRelayPaused)
	}
}

func TestNewRelayPausedByEnvOne(t *testing.T) {
	t.Setenv(envRelayPaused, "1")

	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if !r.Paused() {
		t.Fatalf("expected relay paused when env %s=1", envRelayPaused)
	}
}

func TestNewRelayEnvCannotUnpauseConfig(t *testing.T) {
	t.Setenv(envRelayPaused, "false")

	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode:   ModeDeleteAfterSend,
			Paused: true,
		},
	})

	if !r.Paused() {
		t.Fatalf("expected relay to stay paused: env cannot unpause a config-level pause")
	}
}

func TestOutboxRelayPauseResume(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if r.Paused() {
		t.Fatalf("expected relay not paused before Pause()")
	}

	r.Pause()
	if !r.Paused() {
		t.Fatalf("expected relay paused after Pause()")
	}

	r.Resume()
	if r.Paused() {
		t.Fatalf("expected relay not paused after Resume()")
	}
}

func TestProcessBatchSkippedWhilePaused(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: nil,
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})
	r.Pause()

	r.processBatch(context.Background())
}
