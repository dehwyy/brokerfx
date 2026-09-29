package consumeroptsbuilder

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestNewDefaultMatchesV019(t *testing.T) {
	cfg := NewDefault().Build()
	if cfg.AckPolicy != jetstream.AckExplicitPolicy {
		t.Fatalf("ack policy %v", cfg.AckPolicy)
	}
	if cfg.AckWait != 30*time.Second {
		t.Fatalf("ack wait %v", cfg.AckWait)
	}
	if cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Fatalf("deliver policy %v", cfg.DeliverPolicy)
	}
}
