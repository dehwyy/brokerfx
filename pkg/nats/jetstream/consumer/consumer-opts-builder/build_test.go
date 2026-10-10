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

func TestWithMaxAckPending(t *testing.T) {
	if got := NewDefault().Build().MaxAckPending; got != 0 {
		t.Fatalf("default max ack pending %d", got)
	}
	if got := NewDefault().WithMaxAckPending(32).Build().MaxAckPending; got != 32 {
		t.Fatalf("max ack pending %d", got)
	}
}

func TestWithDeliverByStartSequence(t *testing.T) {
	cfg := NewDefault().WithDeliverByStartSequence(42).Build()
	if cfg.DeliverPolicy != jetstream.DeliverByStartSequencePolicy {
		t.Fatalf("deliver policy %v", cfg.DeliverPolicy)
	}
	if cfg.OptStartSeq != 42 {
		t.Fatalf("opt start seq %d", cfg.OptStartSeq)
	}

	cfg = NewDefault().WithDeliverByStartSequence(0).Build()
	if cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Fatalf("deliver policy %v", cfg.DeliverPolicy)
	}
	if cfg.OptStartSeq != 0 {
		t.Fatalf("opt start seq %d", cfg.OptStartSeq)
	}
}
