package streamoptsbuilder

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestNewDefaultMatchesV019(t *testing.T) {
	cfg := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).Build()
	if cfg.Storage != jetstream.FileStorage {
		t.Fatalf("storage %v", cfg.Storage)
	}
	if cfg.MaxBytes != 2<<30 {
		t.Fatalf("max bytes %d", cfg.MaxBytes)
	}
	if cfg.MaxAge != 12*time.Hour {
		t.Fatalf("max age %v", cfg.MaxAge)
	}
	if cfg.Retention != jetstream.WorkQueuePolicy {
		t.Fatalf("retention %v", cfg.Retention)
	}
	if cfg.MaxMsgsPerSubject != 1000 {
		t.Fatalf("per subject %d", cfg.MaxMsgsPerSubject)
	}
	if cfg.Compression != jetstream.S2Compression {
		t.Fatalf("compression %v", cfg.Compression)
	}
	if cfg.Replicas != 1 {
		t.Fatalf("replicas %d", cfg.Replicas)
	}
	if cfg.Duplicates != 15*time.Minute {
		t.Fatalf("duplicates %v", cfg.Duplicates)
	}
}
