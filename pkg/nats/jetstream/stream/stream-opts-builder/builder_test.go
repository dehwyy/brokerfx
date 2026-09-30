package streamoptsbuilder

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestNewDefaultValues(t *testing.T) {
	t.Setenv(EnvMaxBytes, "")
	cfg := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).Build()
	if cfg.Storage != jetstream.FileStorage {
		t.Fatalf("storage %v", cfg.Storage)
	}
	if cfg.MaxBytes != 256<<20 {
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

func TestNewDefaultMaxBytesIsDefaultConstant(t *testing.T) {
	t.Setenv(EnvMaxBytes, "")

	cfg := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).Build()
	if cfg.MaxBytes != DefaultMaxBytes {
		t.Fatalf("max bytes %d, want %d", cfg.MaxBytes, DefaultMaxBytes)
	}
	if DefaultMaxBytes != 268435456 {
		t.Fatalf("default max bytes %d, want 256 MiB", DefaultMaxBytes)
	}
}

func TestNewDefaultMaxBytesFromEnv(t *testing.T) {
	t.Setenv(EnvMaxBytes, "  1073741824 ")

	b := NewDefault().WithName("S").WithSubjects([]string{"s.>"})
	if err := b.Err(); err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	if got := b.Build().MaxBytes; got != 1<<30 {
		t.Fatalf("max bytes %d", got)
	}
}

func TestWithMaxBytesOverridesEnv(t *testing.T) {
	t.Setenv(EnvMaxBytes, "1073741824")

	cfg := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).WithMaxBytes(5 << 20).Build()
	if cfg.MaxBytes != 5<<20 {
		t.Fatalf("max bytes %d", cfg.MaxBytes)
	}
}

func TestInvalidEnvMaxBytesIsError(t *testing.T) {
	for _, raw := range []string{"abc", "-1", "0", "1.5", "256MiB", "9223372036854775808"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(EnvMaxBytes, raw)

			b := NewDefault().WithName("S").WithSubjects([]string{"s.>"})
			if err := b.Err(); !errors.Is(err, ErrInvalidMaxBytesEnv) {
				t.Fatalf("err %v, want ErrInvalidMaxBytesEnv", err)
			}

			defer func() {
				if recover() == nil {
					t.Fatal("Build must panic on invalid env")
				}
			}()
			b.Build()
		})
	}
}

func TestInvalidEnvSurvivesExplicitMaxBytes(t *testing.T) {
	t.Setenv(EnvMaxBytes, "garbage")

	b := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).WithMaxBytes(5 << 20)
	if !errors.Is(b.Err(), ErrInvalidMaxBytesEnv) {
		t.Fatalf("err %v", b.Err())
	}
}
