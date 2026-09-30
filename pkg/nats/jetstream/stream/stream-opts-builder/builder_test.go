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

var allRoles = []Role{RoleEvent, RoleCommand, RoleResult, RoleReply, RoleDLQ}

func clearEnv(t *testing.T) {
	t.Helper()
	t.Setenv(EnvMaxBytes, "")
	t.Setenv(EnvCriticalMaxBytes, "")
	t.Setenv(EnvDLQMaxBytes, "")
}

func roleBuilder(role Role) *StreamOptsBuilder {
	return NewDefault().WithName("S").WithSubjects([]string{"s.>"}).WithRole(role)
}

func TestRoleDefaultsToEvent(t *testing.T) {
	clearEnv(t)

	cfg := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).Build()
	if cfg.MaxBytes != 256<<20 || cfg.Discard != jetstream.DiscardOld {
		t.Fatalf("got (%d, %v), want 256 MiB / DiscardOld", cfg.MaxBytes, cfg.Discard)
	}
}

func TestRoleLimits(t *testing.T) {
	clearEnv(t)

	tests := []struct {
		role        Role
		wantMax     int64
		wantDiscard jetstream.DiscardPolicy
	}{
		{RoleEvent, 256 << 20, jetstream.DiscardOld},
		{RoleCommand, 1 << 30, jetstream.DiscardNew},
		{RoleResult, 1 << 30, jetstream.DiscardNew},
		{RoleReply, 1 << 30, jetstream.DiscardNew},
		{RoleDLQ, 256 << 20, jetstream.DiscardNew},
	}

	for _, tt := range tests {
		t.Run(tt.role.String(), func(t *testing.T) {
			cfg := roleBuilder(tt.role).Build()
			if cfg.MaxBytes != tt.wantMax || cfg.Discard != tt.wantDiscard {
				t.Fatalf("builder got (%d, %v), want (%d, %v)", cfg.MaxBytes, cfg.Discard, tt.wantMax, tt.wantDiscard)
			}

			limits, err := LimitsFor(tt.role)
			if err != nil {
				t.Fatalf("LimitsFor err %v", err)
			}
			if limits.MaxBytes != tt.wantMax || limits.Discard != tt.wantDiscard {
				t.Fatalf("LimitsFor got %+v", limits)
			}
		})
	}
}

func TestRoleEnvOverridesAreIndependent(t *testing.T) {
	t.Setenv(EnvMaxBytes, "10485760")
	t.Setenv(EnvCriticalMaxBytes, "20971520")
	t.Setenv(EnvDLQMaxBytes, "31457280")

	if got := roleBuilder(RoleEvent).Build().MaxBytes; got != 10<<20 {
		t.Fatalf("event max bytes %d", got)
	}
	if got := roleBuilder(RoleDLQ).Build().MaxBytes; got != 30<<20 {
		t.Fatalf("dlq max bytes %d", got)
	}
	for _, role := range []Role{RoleCommand, RoleResult, RoleReply} {
		if got := roleBuilder(role).Build().MaxBytes; got != 20<<20 {
			t.Fatalf("%v max bytes %d", role, got)
		}
	}
}

func TestExplicitMaxBytesWinsOverRoleInAnyOrder(t *testing.T) {
	clearEnv(t)

	before := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).WithMaxBytes(5 << 20).WithRole(RoleCommand).Build()
	after := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).WithRole(RoleCommand).WithMaxBytes(5 << 20).Build()
	for _, cfg := range []jetstream.StreamConfig{before, after} {
		if cfg.MaxBytes != 5<<20 {
			t.Fatalf("max bytes %d", cfg.MaxBytes)
		}
		if cfg.Discard != jetstream.DiscardNew {
			t.Fatalf("discard %v, role policy must still apply", cfg.Discard)
		}
	}
}

func TestExplicitDiscardWinsOverRoleInAnyOrder(t *testing.T) {
	clearEnv(t)

	before := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).WithDiscard(jetstream.DiscardOld).WithRole(RoleCommand).Build()
	after := NewDefault().WithName("S").WithSubjects([]string{"s.>"}).WithRole(RoleCommand).WithDiscard(jetstream.DiscardOld).Build()
	for _, cfg := range []jetstream.StreamConfig{before, after} {
		if cfg.Discard != jetstream.DiscardOld {
			t.Fatalf("discard %v", cfg.Discard)
		}
		if cfg.MaxBytes != 1<<30 {
			t.Fatalf("max bytes %d, role limit must still apply", cfg.MaxBytes)
		}
	}
}

func TestLastRoleWins(t *testing.T) {
	clearEnv(t)

	cfg := roleBuilder(RoleCommand).WithRole(RoleEvent).Build()
	if cfg.MaxBytes != 256<<20 || cfg.Discard != jetstream.DiscardOld {
		t.Fatalf("got (%d, %v)", cfg.MaxBytes, cfg.Discard)
	}
}

func TestUnknownRoleIsError(t *testing.T) {
	clearEnv(t)

	for _, role := range []Role{-1, Role(len(allRoles))} {
		b := roleBuilder(role)
		if !errors.Is(b.Err(), ErrUnknownRole) {
			t.Fatalf("role %d: err %v, want ErrUnknownRole", role, b.Err())
		}
		if _, err := LimitsFor(role); !errors.Is(err, ErrUnknownRole) {
			t.Fatalf("role %d: LimitsFor err %v", role, err)
		}
	}
}

func TestInvalidCriticalOrDLQEnvIsErrorForEveryRole(t *testing.T) {
	for _, name := range []string{EnvCriticalMaxBytes, EnvDLQMaxBytes} {
		for _, raw := range []string{"abc", "-1", "0", "1GiB"} {
			t.Run(name+"="+raw, func(t *testing.T) {
				clearEnv(t)
				t.Setenv(name, raw)

				for _, role := range allRoles {
					if err := roleBuilder(role).Err(); !errors.Is(err, ErrInvalidMaxBytesEnv) {
						t.Fatalf("role %v: err %v, want ErrInvalidMaxBytesEnv", role, err)
					}
					if _, err := LimitsFor(role); !errors.Is(err, ErrInvalidMaxBytesEnv) {
						t.Fatalf("role %v: LimitsFor err %v", role, err)
					}
				}
			})
		}
	}
}

func TestDefaultConstants(t *testing.T) {
	if DefaultCriticalMaxBytes != 1<<30 {
		t.Fatalf("critical default %d, want 1 GiB", DefaultCriticalMaxBytes)
	}
	if DefaultDLQMaxBytes != 256<<20 {
		t.Fatalf("dlq default %d, want 256 MiB", DefaultDLQMaxBytes)
	}
	if !RoleCommand.Critical() || !RoleResult.Critical() || !RoleReply.Critical() || !RoleDLQ.Critical() || RoleEvent.Critical() {
		t.Fatal("only command/result/reply/DLQ roles are critical")
	}
}
