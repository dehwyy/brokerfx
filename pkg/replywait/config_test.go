package replywait

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func validConfig() Config {
	return Config{
		Stream:            "BALANCE_REPLY",
		ReplySubject:      "balance.result.balanceapi.balanceapi-7f9c-x2",
		CorrelationFunc:   func(jetstream.Msg) (string, error) { return "", nil },
		DefaultTimeout:    5 * time.Second,
		InactiveThreshold: 10 * time.Second,
		DrainTimeout:      3 * time.Second,
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(Config) Config
		wantErr bool
	}{
		{
			name: "empty stream",
			mutate: func(c Config) Config {
				c.Stream = ""
				return c
			},
			wantErr: true,
		},
		{
			name: "reply subject with wildcard star",
			mutate: func(c Config) Config {
				c.ReplySubject = "balance.result.*.balanceapi-7f9c-x2"
				return c
			},
			wantErr: true,
		},
		{
			name: "reply subject with wildcard gt",
			mutate: func(c Config) Config {
				c.ReplySubject = "balance.result.balanceapi.>"
				return c
			},
			wantErr: true,
		},
		{
			name: "reply subject with space",
			mutate: func(c Config) Config {
				c.ReplySubject = "balance.result.balanceapi. x2"
				return c
			},
			wantErr: true,
		},
		{
			name: "reply subject with empty token",
			mutate: func(c Config) Config {
				c.ReplySubject = "balance..balanceapi.balanceapi-7f9c-x2"
				return c
			},
			wantErr: true,
		},
		{
			name: "reply subject with fewer than 4 tokens",
			mutate: func(c Config) Config {
				c.ReplySubject = "balance.result.balanceapi"
				return c
			},
			wantErr: true,
		},
		{
			name: "missing correlation func",
			mutate: func(c Config) Config {
				c.CorrelationFunc = nil
				return c
			},
			wantErr: true,
		},
		{
			name: "inactive threshold equal to default timeout",
			mutate: func(c Config) Config {
				c.InactiveThreshold = c.DefaultTimeout
				return c
			},
			wantErr: true,
		},
		{
			name: "inactive threshold less than default timeout",
			mutate: func(c Config) Config {
				c.InactiveThreshold = c.DefaultTimeout - time.Second
				return c
			},
			wantErr: true,
		},
		{
			name: "drain timeout zero",
			mutate: func(c Config) Config {
				c.DrainTimeout = 0
				return c
			},
			wantErr: true,
		},
		{
			name: "drain timeout negative",
			mutate: func(c Config) Config {
				c.DrainTimeout = -time.Second
				return c
			},
			wantErr: true,
		},
		{
			name:    "valid config",
			mutate:  func(c Config) Config { return c },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.mutate(validConfig())
			err := cfg.Validate()

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("expected error to wrap ErrInvalidConfig, got %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestInstanceFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		podName  string
		unset    bool
		wantErr  bool
		wantName string
	}{
		{
			name:    "unset",
			unset:   true,
			wantErr: true,
		},
		{
			name:    "empty",
			podName: "",
			wantErr: true,
		},
		{
			name:    "contains dot",
			podName: "balanceapi-7f9c-x2.local",
			wantErr: true,
		},
		{
			name:     "valid",
			podName:  "balanceapi-7f9c-x2",
			wantErr:  false,
			wantName: "balanceapi-7f9c-x2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unset {
				t.Setenv("POD_NAME", "")
			} else {
				t.Setenv("POD_NAME", tt.podName)
			}

			instance, err := InstanceFromEnv()

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !errors.Is(err, ErrInvalidInstance) {
					t.Fatalf("expected error to wrap ErrInvalidInstance, got %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if instance != tt.wantName {
				t.Fatalf("expected instance %q, got %q", tt.wantName, instance)
			}
		})
	}
}
