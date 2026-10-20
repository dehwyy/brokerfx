package outbox

import (
	"testing"
	"time"
)

func clearRetryEnv(t *testing.T) {
	t.Helper()

	t.Setenv(
		envRetryBackoffBase,
		"",
	)
	t.Setenv(
		envRetryBackoffMax,
		"",
	)
	t.Setenv(
		envRetryJitter,
		"",
	)
}

func newRetryRelay(cfg Config) *OutboxRelay {
	return NewRelay(RelayDeps{
		Store:  &OutboxStore{},
		Config: cfg,
	})
}

func TestResolveRetryWithoutEnvKeepsConfig(t *testing.T) {
	clearRetryEnv(t)

	for name, cfg := range map[string]Config{
		"default":     DefaultConfig(),
		"recommended": RecommendedConfig(),
	} {
		t.Run(
			name,
			func(t *testing.T) {
				relay := newRetryRelay(cfg)

				if relay.config.RetryBackoffBase != cfg.RetryBackoffBase ||
					relay.config.RetryBackoffMax != cfg.RetryBackoffMax ||
					relay.config.MaxAttempts != cfg.MaxAttempts ||
					relay.config.RetryJitter {
					t.Fatalf("retry config changed without env: %+v", relay.config)
				}

				for attempts := 0; attempts <= 40; attempts++ {
					want := nextAttemptDelay(
						attempts,
						cfg.RetryBackoffBase,
						cfg.RetryBackoffMax,
					)
					if got := relay.retryDelay(attempts); got != want {
						t.Fatalf("retryDelay(%d) = %s, want %s", attempts, got, want)
					}
				}
			},
		)
	}
}

func TestResolveRetryEnvAppliesOnlyWithoutCodeBackoff(t *testing.T) {
	clearRetryEnv(t)
	t.Setenv(
		envRetryBackoffBase,
		"100ms",
	)
	t.Setenv(
		envRetryBackoffMax,
		"4s",
	)

	defaults := newRetryRelay(DefaultConfig())
	if defaults.config.RetryBackoffBase != 100*time.Millisecond || defaults.config.RetryBackoffMax != 4*time.Second {
		t.Fatalf("default config got base %s max %s", defaults.config.RetryBackoffBase, defaults.config.RetryBackoffMax)
	}
	if defaults.config.MaxAttempts != 0 {
		t.Fatalf("default MaxAttempts changed to %d", defaults.config.MaxAttempts)
	}

	recommended := newRetryRelay(RecommendedConfig())
	if recommended.config.RetryBackoffBase != 2*time.Second || recommended.config.RetryBackoffMax != 5*time.Minute {
		t.Fatalf("recommended config got base %s max %s", recommended.config.RetryBackoffBase, recommended.config.RetryBackoffMax)
	}
	if recommended.config.MaxAttempts != 20 {
		t.Fatalf("recommended MaxAttempts changed to %d", recommended.config.MaxAttempts)
	}
}

func TestResolveRetryEnvBounds(t *testing.T) {
	cases := []struct {
		name     string
		base     string
		max      string
		wantBase time.Duration
		wantMax  time.Duration
	}{
		{
			name:     "base without max caps at 5s",
			base:     "100ms",
			wantBase: 100 * time.Millisecond,
			wantMax:  5 * time.Second,
		},
		{
			name:     "max below base becomes base",
			base:     "10s",
			max:      "1s",
			wantBase: 10 * time.Second,
			wantMax:  10 * time.Second,
		},
		{
			name: "max without base is ignored",
			max:  "4s",
		},
		{
			name: "garbage base is ignored",
			base: "abc",
			max:  "4s",
		},
		{
			name: "zero base is ignored",
			base: "0",
			max:  "4s",
		},
		{
			name: "negative base is ignored",
			base: "-1s",
			max:  "4s",
		},
		{
			name:     "garbage max falls back to 5s",
			base:     "100ms",
			max:      "abc",
			wantBase: 100 * time.Millisecond,
			wantMax:  5 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				clearRetryEnv(t)
				t.Setenv(
					envRetryBackoffBase,
					tc.base,
				)
				t.Setenv(
					envRetryBackoffMax,
					tc.max,
				)

				relay := newRetryRelay(DefaultConfig())
				if relay.config.RetryBackoffBase != tc.wantBase || relay.config.RetryBackoffMax != tc.wantMax {
					t.Fatalf("got base %s max %s, want base %s max %s", relay.config.RetryBackoffBase, relay.config.RetryBackoffMax, tc.wantBase, tc.wantMax)
				}
				if relay.config.MaxAttempts != 0 {
					t.Fatalf("MaxAttempts changed to %d", relay.config.MaxAttempts)
				}
			},
		)
	}
}

func TestResolveRetryJitterEnv(t *testing.T) {
	cases := map[string]bool{
		"":      false,
		"true":  true,
		"TRUE":  true,
		"1":     true,
		"false": false,
		"0":     false,
		"yes":   false,
	}

	for value, want := range cases {
		t.Run(
			value,
			func(t *testing.T) {
				clearRetryEnv(t)
				t.Setenv(
					envRetryJitter,
					value,
				)

				relay := newRetryRelay(RecommendedConfig())
				if relay.config.RetryJitter != want {
					t.Fatalf("jitter %q = %v, want %v", value, relay.config.RetryJitter, want)
				}
				if relay.config.RetryBackoffBase != 2*time.Second || relay.config.RetryBackoffMax != 5*time.Minute {
					t.Fatalf("jitter env changed backoff: base %s max %s", relay.config.RetryBackoffBase, relay.config.RetryBackoffMax)
				}
			},
		)
	}
}

func TestResolveRetryKeepsConfigJitter(t *testing.T) {
	clearRetryEnv(t)

	cfg := RecommendedConfig()
	cfg.RetryJitter = true

	relay := newRetryRelay(cfg)
	if !relay.config.RetryJitter {
		t.Fatal("RetryJitter from Config was dropped")
	}
}
