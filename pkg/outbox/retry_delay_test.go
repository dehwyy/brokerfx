package outbox

import (
	"math"
	"testing"
	"time"
)

func jitterRelay(t *testing.T, cfg Config) *OutboxRelay {
	t.Helper()
	clearRetryEnv(t)

	cfg.RetryJitter = true

	return newRetryRelay(cfg)
}

func TestRetryDelayJitterSeamBounds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetryBackoffBase = 100 * time.Millisecond
	cfg.RetryBackoffMax = 4 * time.Second
	relay := jitterRelay(
		t,
		cfg,
	)

	for attempts := 1; attempts <= 8; attempts++ {
		delay := nextAttemptDelay(
			attempts,
			cfg.RetryBackoffBase,
			cfg.RetryBackoffMax,
		)

		relay.jitter = func(n int64) int64 {
			return 0
		}
		if got := relay.retryDelay(attempts); got != delay {
			t.Fatalf("attempt %d with zero jitter = %s, want %s", attempts, got, delay)
		}

		var spread int64
		relay.jitter = func(n int64) int64 {
			spread = n
			return n - 1
		}
		if got := relay.retryDelay(attempts); got != delay+delay/2-1 {
			t.Fatalf("attempt %d with top jitter = %s, want %s", attempts, got, delay+delay/2-1)
		}
		if spread != int64(delay/2) {
			t.Fatalf("attempt %d jitter spread = %d, want %d", attempts, spread, int64(delay/2))
		}
	}
}

func TestRetryDelayJitterStaysInRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetryBackoffBase = 100 * time.Millisecond
	cfg.RetryBackoffMax = 4 * time.Second
	relay := jitterRelay(
		t,
		cfg,
	)

	for i := range 10000 {
		attempts := i%8 + 1
		delay := nextAttemptDelay(
			attempts,
			cfg.RetryBackoffBase,
			cfg.RetryBackoffMax,
		)
		got := relay.retryDelay(attempts)
		if got < delay || got >= delay+delay/2 {
			t.Fatalf("attempt %d delay %s outside [%s; %s)", attempts, got, delay, delay+delay/2)
		}
	}
}

func TestRetryDelayZeroBaseSkipsJitter(t *testing.T) {
	relay := jitterRelay(
		t,
		DefaultConfig(),
	)

	called := false
	relay.jitter = func(n int64) int64 {
		called = true
		return n
	}

	for attempts := 0; attempts <= 40; attempts++ {
		if got := relay.retryDelay(attempts); got != 0 {
			t.Fatalf("retryDelay(%d) = %s, want 0", attempts, got)
		}
	}
	if called {
		t.Fatal("jitter called for a zero delay")
	}
}

func TestRetryDelayJitterNeverShortensPathToParked(t *testing.T) {
	clearRetryEnv(t)
	cfg := RecommendedConfig()
	plain := newRetryRelay(cfg)
	jittered := jitterRelay(
		t,
		cfg,
	)

	var plainTotal, jitteredTotal time.Duration
	for attempts := 1; attempts < cfg.MaxAttempts; attempts++ {
		plainDelay := plain.retryDelay(attempts)
		jitteredDelay := jittered.retryDelay(attempts)
		if jitteredDelay < plainDelay {
			t.Fatalf("attempt %d jittered %s below plain %s", attempts, jitteredDelay, plainDelay)
		}
		plainTotal += plainDelay
		jitteredTotal += jitteredDelay
	}
	if jitteredTotal < plainTotal {
		t.Fatalf("jittered path %s shorter than plain %s", jitteredTotal, plainTotal)
	}
}

func TestRetryDelayLargeAttemptsDoNotOverflow(t *testing.T) {
	recommended := jitterRelay(
		t,
		RecommendedConfig(),
	)
	if got := recommended.retryDelay(1000); got < 5*time.Minute || got >= 7*time.Minute+30*time.Second {
		t.Fatalf("recommended retryDelay(1000) = %s", got)
	}

	clearRetryEnv(t)
	t.Setenv(
		envRetryBackoffBase,
		"100ms",
	)
	t.Setenv(
		envRetryJitter,
		"true",
	)
	fromEnv := newRetryRelay(DefaultConfig())
	if got := fromEnv.retryDelay(1000); got < 5*time.Second || got >= 7*time.Second+500*time.Millisecond {
		t.Fatalf("env retryDelay(1000) = %s", got)
	}

	huge := DefaultConfig()
	huge.RetryBackoffBase = time.Duration(math.MaxInt64)
	edge := jitterRelay(
		t,
		huge,
	)
	edge.jitter = func(n int64) int64 {
		return n - 1
	}
	if got := edge.retryDelay(1); got != huge.RetryBackoffBase {
		t.Fatalf("retryDelay at MaxInt64 = %s, want %s", got, huge.RetryBackoffBase)
	}
}

func TestUnknownRetryDelay(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TxDrain = true
	cfg.TxDrainPublishTimeout = 2 * time.Second

	clearRetryEnv(t)
	plain := newRetryRelay(cfg)
	plain.jitter = func(n int64) int64 {
		t.Fatal("jitter called with RetryJitter off")
		return 0
	}
	if got := plain.unknownRetryDelay(); got != txDrainUnknownRetryFloor {
		t.Fatalf("unknown delay without jitter = %s, want %s", got, txDrainUnknownRetryFloor)
	}

	jittered := jitterRelay(
		t,
		cfg,
	)
	jittered.jitter = func(n int64) int64 {
		return n - 1
	}
	if got := jittered.unknownRetryDelay(); got != txDrainUnknownRetryFloor+txDrainUnknownRetryFloor/2-1 {
		t.Fatalf("unknown delay with top jitter = %s", got)
	}

	jittered = jitterRelay(
		t,
		cfg,
	)
	for range 10000 {
		got := jittered.unknownRetryDelay()
		if got < txDrainUnknownRetryFloor || got >= txDrainUnknownRetryFloor+txDrainUnknownRetryFloor/2 {
			t.Fatalf("unknown delay %s outside [%s; %s)", got, txDrainUnknownRetryFloor, txDrainUnknownRetryFloor+txDrainUnknownRetryFloor/2)
		}
	}
}

func TestRetryDelayWithoutSeamUsesRand(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetryBackoffBase = time.Second
	cfg.RetryBackoffMax = 4 * time.Second
	cfg.RetryJitter = true
	relay := &OutboxRelay{
		config: cfg,
	}

	got := relay.retryDelay(1)
	if got < time.Second || got >= 1500*time.Millisecond {
		t.Fatalf("retryDelay without seam = %s", got)
	}
}
