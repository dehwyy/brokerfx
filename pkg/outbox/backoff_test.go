package outbox

import (
	"errors"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
)

func TestNextAttemptDelayGrowsExponentiallyFromBase(t *testing.T) {
	base := 2 * time.Second
	max := time.Hour

	cases := map[int]time.Duration{
		1: base,
		2: base * 2,
		3: base * 4,
		4: base * 8,
	}

	for attempt, want := range cases {
		got := nextAttemptDelay(attempt, base, max)
		if got != want {
			t.Fatalf("nextAttemptDelay(%d, %s, %s) = %s, want %s", attempt, base, max, got, want)
		}
	}
}

func TestNextAttemptDelayCapsAtMax(t *testing.T) {
	base := 2 * time.Second
	max := 10 * time.Second

	got := nextAttemptDelay(10, base, max)
	if got != max {
		t.Fatalf("nextAttemptDelay(10, %s, %s) = %s, want %s (capped)", base, max, got, max)
	}
}

func TestNextAttemptDelayZeroBaseMeansNoBackoff(t *testing.T) {
	got := nextAttemptDelay(5, 0, time.Minute)
	if got != 0 {
		t.Fatalf("nextAttemptDelay with zero base = %s, want 0", got)
	}
}

func TestNextAttemptDelayTreatsAttemptBelowOneAsOne(t *testing.T) {
	base := 3 * time.Second
	max := time.Minute

	got := nextAttemptDelay(0, base, max)
	if got != base {
		t.Fatalf("nextAttemptDelay(0, ...) = %s, want %s (same as attempt 1)", got, base)
	}
}

func TestIsTransportErrorClassifiesConnectionLossSentinels(t *testing.T) {
	transportErrors := []error{
		natsgo.ErrNoServers,
		natsgo.ErrConnectionClosed,
		natsgo.ErrDisconnected,
		natsgo.ErrConnectionReconnecting,
	}

	for _, err := range transportErrors {
		if !isTransportError(err) {
			t.Fatalf("isTransportError(%v) = false, want true", err)
		}
	}
}

func TestIsTransportErrorRejectsOrdinaryPublishFailures(t *testing.T) {
	err := errors.New("simulated publish failure")
	if isTransportError(err) {
		t.Fatalf("isTransportError(%v) = true, want false", err)
	}
}

func TestIsTransportErrorMatchesWrappedSentinels(t *testing.T) {
	wrapped := errors.New("outer: " + natsgo.ErrNoServers.Error())
	if isTransportError(wrapped) {
		t.Fatalf("isTransportError(%v) = true, want false (message match is not errors.Is)", wrapped)
	}

	joined := errors.Join(errors.New("outer"), natsgo.ErrDisconnected)
	if !isTransportError(joined) {
		t.Fatalf("isTransportError(%v) = false, want true (errors.Is must see the wrapped sentinel)", joined)
	}
}
