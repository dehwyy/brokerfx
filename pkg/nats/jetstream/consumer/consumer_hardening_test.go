package consumer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gorm.io/gorm"
)

// fakeMsg satisfies jetstream.Msg, recording the ack-lifecycle calls used by the
// hardening helpers. Only the methods under test are implemented; the rest panic
// via the embedded nil interface, surfacing accidental use.
type fakeMsg struct {
	jetstream.Msg

	subject    string
	headers    nats.Header
	data       []byte
	inProgress atomic.Int32
	acks       atomic.Int32
	naks       atomic.Int32
}

func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Headers() nats.Header { return m.headers }
func (m *fakeMsg) Data() []byte         { return m.data }

func (m *fakeMsg) InProgress() error {
	m.inProgress.Add(1)
	return nil
}

func (m *fakeMsg) Ack() error {
	m.acks.Add(1)
	return nil
}

func (m *fakeMsg) Nak() error {
	m.naks.Add(1)
	return nil
}

// TestRunWithHeartbeatPingsInProgress (D2): a handler that runs longer than the
// tick interval must trigger at least one msg.InProgress() heartbeat.
func TestRunWithHeartbeatPingsInProgress(t *testing.T) {
	old := inProgressIntervalForTest(20 * time.Millisecond)
	defer old()

	msg := &fakeMsg{subject: "test.slow"}

	err := runWithHeartbeat(
		context.Background(),
		msg,
		func(ctx context.Context, _ jetstream.Msg) error {
			// Sleep past several tick intervals so heartbeats fire.
			time.Sleep(120 * time.Millisecond)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected handler error: %v", err)
	}

	if got := msg.inProgress.Load(); got < 1 {
		t.Fatalf("expected >=1 InProgress heartbeat, got %d", got)
	}
}

// TestRunWithHeartbeatFastHandlerNoPing: a handler that returns before the first
// tick must not emit a heartbeat (ticker stopped on return).
func TestRunWithHeartbeatFastHandlerNoPing(t *testing.T) {
	old := inProgressIntervalForTest(500 * time.Millisecond)
	defer old()

	msg := &fakeMsg{subject: "test.fast"}

	err := runWithHeartbeat(
		context.Background(),
		msg,
		func(ctx context.Context, _ jetstream.Msg) error {
			return nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected handler error: %v", err)
	}

	if got := msg.inProgress.Load(); got != 0 {
		t.Fatalf("expected 0 heartbeats for fast handler, got %d", got)
	}
}

// fakeTxManager satisfies txmanager.TxManager. Do simply runs fn (the dedup row
// claim is faked via claimMessage), GetConnection returns nil since the faked
// claimer ignores the connection.
type fakeTxManager struct{}

func (fakeTxManager) Do(ctx context.Context, _ string, fn func(ctx context.Context) error) error {
	return fn(ctx)
}
func (fakeTxManager) GetConnection(context.Context) *gorm.DB    { return nil }
func (fakeTxManager) GetRawConnection(context.Context) *gorm.DB { return nil }

// TestWithIdempotencyDedup (D3): first delivery runs the handler and claims the
// key; a redelivery with the same message_id skips the handler. The wrapper
// returns nil in both cases so the consumer Acks.
func TestWithIdempotencyDedup(t *testing.T) {
	// In-memory claimer standing in for the gorm ON CONFLICT DO NOTHING insert.
	seen := map[string]bool{}
	restore := claimMessage
	claimMessage = func(_ *gorm.DB, row ProcessedMessage) (bool, error) {
		if seen[row.MessageID] {
			return false, nil
		}
		seen[row.MessageID] = true
		return true, nil
	}
	defer func() { claimMessage = restore }()

	var handlerCalls atomic.Int32
	wrapped := WithIdempotency(
		fakeTxManager{},
		DefaultIdempotencyKey,
		func(ctx context.Context, _ jetstream.Msg) error {
			handlerCalls.Add(1)
			return nil
		},
	)

	headers := nats.Header{}
	headers.Set(jetstream.MsgIDHeader, "msg-1")
	msg := &fakeMsg{
		subject: "test.idem",
		headers: headers,
	}

	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("first delivery returned error: %v", err)
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("expected handler called once on first delivery, got %d", got)
	}

	// Redelivery: same message_id → handler must NOT run, wrapper returns nil (Ack).
	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("redelivery returned error: %v", err)
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("expected handler NOT called on redelivery, total calls %d", got)
	}
}

// TestWithIdempotencyPropagatesHandlerError: a handler error must bubble up so the
// consumer Naks and the dedup row is rolled back with the transaction.
func TestWithIdempotencyPropagatesHandlerError(t *testing.T) {
	restore := claimMessage
	claimMessage = func(_ *gorm.DB, _ ProcessedMessage) (bool, error) {
		return true, nil
	}
	defer func() { claimMessage = restore }()

	wantErr := errors.New("boom")
	wrapped := WithIdempotency(
		fakeTxManager{},
		nil,
		func(ctx context.Context, _ jetstream.Msg) error {
			return wantErr
		},
	)

	msg := &fakeMsg{
		subject: "test.idem.err",
		headers: nats.Header{},
	}

	if err := wrapped(context.Background(), msg); !errors.Is(err, wantErr) {
		t.Fatalf("expected handler error to propagate, got %v", err)
	}
}

// TestDefaultIdempotencyKeyFallback: with no Nats-Msg-Id header the key is a
// deterministic hash of subject+payload (stable across redeliveries).
func TestDefaultIdempotencyKeyFallback(t *testing.T) {
	msg := &fakeMsg{
		subject: "test.nokey",
		headers: nats.Header{},
		data:    []byte("payload"),
	}

	k1 := DefaultIdempotencyKey(msg)
	k2 := DefaultIdempotencyKey(msg)

	if k1 == "" {
		t.Fatal("expected non-empty fallback key")
	}
	if k1 != k2 {
		t.Fatalf("fallback key not deterministic: %q != %q", k1, k2)
	}
}
