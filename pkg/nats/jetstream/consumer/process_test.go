package consumer

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/middleware"
)

type lockedBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLog(t *testing.T) *lockedBuf {
	t.Helper()
	buf := &lockedBuf{}
	prev := log.Logger
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = prev })
	return buf
}

type settleMsg struct {
	jetstream.Msg

	mu     sync.Mutex
	done   bool
	calls  []string
	nakErr error
	ackErr error
}

func (m *settleMsg) Subject() string { return "test.subject" }

func (m *settleMsg) settle(name string, forced error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, name)
	if forced != nil {
		return forced
	}
	if m.done {
		return jetstream.ErrMsgAlreadyAckd
	}
	m.done = true
	return nil
}

func (m *settleMsg) Ack() error                       { return m.settle("ack", m.ackErr) }
func (m *settleMsg) Nak() error                       { return m.settle("nak", m.nakErr) }
func (m *settleMsg) NakWithDelay(time.Duration) error { return m.settle("nakdelay", nil) }
func (m *settleMsg) Term() error                      { return m.settle("term", nil) }
func (m *settleMsg) Headers() nats.Header             { return nats.Header{} }
func (m *settleMsg) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "progress")
	if m.done {
		return jetstream.ErrMsgAlreadyAckd
	}
	return nil
}

func (m *settleMsg) count(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.calls {
		if c == name {
			n++
		}
	}
	return n
}

func handlerOf(fn func(msg jetstream.Msg) error) Opts {
	return Opts{HandlerFunc: func(_ context.Context, msg jetstream.Msg) error { return fn(msg) }}
}

func TestProcessNakWithDelayThenErrorIsWarnOnly(t *testing.T) {
	buf := captureLog(t)
	msg := &settleMsg{}
	process(msg, handlerOf(func(m jetstream.Msg) error {
		if err := m.NakWithDelay(time.Second); err != nil {
			t.Fatal(err)
		}
		return errors.New("transient")
	}))
	out := buf.String()
	if strings.Count(out, "handler failed, message already settled by handler") != 1 {
		t.Fatalf("want %v got %v", 1, strings.Count(out, "handler failed, message already settled by handler"))
	}
	if strings.Contains(out, `"level":"error"`) {
		t.Fatalf("unexpected %s in %s", `"level":"error"`, out)
	}
	if msg.count("nakdelay") != 1 {
		t.Fatalf("want %v got %v", 1, msg.count("nakdelay"))
	}
}

func TestProcessTermThenNilIsQuietAndRunsAfterMiddleware(t *testing.T) {
	buf := captureLog(t)
	msg := &settleMsg{}
	ran := 0
	opts := handlerOf(func(m jetstream.Msg) error { return m.Term() })
	opts.AfterHandlerMiddleware = []middleware.Middleware{
		func(ctx context.Context, _ jetstream.Msg) (context.Context, error) {
			ran++
			return ctx, nil
		},
	}
	process(msg, opts)
	if strings.Contains(buf.String(), "failed to ACK") {
		t.Fatalf("unexpected %s in %s", "failed to ACK", buf.String())
	}
	if strings.Contains(buf.String(), `"level":"error"`) {
		t.Fatalf("unexpected %s in %s", `"level":"error"`, buf.String())
	}
	if ran != 1 {
		t.Fatalf("want %v got %v", 1, ran)
	}
}

func TestProcessPanicAfterSettleLogsPanicOnly(t *testing.T) {
	buf := captureLog(t)
	msg := &settleMsg{}
	process(msg, handlerOf(func(m jetstream.Msg) error {
		_ = m.NakWithDelay(time.Second)
		panic("boom")
	}))
	out := buf.String()
	if !strings.Contains(out, "consumer handler panic") {
		t.Fatalf("missing %s in %s", "consumer handler panic", out)
	}
	if strings.Contains(out, "failed to NAK after panic") {
		t.Fatalf("unexpected %s in %s", "failed to NAK after panic", out)
	}
}

func TestProcessSettledHandlerNoHeartbeatWarn(t *testing.T) {
	restore := inProgressIntervalForTest(10 * time.Millisecond)
	defer restore()
	buf := captureLog(t)
	msg := &settleMsg{}
	process(msg, handlerOf(func(m jetstream.Msg) error {
		_ = m.NakWithDelay(time.Second)
		time.Sleep(60 * time.Millisecond)
		return nil
	}))
	if strings.Contains(buf.String(), "InProgress heartbeat") {
		t.Fatalf("unexpected %s in %s", "InProgress heartbeat", buf.String())
	}
}

func TestProcessUnsettledErrorNaksAndLogsError(t *testing.T) {
	buf := captureLog(t)
	msg := &settleMsg{}
	process(msg, handlerOf(func(jetstream.Msg) error { return errors.New("bad") }))
	if msg.count("nak") != 1 {
		t.Fatalf("want %v got %v", 1, msg.count("nak"))
	}
	if !strings.Contains(buf.String(), `"level":"error"`) {
		t.Fatalf("missing %s in %s", `"level":"error"`, buf.String())
	}
	if !strings.Contains(buf.String(), "handler failed") {
		t.Fatalf("missing %s in %s", "handler failed", buf.String())
	}
}

func TestProcessNakConnectionClosedStillErrors(t *testing.T) {
	buf := captureLog(t)
	msg := &settleMsg{nakErr: nats.ErrConnectionClosed}
	process(msg, handlerOf(func(jetstream.Msg) error { return errors.New("bad") }))
	if !strings.Contains(buf.String(), "failed to NAK after handler error") {
		t.Fatalf("missing %s in %s", "failed to NAK after handler error", buf.String())
	}
}

func TestProcessAckConnectionClosedStillErrors(t *testing.T) {
	buf := captureLog(t)
	msg := &settleMsg{ackErr: nats.ErrConnectionClosed}
	process(msg, handlerOf(func(jetstream.Msg) error { return nil }))
	if !strings.Contains(buf.String(), "failed to ACK after successful handler") {
		t.Fatalf("missing %s in %s", "failed to ACK after successful handler", buf.String())
	}
}

func TestProcessBeforeMiddlewareErrorNaksWithoutHandler(t *testing.T) {
	captureLog(t)
	msg := &settleMsg{}
	called := false
	opts := handlerOf(func(jetstream.Msg) error { called = true; return nil })
	opts.BeforeHandlerMiddleware = []middleware.Middleware{
		func(ctx context.Context, _ jetstream.Msg) (context.Context, error) {
			return ctx, errors.New("mw")
		},
	}
	process(msg, opts)
	if called {
		t.Fatal("unexpected")
	}
	if msg.count("nak") != 1 {
		t.Fatalf("want %v got %v", 1, msg.count("nak"))
	}
}
