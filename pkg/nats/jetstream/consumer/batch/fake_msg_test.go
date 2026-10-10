package batch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeMsg struct {
	jetstream.Msg

	mu        sync.Mutex
	data      []byte
	subject   string
	headers   nats.Header
	delivered uint64
	done      bool
	calls     []string
}

func newMsg(data string, delivered uint64) *fakeMsg {
	return &fakeMsg{
		data:      []byte(data),
		subject:   "test.subject",
		headers:   nats.Header{},
		delivered: delivered,
	}
}

func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Headers() nats.Header { return m.headers }

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		NumDelivered: m.delivered,
	}, nil
}

func (m *fakeMsg) settle(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, name)
	if m.done {
		return jetstream.ErrMsgAlreadyAckd
	}
	m.done = true
	return nil
}

func (m *fakeMsg) Ack() error  { return m.settle("ack") }
func (m *fakeMsg) Nak() error  { return m.settle("nak") }
func (m *fakeMsg) Term() error { return m.settle("term") }

func (m *fakeMsg) NakWithDelay(delay time.Duration) error {
	return m.settle(fmt.Sprintf("nakdelay:%s", delay))
}

func (m *fakeMsg) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		return jetstream.ErrMsgAlreadyAckd
	}
	m.calls = append(m.calls, "progress")
	return nil
}

func (m *fakeMsg) settles() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.calls))
	for _, call := range m.calls {
		if call != "progress" {
			out = append(out, call)
		}
	}
	return out
}

func (m *fakeMsg) count(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, call := range m.calls {
		if call == name {
			n++
		}
	}
	return n
}

func asMsgs(msgs ...*fakeMsg) []jetstream.Msg {
	out := make([]jetstream.Msg, len(msgs))
	for i, msg := range msgs {
		out[i] = msg
	}
	return out
}

type poisonCall struct {
	msg jetstream.Msg
	err error
}

type poisonRecorder struct {
	mu    sync.Mutex
	calls []poisonCall
	err   error
}

func (r *poisonRecorder) fn(_ context.Context, msg jetstream.Msg, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, poisonCall{msg: msg, err: err})
	return r.err
}

func (r *poisonRecorder) snapshot() []poisonCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]poisonCall(nil), r.calls...)
}

func testConsumer(handler Handler, maxAttempts int, poison *poisonRecorder) *Consumer {
	c := &Consumer{
		handler:        handler,
		delays:         DefaultDelays,
		maxAttempts:    maxAttempts,
		heartbeatEvery: time.Hour,
	}
	if poison != nil {
		c.onPoison = poison.fn
	}
	return c
}

func expectSettles(t *testing.T, msg *fakeMsg, want ...string) {
	t.Helper()
	got := msg.settles()
	if len(got) != len(want) {
		t.Fatalf("msg %s: want %v got %v", msg.data, want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("msg %s: want %v got %v", msg.data, want, got)
		}
	}
}

func resultsHandler(results ...Result) Handler {
	return func(_ context.Context, _ []jetstream.Msg) []Result {
		return results
	}
}
