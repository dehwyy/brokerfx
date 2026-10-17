package frame

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeMsg struct {
	jetstream.Msg

	mu       sync.Mutex
	subject  string
	headers  nats.Header
	data     []byte
	calls    []string
	delays   []time.Duration
	progress int
}

func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Headers() nats.Header { return m.headers }
func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Reply() string        { return "$JS.ACK.test" }

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 3}, nil
}

func (m *fakeMsg) record(call string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, call)
	return nil
}

func (m *fakeMsg) Ack() error                      { return m.record("ack") }
func (m *fakeMsg) DoubleAck(context.Context) error { return m.record("double_ack") }
func (m *fakeMsg) Nak() error                      { return m.record("nak") }
func (m *fakeMsg) Term() error                     { return m.record("term") }

func (m *fakeMsg) NakWithDelay(delay time.Duration) error {
	m.mu.Lock()
	m.delays = append(m.delays, delay)
	m.mu.Unlock()
	return m.record("nak_delay")
}

func (m *fakeMsg) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.progress++
	return nil
}

func (m *fakeMsg) snapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}

func framed(subject string, n int) *fakeMsg {
	encoded := make([][]byte, 0, n)
	ids := make([]string, 0, n)
	for i := range n {
		id := string(rune('a' + i%26))
		element, err := Marshal(Element{
			Subject: subject,
			Headers: map[string][]string{jetstream.MsgIDHeader: {id}},
			Payload: []byte{byte(i)},
		})
		if err != nil {
			panic(err)
		}
		encoded = append(encoded, element)
		ids = append(ids, id)
	}
	headers := nats.Header{}
	for k, v := range WireHeaders(ids) {
		headers.Set(k, v)
	}
	return &fakeMsg{subject: subject, headers: headers, data: Join(encoded)}
}
