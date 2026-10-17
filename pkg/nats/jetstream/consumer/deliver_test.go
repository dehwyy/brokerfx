package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type frameSettleMsg struct {
	settleMsg

	headers nats.Header
	data    []byte
}

func (m *frameSettleMsg) Headers() nats.Header { return m.headers }
func (m *frameSettleMsg) Data() []byte         { return m.data }

func (m *frameSettleMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}

func newFrameSettleMsg(t *testing.T, payloads ...string) *frameSettleMsg {
	t.Helper()
	encoded := make([][]byte, 0, len(payloads))
	ids := make([]string, 0, len(payloads))
	for _, payload := range payloads {
		element, err := frame.Marshal(frame.Element{Subject: "test.subject", Payload: []byte(payload)})
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, element)
		ids = append(ids, payload)
	}
	headers := nats.Header{}
	for key, value := range frame.WireHeaders(ids) {
		headers.Set(key, value)
	}
	return &frameSettleMsg{headers: headers, data: frame.Join(encoded)}
}

func TestDeliverRunsEveryElementAndNaksTheFrameOnAFailure(t *testing.T) {
	msg := newFrameSettleMsg(t, "a", "b", "c")
	var seen []string
	deliver(msg, handlerOf(func(m jetstream.Msg) error {
		seen = append(seen, string(m.Data()))
		if string(m.Data()) == "b" {
			return errors.New("transient")
		}
		return nil
	}))

	if len(seen) != 3 || seen[2] != "c" {
		t.Fatalf("handler saw %v", seen)
	}
	if msg.count("nak") != 1 || msg.count("ack") != 0 {
		t.Fatalf("frame calls %v", msg.calls)
	}
}

func TestDeliverAcksTheFrameOnce(t *testing.T) {
	msg := newFrameSettleMsg(t, "a", "b")
	deliver(msg, handlerOf(func(jetstream.Msg) error { return nil }))

	if msg.count("ack") != 1 || msg.count("nak") != 0 {
		t.Fatalf("frame calls %v", msg.calls)
	}
}

func TestDeliverPoisonsABrokenFrame(t *testing.T) {
	msg := newFrameSettleMsg(t, "a")
	msg.data = []byte("garbage")
	var poisoned error
	opts := handlerOf(func(jetstream.Msg) error {
		t.Fatal("handler must not see a broken frame")
		return nil
	})
	opts.OnPoison = func(_ context.Context, got jetstream.Msg, err error) error {
		if got != msg {
			t.Fatal("OnPoison must get the frame")
		}
		poisoned = err
		return nil
	}

	deliver(msg, opts)

	if !errors.Is(poisoned, frame.ErrBadFrame) || msg.count("term") != 1 {
		t.Fatalf("poisoned %v, calls %v", poisoned, msg.calls)
	}
}

func TestDeliverNaksABrokenFrameWhenOnPoisonFails(t *testing.T) {
	msg := newFrameSettleMsg(t, "a")
	msg.headers.Set(frame.HeaderVersion, "2")
	opts := handlerOf(func(jetstream.Msg) error { return nil })
	opts.OnPoison = func(context.Context, jetstream.Msg, error) error { panic("dlq down") }

	deliver(msg, opts)

	if msg.count("nak") != 1 || msg.count("term") != 0 {
		t.Fatalf("frame calls %v", msg.calls)
	}
}

func TestDeliverHandsABrokenFrameToTheHandlerWithoutOnPoison(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(msg *frameSettleMsg)
		handler error
		want    string
	}{
		{
			name:    "bad msgpack, handler dead-letters it",
			mutate:  func(msg *frameSettleMsg) { msg.data = nil },
			handler: nil,
			want:    "ack",
		},
		{
			name:    "unknown version, handler fails",
			mutate:  func(msg *frameSettleMsg) { msg.headers.Set(frame.HeaderVersion, "2") },
			handler: errors.New("cannot decode"),
			want:    "nak",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := newFrameSettleMsg(t, "a", "b")
			tc.mutate(msg)
			var seen []jetstream.Msg
			deliver(msg, handlerOf(func(got jetstream.Msg) error {
				seen = append(seen, got)
				return tc.handler
			}))

			if len(seen) != 1 || seen[0] != msg {
				t.Fatalf("handler must get the raw frame once, got %v", seen)
			}
			if msg.count(tc.want) != 1 || msg.count("term") != 0 {
				t.Fatalf("frame calls %v", msg.calls)
			}
		})
	}
}
