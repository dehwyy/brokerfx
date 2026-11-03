package batch

import (
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

type itemMsg struct {
	jetstream.Msg

	span trace.Span

	mu      sync.Mutex
	kind    settleKind
	settled bool
}

type itemSpanCarrier interface {
	itemSpanContext() trace.SpanContext
}

func newItemMsgs(msgs []jetstream.Msg, spans itemSpans) []*itemMsg {
	wrapped := make(
		[]*itemMsg,
		len(msgs),
	)
	for i, msg := range msgs {
		wrapped[i] = &itemMsg{
			Msg:  msg,
			span: spans.at(i),
		}
	}

	return wrapped
}

func asHandlerMsgs(wrapped []*itemMsg) []jetstream.Msg {
	msgs := make(
		[]jetstream.Msg,
		len(wrapped),
	)
	for i, msg := range wrapped {
		msgs[i] = msg
	}

	return msgs
}

func (m *itemMsg) itemSpanContext() trace.SpanContext {
	if m.span == nil {
		return trace.SpanContext{}
	}

	return m.span.SpanContext()
}

func (m *itemMsg) record(kind settleKind, err error) {
	if err != nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.settled {
		return
	}

	m.settled = true
	m.kind = kind
}

func (m *itemMsg) verdict() settleKind {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.kind
}
