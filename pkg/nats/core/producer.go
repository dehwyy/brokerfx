package core

import (
	"context"

	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
)

// CoreProducer publishes events over plain NATS (no JetStream).
// Delivery is fire-and-forget: there is no server acknowledgement,
// so callers must tolerate at-most-once delivery semantics.
type CoreProducer struct {
	conn *nats.Conn
}

type Opts struct {
	fx.In

	Conn *nats.Conn
}

func New(opts Opts) *CoreProducer {
	return &CoreProducer{
		conn: opts.Conn,
	}
}

var _ outbox.Producer = (*CoreProducer)(nil)

func (p *CoreProducer) Produce(_ context.Context, event outbox.ProducerEvent) error {
	msg := &nats.Msg{
		Subject: event.Subject,
		Data:    event.Payload,
		Header:  nats.Header{},
	}

	for k, v := range event.Headers {
		msg.Header.Set(k, v)
	}

	return p.conn.PublishMsg(msg)
}
