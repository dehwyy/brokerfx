package producer

import (
	"context"

	cryptov1 "github.com/dehwyy/brokerfx/pkg/crypto/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func (p *Producer) message(
	ctx context.Context,
	event Event,
	opts []PublishOption,
) (*nats.Msg, error) {
	cfg := &publishConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	data, err := cryptov1.Encode(event.Data())
	if err != nil {
		return nil, err
	}

	msg := &nats.Msg{
		Subject: event.Subject(),
		Data:    data,
		Header:  nats.Header{},
	}

	if cfg.msgID != "" {
		msg.Header.Set(
			jetstream.MsgIDHeader,
			cfg.msgID,
		)
	}

	for _, mw := range p.beforePublishMiddleware {
		mw(
			ctx,
			msg,
		)
	}

	return msg, nil
}
