package producer

import (
	"context"

	cryptov1 "github.com/dehwyy/brokerfx/pkg/crypto/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// BeforePublishMiddleware is called before publishing a message.
// It can be used to inject trace context into NATS headers.
type BeforePublishMiddleware func(ctx context.Context, msg *nats.Msg)

type Opts struct {
	fx.In

	JetStream               jetstream.JetStream
	BeforePublishMiddleware []BeforePublishMiddleware `group:"before_publish_middleware"`
}

type Producer struct {
	js                      jetstream.JetStream
	beforePublishMiddleware []BeforePublishMiddleware
}

func New(opts Opts) *Producer {
	return &Producer{
		js:                      opts.JetStream,
		beforePublishMiddleware: opts.BeforePublishMiddleware,
	}
}

func (p *Producer) Produce(
	ctx context.Context,
	event Event,
) error {
	data, err := cryptov1.Encode(event.Data())
	if err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: event.Subject(),
		Data:    data,
		Header:  nats.Header{},
	}

	for _, mw := range p.beforePublishMiddleware {
		mw(ctx, msg)
	}

	ack, err := p.js.PublishMsgAsync(msg)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-ack.Ok():
			log.Trace().Msg("message published")
		case err := <-ack.Err():
			log.Error().Err(err).Msg("error publishing message")
		}
	}()

	return nil
}
