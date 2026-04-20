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

// PublishOption tunes a single Produce call. Use WithMsgID to enable JetStream
// server-side deduplication within the stream's Duplicates window.
type PublishOption func(*publishConfig)

type publishConfig struct {
	msgID string
}

// WithMsgID sets the Nats-Msg-Id header so the JetStream server suppresses duplicate
// publishes of the same id within the stream's configured Duplicates window.
func WithMsgID(id string) PublishOption {
	return func(c *publishConfig) {
		c.msgID = id
	}
}

func (p *Producer) Produce(
	ctx context.Context,
	event Event,
	opts ...PublishOption,
) error {
	cfg := &publishConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	data, err := cryptov1.Encode(event.Data())
	if err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: event.Subject(),
		Data:    data,
		Header:  nats.Header{},
	}

	if cfg.msgID != "" {
		msg.Header.Set(jetstream.MsgIDHeader, cfg.msgID)
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
