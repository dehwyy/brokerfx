package producer

import (
	"context"

	cryptov1 "github.com/dehwyy/brokerfx/pkg/crypto/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

type Opts struct {
	fx.In

	JetStream jetstream.JetStream
}

type Producer struct {
	jetstream.JetStream
}

func New(opts Opts) *Producer {
	return &Producer{
		JetStream: opts.JetStream,
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

	ack, err := p.PublishAsync(
		event.Subject(),
		data,
	)
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
