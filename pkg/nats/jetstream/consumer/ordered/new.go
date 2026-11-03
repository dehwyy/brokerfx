package ordered

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func New(ctx context.Context, opts Opts) (jetstream.ConsumeContext, error) {
	consumer, err := opts.JetStream.OrderedConsumer(
		ctx,
		opts.Stream,
		jetstream.OrderedConsumerConfig{
			FilterSubjects: opts.FilterSubjects,
			DeliverPolicy:  opts.DeliverPolicy,
		},
	)
	if err != nil {
		return nil, err
	}

	return consumer.Consume(
		func(msg jetstream.Msg) {
			process(
				ctx,
				msg,
				opts,
			)
		},
		jetstream.ConsumeErrHandler(
			func(_ jetstream.ConsumeContext, consumeErr error) {
				log.Warn().
					Err(consumeErr).
					Str("stream", opts.Stream).
					Msg("ordered consumer error")

				if opts.OnFatal != nil && fatal(consumeErr) {
					opts.OnFatal(consumeErr)
				}
			},
		),
	)
}

func fatal(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerDeleted) ||
		errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, jetstream.ErrStreamNotFound)
}
