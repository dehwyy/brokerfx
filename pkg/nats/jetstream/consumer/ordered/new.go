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

	if opts.OnAttached != nil {
		info, infoErr := consumer.Info(ctx)
		if infoErr != nil {
			return nil, infoErr
		}

		opts.OnAttached(
			ctx,
			info,
		)
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
				if fatal(consumeErr) {
					log.Error().
						Err(consumeErr).
						Str(
							"stream",
							opts.Stream,
						).
						Msg("ordered consumer fatal error")

					if opts.OnFatal != nil {
						opts.OnFatal(consumeErr)
					}

					return
				}

				log.Warn().
					Err(consumeErr).
					Str(
						"stream",
						opts.Stream,
					).
					Msg("ordered consumer error, recovering")
			},
		),
	)
}

func fatal(err error) bool {
	return errors.Is(
		err,
		jetstream.ErrStreamNotFound,
	)
}
