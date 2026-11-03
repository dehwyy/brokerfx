package ordered

import (
	"context"
	"errors"
	"sync/atomic"

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

	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}

	caughtUp := &atomic.Bool{}
	caughtUp.Store(info.NumPending == 0)

	if opts.OnAttached != nil {
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
				caughtUp,
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
