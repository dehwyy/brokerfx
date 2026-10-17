package consumer

import (
	"context"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/durable"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

const msgUpdateRejected = "consumer config update rejected, using existing durable as is"

func bindDurable(
	opts Opts,
	cfg jetstream.ConsumerConfig,
) (jetstream.Consumer, error) {
	if durable.Strict(opts.StrictUpdate) {
		return durable.Bind(
			context.Background(),
			&durable.BindRequest{
				JetStream: opts.JetStream,
				Stream:    opts.Stream.Name(),
				Config:    cfg,
				Strict:    true,
				Rejected:  msgUpdateRejected,
			},
		)
	}

	bound, err := opts.JetStream.CreateOrUpdateConsumer(
		context.Background(),
		opts.Stream.Name(),
		cfg,
	)
	if err == nil {
		return bound, nil
	}

	existing, getErr := opts.JetStream.Consumer(
		context.Background(),
		opts.Stream.Name(),
		durable.Name(cfg),
	)
	if getErr != nil {
		return nil, err
	}

	log.Warn().
		Err(err).
		Str(
			"stream",
			opts.Stream.Name(),
		).
		Str(
			"consumer",
			durable.Name(cfg),
		).
		Msg(msgUpdateRejected)

	return existing, nil
}
