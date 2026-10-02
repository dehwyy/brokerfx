package consumer

import (
	"context"
	"errors"
	"time"

	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/middleware"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

type Opts struct {
	JetStream           jetstream.JetStream
	ConsumerOptsBuilder *consumeroptsbuilder.ConsumerOptsBuilder
	Stream              *stream.Stream

	HandlerFunc             func(ctx context.Context, msg jetstream.Msg) error
	BeforeHandlerMiddleware []middleware.Middleware
	AfterHandlerMiddleware  []middleware.Middleware

	// OnFatal is called when the pull subscription stops for good (the consumer was deleted),
	// so the process can restart instead of idling without a consumer.
	OnFatal func(error)
}

func consumeErrHandler(opts Opts) jetstream.ConsumeErrHandlerFunc {
	return func(_ jetstream.ConsumeContext, err error) {
		fatal := errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound)

		log.Warn().Err(err).Bool("fatal", fatal).Msg("jetstream consume error")

		if fatal && opts.OnFatal != nil {
			opts.OnFatal(err)
		}
	}
}

type Consumer struct {
	consumeCtx jetstream.ConsumeContext
}

func New(opts Opts) (*Consumer, error) {
	cfg := opts.ConsumerOptsBuilder.Build()
	consumer, err := opts.JetStream.CreateOrUpdateConsumer(
		context.Background(),
		opts.Stream.Name(),
		cfg,
	)
	if err != nil {
		name := cfg.Name
		if name == "" {
			name = cfg.Durable
		}
		existing, getErr := opts.JetStream.Consumer(
			context.Background(),
			opts.Stream.Name(),
			name,
		)
		if getErr != nil {
			return nil, err
		}
		consumer = existing
	}

	consumeCtx, err := consumer.Consume(
		func(msg jetstream.Msg) {
			go process(msg, opts)
		},
		jetstream.PullMaxMessages(50),
		jetstream.PullHeartbeat(10*time.Second),
		jetstream.ConsumeErrHandler(consumeErrHandler(opts)),
	)

	if err != nil {
		log.Error().Err(err).Msg("failed to start jetstream consume")
		panic(err)
	}

	return &Consumer{
		consumeCtx: consumeCtx,
	}, nil
}
