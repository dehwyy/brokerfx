package consumer

import (
	"context"
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
}

type Consumer struct {
	consumeCtx jetstream.ConsumeContext
}

func New(opts Opts) *Consumer {
	consumer, err := opts.JetStream.CreateOrUpdateConsumer(
		context.Background(),
		opts.Stream.Name(),
		opts.ConsumerOptsBuilder.Build(),
	)
	if err != nil {
		panic(err)
	}

	consumeCtx, err := consumer.Consume(
		func(msg jetstream.Msg) {
			go func() {
				if r := recover(); r != nil {
					log.Error().Msgf("panic: %v", r)
				}

				ctx := context.Background()
				var err error
				for _, middleware := range opts.BeforeHandlerMiddleware {
					ctx, err = middleware(ctx, msg)
					if err != nil {
						log.Error().Err(err).Msg("error in before handler middleware")
						return
					}
				}

				msg.Ack()
				if err = opts.HandlerFunc(ctx, msg); err != nil {
					log.Error().Err(err).Msg("error handling message")
				}

				for _, middleware := range opts.AfterHandlerMiddleware {
					ctx, err = middleware(ctx, msg)
					if err != nil {
						log.Error().Err(err).Msg("error in after handler middleware")
						return
					}
				}
			}()
		},
		jetstream.PullMaxMessages(50),
		jetstream.PullHeartbeat(10*time.Second),
	)

	if err != nil {
		log.Error().Err(err).Msg("failed to start jetstream consume")
		panic(err)
	}

	return &Consumer{
		consumeCtx: consumeCtx,
	}
}
