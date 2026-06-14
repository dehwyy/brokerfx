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

func New(opts Opts) (*Consumer, error) {
	cfg := opts.ConsumerOptsBuilder.Build()
	consumer, err := opts.JetStream.CreateOrUpdateConsumer(
		context.Background(),
		opts.Stream.Name(),
		cfg,
	)
	if err != nil {
		// On WorkQueue streams a consumer's filter must be unique, so an existing
		// durable cannot be re-created/updated with a changed config (server returns
		// "filtered consumer not unique"). Fall back to binding the existing consumer.
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
			go func() {
				log.Debug().Any("subject", msg.Subject()).Msg("nats message received")
				defer func() {
					if r := recover(); r != nil {
						log.Error().
							Str("subject", msg.Subject()).
							Any("panic", r).
							Msg("consumer handler panic — NAK for redelivery")
						if nakErr := msg.Nak(); nakErr != nil {
							log.Error().Err(nakErr).Msg("failed to NAK after panic")
						}
					}
				}()

				ctx := context.Background()
				var err error
				for _, middleware := range opts.BeforeHandlerMiddleware {
					ctx, err = middleware(ctx, msg)
					if err != nil {
						log.Error().Err(err).Msg("error in before handler middleware — NAK for redelivery")
						if nakErr := msg.Nak(); nakErr != nil {
							log.Error().Err(nakErr).Msg("failed to NAK after middleware error")
						}
						return
					}
				}

				// Handler is authoritative: Ack only on success, Nak on error so JetStream
				// redelivers the message. Handlers MUST be idempotent — Nats-Msg-Id dedup
				// on the producer side bounds the replay risk to the Duplicates window.
				if err = opts.HandlerFunc(ctx, msg); err != nil {
					log.Error().Err(err).Str("subject", msg.Subject()).Msg("handler failed — NAK for redelivery")
					if nakErr := msg.Nak(); nakErr != nil {
						log.Error().Err(nakErr).Msg("failed to NAK after handler error")
					}
					return
				}

				if ackErr := msg.Ack(); ackErr != nil {
					log.Error().Err(ackErr).Str("subject", msg.Subject()).Msg("failed to ACK after successful handler")
				}

				for _, middleware := range opts.AfterHandlerMiddleware {
					ctx, err = middleware(ctx, msg)
					if err != nil {
						log.Error().Err(err).Msg("error in after handler middleware (message already acked)")
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
	}, nil
}
