package consumer

import (
	"context"
	"errors"
	"time"

	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/durable"
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

	MaxInFlight  int
	NakBackoff   []time.Duration
	StrictUpdate bool

	inProgressEvery time.Duration
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
	inFlight   *pool
}

func New(opts Opts) (*Consumer, error) {
	opts, err := resolveOpts(opts)
	if err != nil {
		return nil, err
	}

	cfg := consumeroptsbuilder.FitReplicas(
		opts.ConsumerOptsBuilder.Build(),
		opts.Stream.Config(),
	)
	consumer, err := bindDurable(
		opts,
		cfg,
	)
	if err != nil {
		return nil, err
	}

	inFlight := newPool(opts.MaxInFlight)
	if inFlight != nil {
		opts.inProgressEvery = heartbeatForAckWait(ackWaitOf(consumer))
		log.Info().
			Str(
				"stream",
				opts.Stream.Name(),
			).
			Str(
				"consumer",
				cfg.Name,
			).
			Int(
				"max_in_flight",
				opts.MaxInFlight,
			).
			Dur(
				"heartbeat",
				opts.inProgressEvery,
			).
			Bool(
				"nak_backoff",
				len(opts.NakBackoff) > 0,
			).
			Msg("jetstream consumer dispatch bounded")
	}

	consumeCtx, err := consumer.Consume(
		dispatch(
			opts,
			inFlight,
		),
		jetstream.PullMaxMessages(pullMaxMessages(opts.MaxInFlight)),
		jetstream.PullHeartbeat(10*time.Second),
		jetstream.ConsumeErrHandler(consumeErrHandler(opts)),
	)

	if err != nil {
		log.Error().Err(err).Msg("failed to start jetstream consume")
		panic(err)
	}

	log.Info().
		Str(
			"stream",
			opts.Stream.Name(),
		).
		Str(
			"consumer",
			durable.Name(cfg),
		).
		Strs(
			"filters",
			durable.Filters(consumer.CachedInfo().Config),
		).
		Msg("jetstream consumer started")

	return &Consumer{
		consumeCtx: consumeCtx,
		inFlight:   inFlight,
	}, nil
}
