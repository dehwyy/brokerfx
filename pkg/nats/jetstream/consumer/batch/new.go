package batch

import (
	"context"
	"errors"
	"fmt"

	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func New(opts Opts) (*Consumer, error) {
	if opts.JetStream == nil || opts.Stream == nil || opts.ConsumerOptsBuilder == nil {
		return nil, fmt.Errorf(
			"%w: JetStream, Stream and ConsumerOptsBuilder are required",
			ErrInvalidOpts,
		)
	}

	s, err := resolve(opts)
	if err != nil {
		return nil, err
	}

	cfg, err := validate(
		consumeroptsbuilder.FitReplicas(
			opts.ConsumerOptsBuilder.Build(),
			opts.Stream.Config(),
		),
		s,
	)
	if err != nil {
		return nil, err
	}

	bound, err := bindConsumer(
		context.Background(),
		opts.JetStream,
		opts.Stream.Name(),
		cfg,
	)
	if err != nil {
		return nil, err
	}

	actual := bound.CachedInfo().Config
	if _, err := validate(
		actual,
		s.perPod(),
	); err != nil {
		return nil, err
	}

	c := &Consumer{
		handler:        opts.Handler,
		observer:       observerOrNoop(opts.Observer),
		onPoison:       opts.OnPoison,
		onFatal:        opts.OnFatal,
		delays:         s.delays,
		maxAttempts:    s.maxAttempts,
		maxDeliver:     actual.MaxDeliver,
		heartbeatEvery: actual.AckWait / 2,
		max:            s.max,
		linger:         s.linger,
		minBatch:       s.minBatch,
		lingerMax:      s.lingerMax,
		pipeline:       s.pipeline,
		fetcher:        bound,
	}
	c.start()

	log.Info().
		Str(
			"stream",
			opts.Stream.Name(),
		).
		Str(
			"consumer",
			consumerName(actual),
		).
		Int(
			"max",
			s.max,
		).
		Dur(
			"linger",
			s.linger,
		).
		Int(
			"pipeline",
			s.pipeline,
		).
		Int(
			"min_batch",
			s.minBatch,
		).
		Dur(
			"linger_max",
			s.lingerMax,
		).
		Int(
			"max_attempts",
			s.maxAttempts,
		).
		Int(
			"replicas",
			s.replicas,
		).
		Int(
			"max_ack_pending",
			actual.MaxAckPending,
		).
		Int(
			"max_ack_pending_wanted",
			cfg.MaxAckPending,
		).
		Int(
			"max_deliver",
			actual.MaxDeliver,
		).
		Msg("batch consumer started")

	return c, nil
}

func bindConsumer(
	ctx context.Context,
	js jetstream.JetStream,
	stream string,
	cfg jetstream.ConsumerConfig,
) (jetstream.Consumer, error) {
	name := consumerName(cfg)
	if name == "" {
		return js.CreateConsumer(
			ctx,
			stream,
			cfg,
		)
	}

	existing, err := js.Consumer(
		ctx,
		stream,
		name,
	)
	switch {
	case err == nil:
		return updateKeepingStart(
			ctx,
			js,
			stream,
			cfg,
			existing,
		), nil
	case !errors.Is(
		err,
		jetstream.ErrConsumerNotFound,
	):
		return nil, err
	}

	created, err := js.CreateConsumer(
		ctx,
		stream,
		cfg,
	)
	if errors.Is(
		err,
		jetstream.ErrConsumerExists,
	) {
		return js.Consumer(
			ctx,
			stream,
			name,
		)
	}

	return created, err
}

func updateKeepingStart(
	ctx context.Context,
	js jetstream.JetStream,
	stream string,
	cfg jetstream.ConsumerConfig,
	existing jetstream.Consumer,
) jetstream.Consumer {
	current := existing.CachedInfo().Config
	cfg.DeliverPolicy = current.DeliverPolicy
	cfg.OptStartSeq = current.OptStartSeq
	cfg.OptStartTime = current.OptStartTime

	updated, err := js.UpdateConsumer(
		ctx,
		stream,
		cfg,
	)
	if err != nil {
		log.Warn().
			Err(err).
			Str(
				"stream",
				stream,
			).
			Str(
				"consumer",
				consumerName(cfg),
			).
			Msg("batch consumer config update rejected, using existing durable as is")
		return existing
	}

	return updated
}

func consumerName(cfg jetstream.ConsumerConfig) string {
	if cfg.Durable != "" {
		return cfg.Durable
	}

	return cfg.Name
}
