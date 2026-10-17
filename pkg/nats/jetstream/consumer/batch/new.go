package batch

import (
	"context"
	"fmt"

	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/durable"
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

	bound, err := durable.Bind(
		context.Background(),
		&durable.BindRequest{
			JetStream: opts.JetStream,
			Stream:    opts.Stream.Name(),
			Config:    cfg,
			Strict:    durable.Strict(opts.StrictUpdate),
			Rejected:  "batch consumer config update rejected, using existing durable as is",
		},
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
			durable.Name(actual),
		).
		Strs(
			"filters",
			durable.Filters(actual),
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
