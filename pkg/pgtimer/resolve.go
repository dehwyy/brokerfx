package pgtimer

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	envLease          = "BROKERFX_PGTIMER_LEASE"
	envHandlerTimeout = "BROKERFX_PGTIMER_HANDLER_TIMEOUT"
	envPoll           = "BROKERFX_PGTIMER_POLL"
	envBatch          = "BROKERFX_PGTIMER_BATCH"
	envConcurrency    = "BROKERFX_PGTIMER_CONCURRENCY"

	defaultLease          = 60 * time.Second
	defaultHandlerTimeout = 30 * time.Second
	defaultPoll           = time.Second
	defaultBatch          = 100
	defaultConcurrency    = 8

	limitBatch       = 1000
	limitConcurrency = 256

	maxRemoveTimeout = 5 * time.Second
)

type settings struct {
	lease          time.Duration
	handlerTimeout time.Duration
	poll           time.Duration
	batch          int
	concurrency    int
}

func resolve(cfg Config) (settings, error) {
	lease, err := resolveDuration(
		cfg.Lease,
		envLease,
		defaultLease,
	)
	if err != nil {
		return settings{}, err
	}

	handlerTimeout, err := resolveDuration(
		cfg.HandlerTimeout,
		envHandlerTimeout,
		defaultHandlerTimeout,
	)
	if err != nil {
		return settings{}, err
	}

	poll, err := resolveDuration(
		cfg.Poll,
		envPoll,
		defaultPoll,
	)
	if err != nil {
		return settings{}, err
	}

	batch, err := resolveInt(
		cfg.Batch,
		envBatch,
		defaultBatch,
	)
	if err != nil {
		return settings{}, err
	}

	concurrency, err := resolveInt(
		cfg.Concurrency,
		envConcurrency,
		defaultConcurrency,
	)
	if err != nil {
		return settings{}, err
	}

	s := settings{
		lease:          lease,
		handlerTimeout: handlerTimeout,
		poll:           poll,
		batch:          batch,
		concurrency:    concurrency,
	}

	return s, s.validate()
}

func (s settings) validate() error {
	switch {
	case s.lease <= 0 || s.handlerTimeout <= 0 || s.poll <= 0:
		return fmt.Errorf(
			"%w: Lease %s, HandlerTimeout %s and Poll %s must be positive",
			ErrInvalidConfig,
			s.lease,
			s.handlerTimeout,
			s.poll,
		)
	case s.handlerTimeout > s.lease/2:
		return fmt.Errorf(
			"%w: HandlerTimeout %s, want at most Lease/2 %s: the delete after the handler takes up to %s more and must end before the lease",
			ErrInvalidConfig,
			s.handlerTimeout,
			s.lease/2,
			s.removeTimeout(),
		)
	case s.batch < 1 || s.batch > limitBatch:
		return fmt.Errorf(
			"%w: Batch %d out of range 1..%d",
			ErrInvalidConfig,
			s.batch,
			limitBatch,
		)
	case s.concurrency < 1 || s.concurrency > limitConcurrency:
		return fmt.Errorf(
			"%w: Concurrency %d out of range 1..%d",
			ErrInvalidConfig,
			s.concurrency,
			limitConcurrency,
		)
	}

	return nil
}

func (s settings) removeTimeout() time.Duration {
	return min(
		maxRemoveTimeout,
		s.lease/4,
	)
}

func resolveDuration(
	explicit time.Duration,
	env string,
	fallback time.Duration,
) (time.Duration, error) {
	if explicit != 0 {
		return explicit, nil
	}

	raw := os.Getenv(env)
	if raw == "" {
		return fallback, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %s=%q: %w",
			ErrInvalidConfig,
			env,
			raw,
			err,
		)
	}

	return value, nil
}

func resolveInt(
	explicit int,
	env string,
	fallback int,
) (int, error) {
	if explicit != 0 {
		return explicit, nil
	}

	raw := os.Getenv(env)
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %s=%q: %w",
			ErrInvalidConfig,
			env,
			raw,
			err,
		)
	}

	return value, nil
}
