package batch

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	envMax          = "BROKERFX_BATCH_MAX"
	envLinger       = "BROKERFX_BATCH_LINGER"
	envPipeline     = "BROKERFX_BATCH_PIPELINE"
	defaultMax      = 16
	defaultLinger   = 5 * time.Millisecond
	defaultPipeline = 1
	limitMax        = 256
	limitLinger     = time.Second
	limitPipeline   = 8
)

type settings struct {
	max         int
	linger      time.Duration
	pipeline    int
	delays      []time.Duration
	maxAttempts int
}

func resolve(opts Opts) (settings, error) {
	if opts.Handler == nil {
		return settings{}, fmt.Errorf(
			"%w: Handler is nil",
			ErrInvalidOpts,
		)
	}

	maxSize, err := resolveInt(
		opts.Max,
		envMax,
		defaultMax,
	)
	if err != nil {
		return settings{}, err
	}
	if maxSize < 1 || maxSize > limitMax {
		return settings{}, fmt.Errorf(
			"%w: Max %d out of range 1..%d",
			ErrInvalidOpts,
			maxSize,
			limitMax,
		)
	}

	linger, err := resolveDuration(
		opts.Linger,
		envLinger,
		defaultLinger,
	)
	if err != nil {
		return settings{}, err
	}
	if linger <= 0 || linger > limitLinger {
		return settings{}, fmt.Errorf(
			"%w: Linger %s out of range (0, %s]",
			ErrInvalidOpts,
			linger,
			limitLinger,
		)
	}

	pipeline, err := resolveInt(
		opts.Pipeline,
		envPipeline,
		defaultPipeline,
	)
	if err != nil {
		return settings{}, err
	}
	if pipeline < 1 || pipeline > limitPipeline {
		return settings{}, fmt.Errorf(
			"%w: Pipeline %d out of range 1..%d",
			ErrInvalidOpts,
			pipeline,
			limitPipeline,
		)
	}

	delays, err := resolveDelays(opts.Delays)
	if err != nil {
		return settings{}, err
	}

	maxAttempts := opts.MaxAttempts
	switch {
	case maxAttempts == 0:
		maxAttempts = len(delays) + 1
	case maxAttempts < -1:
		return settings{}, fmt.Errorf(
			"%w: MaxAttempts %d, want -1 or >= 1",
			ErrInvalidOpts,
			maxAttempts,
		)
	}

	return settings{
		max:         maxSize,
		linger:      linger,
		pipeline:    pipeline,
		delays:      delays,
		maxAttempts: maxAttempts,
	}, nil
}

func resolveDelays(delays []time.Duration) ([]time.Duration, error) {
	if len(delays) == 0 {
		return DefaultDelays, nil
	}

	for i, delay := range delays {
		if delay <= 0 {
			return nil, fmt.Errorf(
				"%w: Delays[%d] = %s, want > 0",
				ErrInvalidOpts,
				i,
				delay,
			)
		}
	}

	return delays, nil
}

func resolveInt(
	explicit int,
	env string,
	fallback int,
) (int, error) {
	if explicit != 0 {
		return explicit, nil
	}

	raw := strings.TrimSpace(os.Getenv(env))
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %s=%q: %w",
			ErrInvalidEnv,
			env,
			raw,
			err,
		)
	}

	return value, nil
}

func resolveDuration(
	explicit time.Duration,
	env string,
	fallback time.Duration,
) (time.Duration, error) {
	if explicit != 0 {
		return explicit, nil
	}

	raw := strings.TrimSpace(os.Getenv(env))
	if raw == "" {
		return fallback, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %s=%q: %w",
			ErrInvalidEnv,
			env,
			raw,
			err,
		)
	}

	return value, nil
}
