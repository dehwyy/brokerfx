package cleanup

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	envRetention      = "BROKERFX_PROCESSED_RETENTION"
	envInterval       = "BROKERFX_PROCESSED_CLEANUP_INTERVAL"
	defaultRetention  = 48 * time.Hour
	defaultInterval   = time.Hour
	defaultBatchLimit = 5000
	minRetention      = time.Hour
	batchPause        = 100 * time.Millisecond
)

type settings struct {
	retention  time.Duration
	interval   time.Duration
	batchLimit int
}

func resolve(opts Opts) (settings, error) {
	if opts.DB == nil {
		return settings{}, fmt.Errorf(
			"%w: DB is nil",
			ErrInvalidOpts,
		)
	}

	retention, err := resolveDuration(
		opts.Retention,
		envRetention,
		defaultRetention,
	)
	if err != nil {
		return settings{}, err
	}
	if retention < minRetention {
		return settings{}, fmt.Errorf(
			"%w: Retention %s below %s",
			ErrInvalidOpts,
			retention,
			minRetention,
		)
	}
	if opts.Duplicates > 0 && retention <= 2*opts.Duplicates {
		return settings{}, fmt.Errorf(
			"%w: Retention %s must exceed 2*Duplicates %s",
			ErrInvalidOpts,
			retention,
			opts.Duplicates,
		)
	}

	interval, err := resolveDuration(
		opts.Interval,
		envInterval,
		defaultInterval,
	)
	if err != nil {
		return settings{}, err
	}
	if interval <= 0 {
		return settings{}, fmt.Errorf(
			"%w: Interval %s, want > 0",
			ErrInvalidOpts,
			interval,
		)
	}

	batchLimit := opts.BatchLimit
	switch {
	case batchLimit == 0:
		batchLimit = defaultBatchLimit
	case batchLimit < 0:
		return settings{}, fmt.Errorf(
			"%w: BatchLimit %d, want > 0",
			ErrInvalidOpts,
			batchLimit,
		)
	}

	return settings{
		retention:  retention,
		interval:   interval,
		batchLimit: batchLimit,
	}, nil
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
