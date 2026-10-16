package consumer

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	envMaxInFlight = "BROKERFX_CONSUMER_MAX_IN_FLIGHT"
	envNakBackoff  = "BROKERFX_CONSUMER_NAK_BACKOFF"
)

func resolveOpts(opts Opts) (Opts, error) {
	if opts.MaxInFlight < 0 {
		return opts, fmt.Errorf(
			"%w: MaxInFlight %d, want >= 0",
			ErrInvalidOpts,
			opts.MaxInFlight,
		)
	}
	for i, delay := range opts.NakBackoff {
		if delay <= 0 {
			return opts, fmt.Errorf(
				"%w: NakBackoff[%d] = %s, want > 0",
				ErrInvalidOpts,
				i,
				delay,
			)
		}
	}

	if opts.MaxInFlight == 0 {
		opts.MaxInFlight = maxInFlightFromEnv()
	}
	if len(opts.NakBackoff) == 0 {
		opts.NakBackoff = nakBackoffFromEnv()
	}

	return opts, nil
}

func maxInFlightFromEnv() int {
	raw := strings.TrimSpace(os.Getenv(envMaxInFlight))
	if raw == "" {
		return 0
	}

	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		log.Warn().
			Str(
				"env",
				envMaxInFlight,
			).
			Str(
				"value",
				raw,
			).
			Msg("invalid consumer max in flight, keeping unbounded dispatch")
		return 0
	}

	return limit
}

func nakBackoffFromEnv() []time.Duration {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(envNakBackoff)))
	switch raw {
	case "", "0", "false", "off", "no":
		return nil
	case "1", "true", "on", "yes":
		return DefaultNakDelays
	}

	parts := strings.Split(
		raw,
		",",
	)
	delays := make(
		[]time.Duration,
		0,
		len(parts),
	)
	for _, part := range parts {
		delay, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || delay <= 0 {
			log.Warn().
				Str(
					"env",
					envNakBackoff,
				).
				Str(
					"value",
					raw,
				).
				Msg("invalid consumer nak backoff, keeping immediate nak")
			return nil
		}
		delays = append(
			delays,
			delay,
		)
	}

	return delays
}
