package batch

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const defaultAckWait = 30 * time.Second

func resolveMinBatch(
	opts Opts,
	maxSize int,
) (int, time.Duration, error) {
	minBatch, err := resolveInt(
		opts.MinBatch,
		envMinBatch,
		0,
	)
	if err != nil {
		return 0, 0, err
	}

	lingerMax, err := resolveDuration(
		opts.LingerMax,
		envLingerMax,
		0,
	)
	if err != nil {
		return 0, 0, err
	}

	if minBatch < 0 || lingerMax < 0 {
		return 0, 0, fmt.Errorf(
			"%w: MinBatch %d and LingerMax %s must not be negative",
			ErrInvalidOpts,
			minBatch,
			lingerMax,
		)
	}
	if minBatch > maxSize {
		return 0, 0, fmt.Errorf(
			"%w: MinBatch %d above Max %d",
			ErrInvalidOpts,
			minBatch,
			maxSize,
		)
	}
	if minBatch > 0 && lingerMax == 0 {
		log.Warn().
			Int(
				"min_batch",
				minBatch,
			).
			Msg("batch MinBatch is set without LingerMax, MinBatch off")
		return 0, 0, nil
	}
	if minBatch == 0 {
		return 0, 0, nil
	}

	return minBatch, lingerMax, nil
}
