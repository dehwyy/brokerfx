package timedactor

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

const kvStreamPrefix = "KV_"

func bucketMarkerTTL(
	ctx context.Context,
	js jetstream.JetStream,
	cfg Config,
) (time.Duration, error) {
	if cfg.MarkerTTL > 0 {
		return cfg.MarkerTTL, nil
	}

	stream, err := js.Stream(
		ctx,
		kvStreamPrefix+cfg.BucketName,
	)
	switch {
	case errors.Is(
		err,
		jetstream.ErrStreamNotFound,
	):
		return 0, nil
	case err != nil:
		return 0, err
	}

	current := stream.CachedInfo().Config.SubjectDeleteMarkerTTL
	if current > 0 {
		log.Info().
			Str(
				"bucket",
				cfg.BucketName,
			).
			Dur(
				"limit_marker_ttl",
				current,
			).
			Msg("timed-actor keeps the marker TTL of the existing bucket")
	}

	return current, nil
}
