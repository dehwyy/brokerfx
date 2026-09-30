package stream

import (
	"context"
	"errors"

	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

type Opts struct {
	JetStream         jetstream.JetStream
	StreamOptsBuilder *streamoptsbuilder.StreamOptsBuilder
}

type Stream struct {
	stream jetstream.Stream
}

func New(opts Opts) (*Stream, error) {
	if err := opts.StreamOptsBuilder.Err(); err != nil {
		return nil, err
	}

	stream, err := CreateOrUpdate(
		context.Background(),
		opts.JetStream,
		opts.StreamOptsBuilder.Build(),
	)
	if err != nil {
		return nil, err
	}

	return &Stream{stream}, nil
}

func CreateOrUpdate(
	ctx context.Context,
	js jetstream.JetStream,
	cfg jetstream.StreamConfig,
) (jetstream.Stream, error) {
	existing, err := js.Stream(ctx, cfg.Name)
	switch {
	case err == nil:
		info, err := existing.Info(ctx)
		if err != nil {
			return nil, err
		}

		maxBytes, kept := guardMaxBytes(info, cfg.MaxBytes)
		if kept {
			log.Warn().
				Str("stream", cfg.Name).
				Uint64("current_state_bytes", info.State.Bytes).
				Int64("current_max_bytes", info.Config.MaxBytes).
				Int64("requested_max_bytes", cfg.MaxBytes).
				Msg("stream max_bytes decrease skipped: stored data exceeds half of requested limit")
		}
		cfg.MaxBytes = maxBytes
	case errors.Is(err, jetstream.ErrStreamNotFound):
	default:
		return nil, err
	}

	return js.CreateOrUpdateStream(ctx, cfg)
}

func guardMaxBytes(current *jetstream.StreamInfo, desired int64) (int64, bool) {
	if desired <= 0 {
		return desired, false
	}

	currentMax := current.Config.MaxBytes
	if currentMax > 0 && desired >= currentMax {
		return desired, false
	}

	if current.State.Bytes > uint64(desired)/2 {
		return currentMax, true
	}

	return desired, false
}

// Bind attaches to an already-existing JetStream stream by name without
// creating or updating it.
//
// Use this when the stream is owned by another service (or pre-provisioned by
// infra) and this process only needs to attach consumers to it. Unlike New,
// Bind never calls CreateOrUpdateStream, so it cannot trigger a `subjects
// overlap with an existing stream` (10065) error and cannot mutate the
// stream's config (retention, subjects, limits).
//
// Returns an error if no stream with the given name exists.
func Bind(
	js jetstream.JetStream,
	name string,
) (*Stream, error) {
	stream, err := js.Stream(
		context.Background(),
		name,
	)
	if err != nil {
		return nil, err
	}

	return &Stream{stream}, nil
}

func (s *Stream) Name() string {
	return s.stream.CachedInfo().Config.Name
}
