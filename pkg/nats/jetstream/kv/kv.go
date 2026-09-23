package kv

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrReplicasRequired = errors.New("kv: replicas must be at least 1")

const kvStreamNamePrefix = "KV_"

const DefaultDuplicates = 15 * time.Minute

type Opts struct {
	Bucket     string
	Replicas   int
	History    uint8
	Storage    jetstream.StorageType
	MaxBytes   int64
	Duplicates time.Duration
}

func Ensure(ctx context.Context, js jetstream.JetStream, opts Opts) (jetstream.KeyValue, error) {
	if opts.Replicas < 1 {
		return nil, ErrReplicasRequired
	}

	duplicates := opts.Duplicates
	if duplicates <= 0 {
		duplicates = DefaultDuplicates
	}

	history := opts.History
	if history == 0 {
		history = 1
	}

	maxBytes := opts.MaxBytes
	if maxBytes == 0 {
		maxBytes = -1
	}

	streamName := kvStreamNamePrefix + opts.Bucket

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, err
		}

		if _, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:   opts.Bucket,
			Replicas: opts.Replicas,
			History:  opts.History,
			Storage:  opts.Storage,
			MaxBytes: opts.MaxBytes,
		}); err != nil {
			return nil, err
		}

		stream, err = js.Stream(ctx, streamName)
		if err != nil {
			return nil, err
		}

		info, err := stream.Info(ctx)
		if err != nil {
			return nil, err
		}

		cfg := info.Config
		cfg.Duplicates = duplicates
		if _, err := js.UpdateStream(ctx, cfg); err != nil {
			return nil, err
		}

		return js.KeyValue(ctx, opts.Bucket)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	cfg := info.Config
	cfg.MaxMsgsPerSubject = int64(history)
	cfg.Replicas = opts.Replicas
	cfg.MaxBytes = maxBytes
	cfg.Storage = opts.Storage
	cfg.Duplicates = duplicates

	if cfg.MaxMsgsPerSubject != info.Config.MaxMsgsPerSubject ||
		cfg.Replicas != info.Config.Replicas ||
		cfg.MaxBytes != info.Config.MaxBytes ||
		cfg.Storage != info.Config.Storage ||
		cfg.Duplicates != info.Config.Duplicates {
		if _, err := js.UpdateStream(ctx, cfg); err != nil {
			return nil, err
		}
	}

	return js.KeyValue(ctx, opts.Bucket)
}
