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

	store, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   opts.Bucket,
		Replicas: opts.Replicas,
		History:  opts.History,
		Storage:  opts.Storage,
		MaxBytes: opts.MaxBytes,
	})
	if err != nil {
		return nil, err
	}

	stream, err := js.Stream(ctx, kvStreamNamePrefix+opts.Bucket)
	if err != nil {
		return nil, err
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	if info.Config.Duplicates != duplicates {
		cfg := info.Config
		cfg.Duplicates = duplicates
		if _, err := js.UpdateStream(ctx, cfg); err != nil {
			return nil, err
		}
	}

	return store, nil
}
