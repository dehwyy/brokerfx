package replywait

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrInvalidStreamOpts = errors.New("replywait: invalid reply stream opts")

type ReplyStreamOpts struct {
	Name     string
	Subjects []string
	Replicas int
	MaxAge   time.Duration
}

func (o ReplyStreamOpts) validate() error {
	if o.Name == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidStreamOpts)
	}
	if len(o.Subjects) == 0 {
		return fmt.Errorf("%w: subjects is required", ErrInvalidStreamOpts)
	}
	if o.Replicas < 1 {
		return fmt.Errorf("%w: replicas must be at least 1", ErrInvalidStreamOpts)
	}
	if o.MaxAge <= 0 {
		return fmt.Errorf("%w: max age must be positive", ErrInvalidStreamOpts)
	}

	return nil
}

func EnsureReplyStream(ctx context.Context, js jetstream.JetStream, opts ReplyStreamOpts) (jetstream.Stream, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	stream, err := js.Stream(ctx, opts.Name)
	if err != nil {
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("replywait: lookup reply stream %s: %w", opts.Name, err)
		}

		created, err := js.CreateStream(ctx, streamConfigFromOpts(opts))
		if err != nil {
			return nil, fmt.Errorf("replywait: create reply stream %s: %w", opts.Name, err)
		}

		return created, nil
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("replywait: info reply stream %s: %w", opts.Name, err)
	}

	cfg := info.Config
	if replyStreamConfigMatches(cfg, opts) {
		return stream, nil
	}

	cfg.Subjects = opts.Subjects
	cfg.Replicas = opts.Replicas
	cfg.MaxAge = opts.MaxAge

	updated, err := js.UpdateStream(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("replywait: update reply stream %s: %w", opts.Name, err)
	}

	return updated, nil
}

func streamConfigFromOpts(opts ReplyStreamOpts) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      opts.Name,
		Subjects:  opts.Subjects,
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		Replicas:  opts.Replicas,
		MaxAge:    opts.MaxAge,
	}
}

func replyStreamConfigMatches(cfg jetstream.StreamConfig, opts ReplyStreamOpts) bool {
	if cfg.Replicas != opts.Replicas || cfg.MaxAge != opts.MaxAge {
		return false
	}

	return subjectsEqual(cfg.Subjects, opts.Subjects)
}

func subjectsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
