package durable

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

type BindRequest struct {
	JetStream jetstream.JetStream
	Stream    string
	Config    jetstream.ConsumerConfig
	Strict    bool
	Rejected  string
}

func Bind(
	ctx context.Context,
	req *BindRequest,
) (jetstream.Consumer, error) {
	if !req.Strict {
		return bindOnce(
			ctx,
			req,
		)
	}

	bound, err := bindRetrying(
		ctx,
		req,
	)
	if err != nil {
		return nil, err
	}
	if err := checkFilters(
		req,
		bound.CachedInfo().Config,
	); err != nil {
		return nil, err
	}

	return bound, nil
}
