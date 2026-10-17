package durable

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

func bindOnce(
	ctx context.Context,
	req *BindRequest,
) (jetstream.Consumer, error) {
	name := Name(req.Config)
	if name == "" {
		return req.JetStream.CreateConsumer(
			ctx,
			req.Stream,
			req.Config,
		)
	}

	existing, err := req.JetStream.Consumer(
		ctx,
		req.Stream,
		name,
	)
	switch {
	case err == nil:
		return updateKeepingStart(
			ctx,
			req,
			existing,
		)
	case !errors.Is(
		err,
		jetstream.ErrConsumerNotFound,
	):
		return nil, err
	}

	created, err := req.JetStream.CreateConsumer(
		ctx,
		req.Stream,
		req.Config,
	)
	if !req.Strict && errors.Is(
		err,
		jetstream.ErrConsumerExists,
	) {
		return req.JetStream.Consumer(
			ctx,
			req.Stream,
			name,
		)
	}

	return created, err
}
