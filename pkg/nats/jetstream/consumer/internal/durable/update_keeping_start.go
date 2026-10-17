package durable

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func updateKeepingStart(
	ctx context.Context,
	req *BindRequest,
	existing jetstream.Consumer,
) (jetstream.Consumer, error) {
	current := existing.CachedInfo().Config
	cfg := req.Config
	cfg.DeliverPolicy = current.DeliverPolicy
	cfg.OptStartSeq = current.OptStartSeq
	cfg.OptStartTime = current.OptStartTime

	updated, err := req.JetStream.UpdateConsumer(
		ctx,
		req.Stream,
		cfg,
	)
	switch {
	case err == nil:
		return updated, nil
	case req.Strict && isTransient(err):
		return nil, err
	case req.Strict:
		return nil, rejected(
			req,
			err,
		)
	}

	log.Warn().
		Err(err).
		Str(
			"stream",
			req.Stream,
		).
		Str(
			"consumer",
			Name(cfg),
		).
		Msg(req.Rejected)

	return existing, nil
}
