package batch

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

func ResumeSequence(
	ctx context.Context,
	js jetstream.JetStream,
	stream string,
	durable string,
) (uint64, error) {
	previous, err := js.Consumer(
		ctx,
		stream,
		durable,
	)
	if errors.Is(
		err,
		jetstream.ErrConsumerNotFound,
	) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	info, err := previous.Info(ctx)
	if err != nil {
		return 0, err
	}

	return info.AckFloor.Stream + 1, nil
}
