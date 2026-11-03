package producer

import (
	"context"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
)

func (p *Producer) ProduceSync(
	ctx context.Context,
	event Event,
	opts ...PublishOption,
) (err error) {
	ctx, span := tracenats.StartProducer(
		ctx,
		"nats.publish",
		event.Subject(),
	)
	defer func() {
		endPublishSpan(
			span,
			err,
		)
	}()

	msg, err := p.message(
		ctx,
		event,
		opts,
	)
	if err != nil {
		return err
	}

	_, err = p.js.PublishMsg(
		ctx,
		msg,
	)

	return err
}
