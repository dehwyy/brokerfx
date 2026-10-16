package producer

import "context"

func (p *Producer) ProduceSync(
	ctx context.Context,
	event Event,
	opts ...PublishOption,
) error {
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
