package frame

import "context"

func (e *elementMsg) Ack() error {
	return e.frame.resolve(
		context.Background(),
		e,
		outcome{},
		false,
	)
}
