package frame

import "context"

func (e *elementMsg) Term() error {
	return e.frame.resolve(
		context.Background(),
		e,
		outcome{},
		false,
	)
}
