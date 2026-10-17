package frame

import "context"

func (e *elementMsg) Nak() error {
	return e.frame.resolve(
		context.Background(),
		e,
		outcome{
			nak: true,
		},
		false,
	)
}
