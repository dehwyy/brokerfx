package frame

import "context"

func (e *elementMsg) DoubleAck(ctx context.Context) error {
	return e.frame.resolve(
		ctx,
		e,
		outcome{},
		true,
	)
}
