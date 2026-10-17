package frame

import "context"

func (e *elementMsg) TermWithReason(string) error {
	return e.frame.resolve(
		context.Background(),
		e,
		outcome{},
		false,
	)
}
