package frame

import (
	"context"
	"time"
)

func (e *elementMsg) NakWithDelay(delay time.Duration) error {
	return e.frame.resolve(
		context.Background(),
		e,
		outcome{
			nak:   true,
			delay: delay,
		},
		false,
	)
}
