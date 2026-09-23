package replywait

import "errors"

var (
	ErrInvalidConfig        = errors.New("replywait: invalid config")
	ErrInvalidInstance      = errors.New("replywait: invalid instance")
	ErrDuplicateCorrelation = errors.New("replywait: duplicate correlation id")
	ErrTimeout              = errors.New("replywait: timeout waiting for reply")
	ErrDrained              = errors.New("replywait: drained")
)
