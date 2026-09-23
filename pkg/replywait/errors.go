package replywait

import "errors"

var (
	ErrInvalidConfig   = errors.New("replywait: invalid config")
	ErrInvalidInstance = errors.New("replywait: invalid instance")
)
