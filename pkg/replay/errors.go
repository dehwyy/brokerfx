package replay

import "errors"

var (
	ErrInvalidRequest   = errors.New("replay: invalid request")
	ErrInvalidItem      = errors.New("replay: invalid item")
	ErrNotReplay        = errors.New("replay: message is not a replay message")
	ErrStreamUnsuitable = errors.New("replay: stream unsuitable for replay")
	ErrPayloadTooLarge  = errors.New("replay: payload too large")
)
