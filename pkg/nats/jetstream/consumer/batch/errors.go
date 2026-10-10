package batch

import "errors"

var (
	ErrInvalidOpts   = errors.New("batch: invalid options")
	ErrInvalidEnv    = errors.New("batch: invalid environment")
	ErrMaxAckPending = errors.New("batch: consumer max ack pending is below pipeline*max")
	ErrMaxDeliver    = errors.New("batch: consumer max deliver must exceed max attempts")
	ErrMissingResult = errors.New("batch: handler returned no result for message")
	ErrHandlerPanic  = errors.New("batch: handler panicked")
	ErrPoison        = errors.New("batch: poison message")
)
