package pgtimer

import "errors"

var (
	ErrInvalidConfig    = errors.New("pgtimer: invalid config")
	ErrStarted          = errors.New("pgtimer: poller already started")
	ErrNoHandlers       = errors.New("pgtimer: no handlers registered")
	ErrDuplicateHandler = errors.New("pgtimer: handler already registered")
	ErrHandlerPanic     = errors.New("pgtimer: handler panicked")
)
