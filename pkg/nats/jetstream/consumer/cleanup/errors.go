package cleanup

import "errors"

var (
	ErrInvalidOpts = errors.New("cleanup: invalid options")
	ErrInvalidEnv  = errors.New("cleanup: invalid environment")
)
