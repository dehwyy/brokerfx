package frame

import "errors"

var (
	ErrBadFrame       = errors.New("frame: malformed frame")
	ErrUnknownVersion = errors.New("frame: unknown frame version")
)
