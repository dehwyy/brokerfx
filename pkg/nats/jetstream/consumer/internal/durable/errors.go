package durable

import "errors"

var (
	ErrUpdateRejected = errors.New("consumer: durable config update rejected")
	ErrBindTransient  = errors.New("consumer: durable bind kept failing with transient errors")
	ErrFilterMismatch = errors.New("consumer: durable filters differ from the wanted ones")
	ErrInvalidFilters = errors.New("consumer: invalid filter subjects")
	ErrInvalidPinned  = errors.New("consumer: invalid pinned priority group")
)
