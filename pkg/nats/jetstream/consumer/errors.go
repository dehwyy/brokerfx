package consumer

import (
	"errors"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/durable"
)

var (
	ErrInvalidOpts    = errors.New("consumer: invalid opts")
	ErrUpdateRejected = durable.ErrUpdateRejected
	ErrBindTransient  = durable.ErrBindTransient
	ErrFilterMismatch = durable.ErrFilterMismatch
	ErrInvalidFilters = durable.ErrInvalidFilters
)
