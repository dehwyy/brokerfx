package durable

import (
	"context"
	"errors"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func isTransient(err error) bool {
	switch {
	case errors.Is(
		err,
		context.DeadlineExceeded,
	),
		errors.Is(
			err,
			nats.ErrTimeout,
		),
		errors.Is(
			err,
			nats.ErrNoResponders,
		),
		errors.Is(
			err,
			nats.ErrConnectionReconnecting,
		),
		errors.Is(
			err,
			nats.ErrReconnectBufExceeded,
		),
		errors.Is(
			err,
			nats.ErrDisconnected,
		),
		errors.Is(
			err,
			jetstream.ErrConsumerExists,
		),
		errors.Is(
			err,
			jetstream.ErrConsumerDoesNotExist,
		):
		return true
	}

	var apiErr *jetstream.APIError
	if errors.As(
		err,
		&apiErr,
	) {
		return apiErr.Code == http.StatusServiceUnavailable
	}

	return false
}
