package durable

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

func rejected(
	req *BindRequest,
	err error,
) error {
	var apiErr *jetstream.APIError
	if errors.As(
		err,
		&apiErr,
	) {
		return fmt.Errorf(
			"%w: stream %s durable %s: code %d err_code %d: %s",
			ErrUpdateRejected,
			req.Stream,
			Name(req.Config),
			apiErr.Code,
			apiErr.ErrorCode,
			apiErr.Description,
		)
	}

	return fmt.Errorf(
		"%w: stream %s durable %s: %w",
		ErrUpdateRejected,
		req.Stream,
		Name(req.Config),
		err,
	)
}
