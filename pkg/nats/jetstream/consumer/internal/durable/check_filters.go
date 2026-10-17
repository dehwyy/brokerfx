package durable

import (
	"fmt"
	"slices"

	"github.com/nats-io/nats.go/jetstream"
)

func checkFilters(
	req *BindRequest,
	actual jetstream.ConsumerConfig,
) error {
	wanted := Filters(req.Config)
	got := Filters(actual)
	if slices.Equal(
		wanted,
		got,
	) {
		return nil
	}

	return fmt.Errorf(
		"%w: stream %s durable %s: wanted %v, got %v",
		ErrFilterMismatch,
		req.Stream,
		Name(req.Config),
		wanted,
		got,
	)
}
