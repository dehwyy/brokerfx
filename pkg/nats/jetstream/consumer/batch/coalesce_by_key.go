package batch

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

func CoalesceByKey(
	key KeyFunc,
	apply func(ctx context.Context, groups []Group) []Result,
) Handler {
	if key == nil || apply == nil {
		panic("batch: CoalesceByKey requires key and apply")
	}

	return func(ctx context.Context, msgs []jetstream.Msg) []Result {
		results := make(
			[]Result,
			len(msgs),
		)
		if len(msgs) == 0 {
			return results
		}

		groups, indexes := groupIndexed(
			msgs,
			key,
		)
		groupResults := apply(
			ctx,
			groups,
		)

		for position, members := range indexes {
			result := Nak(ErrMissingResult)
			if position < len(groupResults) {
				result = groupResults[position]
			}
			for _, index := range members {
				results[index] = result
			}
		}

		return results
	}
}
