package consumer

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func consumeOpts(
	opts Opts,
	group string,
) []jetstream.PullConsumeOpt {
	consume := []jetstream.PullConsumeOpt{
		jetstream.PullMaxMessages(pullMaxMessages(opts.MaxInFlight)),
		jetstream.PullHeartbeat(10 * time.Second),
		jetstream.PullExpiry(pullExpiry),
		jetstream.ConsumeErrHandler(consumeErrHandler(opts)),
	}
	if group != "" {
		consume = append(
			consume,
			jetstream.PullPriorityGroup(group),
		)
	}

	return consume
}
