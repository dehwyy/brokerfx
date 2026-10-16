package jetstream

import (
	"os"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

const envPublishAsyncMaxPending = "BROKERFX_JS_PUBLISH_ASYNC_MAX_PENDING"

func publishOptions() []jetstream.JetStreamOpt {
	maxPending, ok := publishAsyncMaxPending()
	if !ok {
		return nil
	}

	log.Info().
		Int(
			"publish_async_max_pending",
			maxPending,
		).
		Msg("jetstream async publish window set")

	return []jetstream.JetStreamOpt{
		jetstream.WithPublishAsyncMaxPending(maxPending),
	}
}

func publishAsyncMaxPending() (int, bool) {
	raw := strings.TrimSpace(os.Getenv(envPublishAsyncMaxPending))
	if raw == "" {
		return 0, false
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		log.Warn().
			Str(
				"env",
				envPublishAsyncMaxPending,
			).
			Str(
				"value",
				raw,
			).
			Msg("invalid async publish window, keeping the nats.go default")
		return 0, false
	}

	return value, true
}
