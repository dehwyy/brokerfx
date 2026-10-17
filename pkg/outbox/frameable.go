package outbox

import (
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func (r *OutboxRelay) frameable(event ProducerEvent) bool {
	if !matchesAnyPattern(
		event.Subject,
		r.frames.patterns,
	) || strings.HasPrefix(
		event.Subject,
		kvSubjectPrefix,
	) || event.Headers[jetstream.MsgIDHeader] == "" {
		return false
	}

	if hasAnyPrefix(
		event.Subject,
		r.latestOnly,
	) {
		r.frames.latestOnce.Do(func() {
			log.Warn().
				Str(
					"subject",
					event.Subject,
				).
				Msg("outbox: frame subject is latest-only, publishing it per message")
		})
		return false
	}

	for key := range event.Headers {
		if !keepsFrameOut(key) {
			continue
		}
		r.frames.headerOnce.Do(func() {
			log.Debug().
				Str(
					"subject",
					event.Subject,
				).
				Str(
					"header",
					key,
				).
				Msg("outbox: event with a server-side publish header goes per message")
		})
		return false
	}

	return true
}

func keepsFrameOut(header string) bool {
	lower := strings.ToLower(header)

	return strings.HasPrefix(
		lower,
		"nats-expected-",
	) || lower == "nats-ttl" || lower == "nats-rollup"
}
