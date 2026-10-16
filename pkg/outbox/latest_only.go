package outbox

import (
	"net/textproto"
	"strconv"
	"strings"
)

const LatestVersionHeader = "Brokerfx-Latest-Version"

type SupersededObserver interface {
	OnSuperseded(n int)
}

type latestCandidate struct {
	index      int
	version    int64
	allVersion bool
}

func latestOnlyPrefixes(configured []string, fromEnv []string) []string {
	prefixes := []string{
		kvSubjectPrefix,
	}
	seen := map[string]struct{}{
		kvSubjectPrefix: {},
	}
	for _, list := range [][]string{configured, fromEnv} {
		for _, prefix := range list {
			prefix = strings.TrimSpace(prefix)
			if prefix == "" {
				continue
			}
			if _, ok := seen[prefix]; ok {
				continue
			}
			seen[prefix] = struct{}{}
			prefixes = append(
				prefixes,
				prefix,
			)
		}
	}

	return prefixes
}

func latestIndexes(
	events []relayEvent,
	prefixes []string,
) map[string]int {
	candidates := make(map[string]latestCandidate)
	for i, event := range events {
		if !hasAnyPrefix(
			event.Topic,
			prefixes,
		) {
			continue
		}

		version, versioned := eventVersion(event.Headers)
		current, ok := candidates[event.Topic]
		if !ok {
			candidates[event.Topic] = latestCandidate{
				index:      i,
				version:    version,
				allVersion: versioned,
			}
			continue
		}

		allVersion := current.allVersion && versioned
		next := current
		next.allVersion = allVersion
		if !allVersion || version >= current.version {
			next.index = i
			next.version = version
		}
		candidates[event.Topic] = next
	}

	indexes := make(
		map[string]int,
		len(candidates),
	)
	for topic, candidate := range candidates {
		indexes[topic] = candidate.index
	}

	return indexes
}

func hasAnyPrefix(topic string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(
			topic,
			prefix,
		) {
			return true
		}
	}

	return false
}

func eventVersion(headers map[string]string) (int64, bool) {
	canonical := textproto.CanonicalMIMEHeaderKey(LatestVersionHeader)
	for key, value := range headers {
		if textproto.CanonicalMIMEHeaderKey(key) != canonical {
			continue
		}

		version, err := strconv.ParseInt(
			strings.TrimSpace(value),
			10,
			64,
		)
		if err != nil {
			return 0, false
		}

		return version, true
	}

	return 0, false
}
