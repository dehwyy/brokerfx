package outbox

import (
	"context"
	"strings"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
)

func withTraceHeaders(ctx context.Context, headers map[string]string, v2 bool) map[string]string {
	if !v2 {
		return headers
	}

	trace := make(map[string]string, 3)
	tracenats.InjectMap(
		ctx,
		trace,
	)
	if len(trace) == 0 {
		return headers
	}

	merged := make(
		map[string]string,
		len(headers)+len(trace),
	)
	for key, value := range headers {
		merged[key] = value
	}

	for key, value := range trace {
		if hasHeaderFold(headers, key) {
			continue
		}
		merged[key] = value
	}

	return merged
}

func hasHeaderFold(headers map[string]string, key string) bool {
	for existing := range headers {
		if strings.EqualFold(existing, key) {
			return true
		}
	}

	return false
}
