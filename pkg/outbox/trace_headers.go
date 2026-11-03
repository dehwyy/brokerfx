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

	injected := make(
		map[string]string,
		len(traceHeaderKeys),
	)
	tracenats.InjectMap(
		ctx,
		injected,
	)
	if len(injected) == 0 {
		return headers
	}

	merged := make(
		map[string]string,
		len(headers)+len(injected),
	)
	for key, value := range headers {
		merged[key] = value
	}

	for key, value := range injected {
		if hasHeaderFold(
			headers,
			key,
		) {
			continue
		}
		merged[key] = value
	}

	return merged
}

func hasHeaderFold(headers map[string]string, key string) bool {
	for existing := range headers {
		if strings.EqualFold(
			existing,
			key,
		) {
			return true
		}
	}

	return false
}
