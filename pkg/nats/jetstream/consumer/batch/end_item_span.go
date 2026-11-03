package batch

import "go.opentelemetry.io/otel/codes"

func (s itemSpans) end(index int, kind settleKind) {
	if index < 0 || index >= len(s) {
		return
	}

	span := s[index]
	if description := kind.failure(); description != "" {
		span.SetStatus(
			codes.Error,
			description,
		)
	}
	span.End()
}

func (s itemSpans) endUnsettled() {
	for _, span := range s {
		if !span.IsRecording() {
			continue
		}

		span.SetStatus(
			codes.Error,
			"unsettled",
		)
		span.End()
	}
}
