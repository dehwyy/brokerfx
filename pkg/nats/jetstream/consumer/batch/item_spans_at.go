package batch

import "go.opentelemetry.io/otel/trace"

func (s itemSpans) at(index int) trace.Span {
	if index < 0 || index >= len(s) {
		return nil
	}

	return s[index]
}
