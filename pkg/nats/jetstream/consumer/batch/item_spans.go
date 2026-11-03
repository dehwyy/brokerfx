package batch

import "go.opentelemetry.io/otel/trace"

type itemSpans []trace.Span

func (s itemSpans) slice(from int, to int) itemSpans {
	if len(s) < to {
		return nil
	}

	return s[from:to]
}
