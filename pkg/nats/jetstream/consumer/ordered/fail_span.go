package ordered

import (
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func failSpan(span trace.Span, err error) {
	if span == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(
		codes.Error,
		err.Error(),
	)
}
