package ordered

import (
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

func streamAttributes(msg jetstream.Msg) []attribute.KeyValue {
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return nil
	}

	return []attribute.KeyValue{
		attribute.String(
			"messaging.nats.stream",
			meta.Stream,
		),
		attribute.Int64(
			"messaging.nats.sequence.stream",
			int64(meta.Sequence.Stream),
		),
	}
}
