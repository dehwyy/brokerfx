package consumeattr

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

func Attributes(msg jetstream.Msg) []attribute.KeyValue {
	var attrs []attribute.KeyValue

	if id := msg.Headers().Get(nats.MsgIdHdr); id != "" {
		attrs = append(
			attrs,
			attribute.String(
				"messaging.message.id",
				id,
			),
		)
	}

	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return attrs
	}

	return append(
		attrs,
		attribute.String(
			"messaging.nats.stream",
			meta.Stream,
		),
		attribute.String(
			"messaging.consumer.group.name",
			meta.Consumer,
		),
		attribute.Int64(
			"messaging.nats.sequence.stream",
			int64(meta.Sequence.Stream),
		),
		attribute.Int64(
			"messaging.nats.num_delivered",
			int64(meta.NumDelivered),
		),
	)
}
