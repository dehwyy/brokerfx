package outbox

import "context"

// Producer is the transport abstraction used by the outbox relay to deliver
// events to NATS. Implementations may target JetStream (acked) or core NATS
// (fire-and-forget). The relay treats a nil error as a successful publish.
type Producer interface {
	Produce(ctx context.Context, event ProducerEvent) error
}

// ProducerEvent is the wire-level representation of an outbox row handed to the
// Producer. Subject and Payload mirror the columns in outbox_events; Headers
// carry transport metadata (msg id, trace context, signatures).
type ProducerEvent struct {
	Subject string
	Headers map[string]string
	Payload []byte
}
