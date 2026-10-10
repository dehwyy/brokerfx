package outbox

import "context"

type BatchProducer interface {
	ProduceBatch(ctx context.Context, events []ProducerEvent) []error
}

var _ BatchProducer = (*JetStreamProducer)(nil)
