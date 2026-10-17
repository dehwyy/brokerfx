package outbox

import (
	"context"
	"errors"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func unknownOutcome(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, natsgo.ErrTimeout) ||
		errors.Is(err, jetstream.ErrAsyncPublishTimeout) ||
		errors.Is(err, jetstream.ErrTooManyStalledMsgs) ||
		isTransportError(err)
}
