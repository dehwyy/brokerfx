package outbox

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStreamProducer adapts a jetstream.JetStream connection to the Producer
// interface. It blocks per call until the server acknowledges the publish or
// a safety timeout elapses, so the relay sees a clear success/failure signal.
type JetStreamProducer struct {
	js      jetstream.JetStream
	timeout time.Duration
}

// NewJetStreamProducer constructs a JetStreamProducer with a 30 second ack timeout.
func NewJetStreamProducer(js jetstream.JetStream) *JetStreamProducer {
	return &JetStreamProducer{
		js:      js,
		timeout: 30 * time.Second,
	}
}

var _ Producer = (*JetStreamProducer)(nil)

func (p *JetStreamProducer) Produce(_ context.Context, event ProducerEvent) error {
	msg := &nats.Msg{
		Subject: event.Subject,
		Data:    event.Payload,
		Header:  nats.Header{},
	}

	for k, v := range event.Headers {
		msg.Header.Set(k, v)
	}

	future, err := p.js.PublishMsgAsync(msg)
	if err != nil {
		return err
	}

	select {
	case <-future.Ok():
		return nil
	case ackErr := <-future.Err():
		return ackErr
	case <-time.After(p.timeout):
		return context.DeadlineExceeded
	}
}
