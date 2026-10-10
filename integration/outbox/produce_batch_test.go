//go:build integration

package outbox_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func batchEvents(subject string, n int) []outbox.ProducerEvent {
	events := make([]outbox.ProducerEvent, n)
	for i := range events {
		events[i] = outbox.ProducerEvent{
			Subject: subject,
			Headers: map[string]string{jetstream.MsgIDHeader: fmt.Sprintf("%s-%d", subject, i)},
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
		}
	}
	return events
}

func TestProduceBatchAllAcked(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	newStream(t, js, "PRODUCE_BATCH", "produce.batch")

	errs := outbox.NewJetStreamProducer(js).ProduceBatch(context.Background(), batchEvents("produce.batch", 100))

	require.Len(t, errs, 100)
	for _, err := range errs {
		require.NoError(t, err)
	}
	stream, err := js.Stream(context.Background(), "PRODUCE_BATCH")
	require.NoError(t, err)
	info, err := stream.Info(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 100, info.State.Msgs)
}

func TestProduceBatchPerEventError(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	newStream(t, js, "PRODUCE_PER_EVENT", "produce.ok")

	events := batchEvents("produce.ok", 5)
	events[2].Subject = "produce.nostream"

	errs := outbox.NewJetStreamProducer(js).ProduceBatch(context.Background(), events)

	require.Len(t, errs, 5)
	for i, err := range errs {
		if i == 2 {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
	}
}

func TestProduceBatchOverMaxPending(t *testing.T) {
	base, _ := testenv.EmbeddedNATS(t)
	newStream(t, base, "PRODUCE_MAX_PENDING", "produce.pending")
	js, err := jetstream.New(base.Conn(), jetstream.WithPublishAsyncMaxPending(4))
	require.NoError(t, err)

	started := time.Now()
	errs := outbox.NewJetStreamProducer(js).ProduceBatch(context.Background(), batchEvents("produce.pending", 50))

	require.Less(t, time.Since(started), 5*time.Second)
	require.Len(t, errs, 50)
}
