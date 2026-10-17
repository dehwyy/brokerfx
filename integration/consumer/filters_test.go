//go:build integration

package consumer_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestWideFiltersReceiveBothForms(t *testing.T) {
	buf := captureLog(t)
	js, _ := testenv.EmbeddedNATS(t)
	st := strictStream(t, js, "WIDE")

	var mu sync.Mutex
	var seen []string
	_, err := consumer.New(consumer.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("lane", true).WithFilterSubjects("WIDE.order.updated", "WIDE.order.updated.>"),
		HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, msg.Subject())
			return nil
		},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), `"filters":["WIDE.order.updated","WIDE.order.updated.>"]`)

	for _, subject := range []string{"WIDE.order.updated", "WIDE.order.created", "WIDE.order.updated.7"} {
		_, err := js.Publish(context.Background(), subject, []byte("x"))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 2
	}, 10*time.Second, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	slices.Sort(seen)
	require.Equal(t, []string{"WIDE.order.updated", "WIDE.order.updated.7"}, seen)
}
