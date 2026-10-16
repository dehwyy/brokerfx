//go:build integration

package producer_test

import (
	"context"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/producer"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type syncEvent struct {
	subject string
}

func (e syncEvent) Subject() string {
	return e.subject
}

func (e syncEvent) Data() any {
	return map[string]string{
		"status": "accepted",
	}
}

func TestProduceSyncReturnsErrorWithoutStream(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	publisher := producer.New(producer.Opts{
		JetStream: js,
	})

	err := publisher.ProduceSync(
		context.Background(),
		syncEvent{
			subject: "sync.nostream",
		},
	)
	require.Error(
		t,
		err,
	)
}

func TestProduceSyncWaitsForPubAck(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	ctx := context.Background()
	stream, err := js.CreateStream(
		ctx,
		jetstream.StreamConfig{
			Name:     "SYNC",
			Subjects: []string{"sync.>"},
		},
	)
	require.NoError(
		t,
		err,
	)
	publisher := producer.New(producer.Opts{
		JetStream: js,
	})

	for range 2 {
		require.NoError(
			t,
			publisher.ProduceSync(
				ctx,
				syncEvent{
					subject: "sync.status",
				},
				producer.WithMsgID("status-1"),
			),
		)
	}

	info, err := stream.Info(ctx)
	require.NoError(
		t,
		err,
	)
	require.Equal(
		t,
		uint64(1),
		info.State.Msgs,
	)
}

func TestProduceSyncHonoursContext(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	_, err := js.CreateStream(
		context.Background(),
		jetstream.StreamConfig{
			Name:     "SYNCCTX",
			Subjects: []string{"syncctx.>"},
		},
	)
	require.NoError(
		t,
		err,
	)
	publisher := producer.New(producer.Opts{
		JetStream: js,
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Nanosecond,
	)
	defer cancel()
	time.Sleep(time.Millisecond)

	require.Error(
		t,
		publisher.ProduceSync(
			ctx,
			syncEvent{
				subject: "syncctx.status",
			},
		),
	)
}
