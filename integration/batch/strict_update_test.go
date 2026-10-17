//go:build integration

package batch_test

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

type lockedBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func ackHandler(_ context.Context, msgs []jetstream.Msg) []batch.Result {
	return ackAll(msgs)
}

func TestBatchStrictUpdateRejectsMaxWaitingChange(t *testing.T) {
	buf := &lockedBuf{}
	prev := log.Logger
	log.Logger = zerolog.New(buf).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = prev })

	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "BSTRICT")
	first, err := batch.New(batch.Opts{JetStream: js, Stream: st, ConsumerOptsBuilder: builder("worker", "BSTRICT.>"), Handler: ackHandler})
	require.NoError(t, err)
	drain(t, first)

	_, err = batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("worker", "BSTRICT.>").WithMaxWaiting(7),
		Handler:             ackHandler,
		StrictUpdate:        true,
	})
	require.ErrorIs(t, err, consumer.ErrUpdateRejected)
	require.Contains(t, err.Error(), "BSTRICT")
	require.Contains(t, err.Error(), "err_code 10012")

	lax, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: builder("worker", "BSTRICT.>").WithMaxWaiting(7),
		Handler:             ackHandler,
	})
	require.NoError(t, err)
	drain(t, lax)
	require.Contains(t, buf.String(), "batch consumer config update rejected, using existing durable as is")
	require.NotEqual(t, 7, consumerInfo(t, js, "BSTRICT", "worker").Config.MaxWaiting)
}
