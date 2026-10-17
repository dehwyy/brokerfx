//go:build integration

package batch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type seenLog struct {
	mu       sync.Mutex
	subjects []string
}

func (s *seenLog) handler(_ context.Context, msgs []jetstream.Msg) []batch.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, msg := range msgs {
		s.subjects = append(s.subjects, msg.Subject())
	}
	return ackAll(msgs)
}

func (s *seenLog) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subjects)
}

func (s *seenLog) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.subjects...)
}

func TestBatchFilterSubjectToFilterSubjectsKeepsAckFloor(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "FW")
	publish(t, js, "FW.order.updated", "m1", "m2", "m3")

	narrow := &seenLog{}
	first, err := batch.New(batch.Opts{JetStream: js, Stream: st, ConsumerOptsBuilder: builder("lane", "FW.order.updated"), Handler: narrow.handler})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return narrow.count() == 3 }, 10*time.Second, 20*time.Millisecond)
	drain(t, first)
	require.Equal(t, uint64(3), consumerInfo(t, js, "FW", "lane").AckFloor.Stream)

	wide := &seenLog{}
	second, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("lane", true).WithFilterSubjects("FW.order.updated", "FW.order.updated.>"),
		Handler:             wide.handler,
		StrictUpdate:        true,
	})
	require.NoError(t, err)

	info := consumerInfo(t, js, "FW", "lane")
	require.Empty(t, info.Config.FilterSubject)
	require.Equal(t, []string{"FW.order.updated", "FW.order.updated.>"}, info.Config.FilterSubjects)
	require.Equal(t, uint64(3), info.AckFloor.Stream)

	publish(t, js, "FW.order.created", "skip")
	publish(t, js, "FW.order.updated", "m4")
	publish(t, js, "FW.order.updated.42", "m5")
	require.Eventually(t, func() bool { return wide.count() == 2 }, 10*time.Second, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	drain(t, second)
	require.Equal(t, []string{"FW.order.updated", "FW.order.updated.42"}, wide.snapshot())
}

func TestBatchRejectsDuplicateFilters(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "FWBAD")
	_, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("lane", true).WithFilterSubjects("FWBAD.a", "FWBAD.a"),
		Handler:             ackHandler,
	})
	require.ErrorIs(t, err, batch.ErrInvalidOpts)
}
