//go:build integration

package frames_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func newStream(t *testing.T, js jetstream.JetStream, name string) *stream.Stream {
	t.Helper()
	_, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{name + ".>"},
		Storage:  jetstream.FileStorage,
	})
	require.NoError(t, err)
	bound, err := stream.Bind(js, name)
	require.NoError(t, err)
	return bound
}

func publishFrame(t *testing.T, js jetstream.JetStream, subject string, ids ...string) {
	t.Helper()
	encoded := make([][]byte, 0, len(ids))
	for _, id := range ids {
		element, err := frame.Marshal(frame.Element{
			Subject: subject,
			Headers: map[string][]string{jetstream.MsgIDHeader: {id}},
			Payload: []byte(id),
		})
		require.NoError(t, err)
		encoded = append(encoded, element)
	}
	msg := nats.NewMsg(subject)
	msg.Data = frame.Join(encoded)
	for key, value := range frame.WireHeaders(ids) {
		msg.Header.Set(key, value)
	}
	_, err := js.PublishMsg(context.Background(), msg)
	require.NoError(t, err)
}

func streamMsgs(t *testing.T, js jetstream.JetStream, name string) uint64 {
	t.Helper()
	s, err := js.Stream(context.Background(), name)
	require.NoError(t, err)
	info, err := s.Info(context.Background())
	require.NoError(t, err)
	return info.State.Msgs
}

func consumerInfo(t *testing.T, js jetstream.JetStream, streamName, name string) *jetstream.ConsumerInfo {
	t.Helper()
	c, err := js.Consumer(context.Background(), streamName, name)
	require.NoError(t, err)
	info, err := c.Info(context.Background())
	require.NoError(t, err)
	return info
}

func settledClean(t *testing.T, js jetstream.JetStream, streamName, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		info := consumerInfo(t, js, streamName, name)
		return info.NumAckPending == 0 && info.NumPending == 0
	}, 10*time.Second, 50*time.Millisecond, name)
}

type recorder struct {
	mu        sync.Mutex
	bySubject map[string][]string
	calls     map[string]int
	total     int
}

func newRecorder() *recorder {
	return &recorder{bySubject: map[string][]string{}, calls: map[string]int{}}
}

func (r *recorder) add(msg jetstream.Msg) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bySubject[msg.Subject()] = append(r.bySubject[msg.Subject()], string(msg.Data()))
	r.calls[string(msg.Data())]++
	r.total++
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

func (r *recorder) subject(subject string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.bySubject[subject]...)
}

func (r *recorder) callsOf(payload string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[payload]
}
