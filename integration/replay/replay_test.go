//go:build integration

package replay_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/replay"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type countingSource struct {
	total   int
	batch   int
	sent    int
	calls   int
	failAt  int
	onBatch func(sent int)
}

func (s *countingSource) Next(_ context.Context) ([]replay.Item, error) {
	s.calls++
	if s.onBatch != nil {
		s.onBatch(s.sent)
	}
	if s.failAt > 0 && s.sent >= s.failAt {
		return nil, errors.New("source broke")
	}
	if s.sent >= s.total {
		return nil, nil
	}
	n := s.batch
	if s.sent+n > s.total {
		n = s.total - s.sent
	}
	items := make([]replay.Item, 0, n)
	for i := 0; i < n; i++ {
		s.sent++
		items = append(items, replay.Item{Key: fmt.Sprintf("k%d", s.sent), Version: "1", Payload: []byte("p")})
	}
	return items, nil
}

func fastConfig() replay.Config {
	return replay.Config{RatePerSecond: 100000, PublishAttempts: 3, RetryDelay: 10 * time.Millisecond, FinalizeTimeout: 5 * time.Second}
}

func createStream(t *testing.T, js jetstream.JetStream, cfg jetstream.StreamConfig) {
	t.Helper()
	_, err := js.CreateStream(context.Background(), cfg)
	require.NoError(t, err)
}

func replayStream() jetstream.StreamConfig {
	return streamoptsbuilder.NewDefault().
		WithName("REPLAY_T").
		WithSubjects([]string{"t.replay.>"}).
		WithRetentionPolicy(jetstream.LimitsPolicy).
		WithMaxMsgsPerSubject(-1).
		WithReplicas(1).
		WithMaxAge(time.Hour).
		Build()
}

func readAll(t *testing.T, js jetstream.JetStream, stream, subject string) []replay.Meta {
	t.Helper()
	ctx := context.Background()
	cons, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{FilterSubjects: []string{subject}})
	require.NoError(t, err)
	var out []replay.Meta
	for {
		batch, err := cons.Fetch(500, jetstream.FetchMaxWait(1500*time.Millisecond))
		require.NoError(t, err)
		got := 0
		for m := range batch.Messages() {
			meta, err := replay.ParseMeta(m.Headers())
			require.NoError(t, err)
			out = append(out, meta)
			got++
		}
		require.NoError(t, batch.Error())
		if got == 0 {
			return out
		}
	}
}

func TestReplayLargeRunKeepsAllSnapshots(t *testing.T) {
	js := testenv.NATS(t)
	createStream(t, js, replayStream())
	pub, err := replay.NewPublisher(js, fastConfig())
	require.NoError(t, err)

	res, err := pub.Run(context.Background(), replay.Request{ReplayID: "r1", Subject: "t.replay.r1", EndPayload: []byte("end")}, &countingSource{total: 1500, batch: 100})
	require.NoError(t, err)
	require.Equal(t, replay.StatusComplete, res.Status)

	metas := readAll(t, js, "REPLAY_T", "t.replay.r1")
	require.Len(t, metas, 1501)
	last := metas[len(metas)-1]
	require.True(t, last.End)
	require.Equal(t, int64(1500), last.Count)
	require.Equal(t, replay.StatusComplete, last.Status)
	for i, m := range metas[:1500] {
		require.False(t, m.End)
		require.Equal(t, int64(i+1), m.Seq)
	}
}

func TestReplayRejectsStreamWithSubjectLimit(t *testing.T) {
	js := testenv.NATS(t)
	cfg := streamoptsbuilder.NewDefault().
		WithName("REPLAY_DEF").
		WithSubjects([]string{"t.replay.>"}).
		Build()
	createStream(t, js, cfg)
	pub, err := replay.NewPublisher(js, fastConfig())
	require.NoError(t, err)

	src := &countingSource{total: 10, batch: 10}
	_, err = pub.Run(context.Background(), replay.Request{ReplayID: "r1", Subject: "t.replay.r1"}, src)
	require.ErrorIs(t, err, replay.ErrStreamUnsuitable)
	require.Contains(t, err.Error(), "REPLAY_DEF")
	require.Contains(t, err.Error(), "1000")
	require.Zero(t, src.calls)
}

func TestReplayRejectsSubjectWithoutStream(t *testing.T) {
	js := testenv.NATS(t)
	pub, err := replay.NewPublisher(js, fastConfig())
	require.NoError(t, err)

	src := &countingSource{total: 10, batch: 10}
	_, err = pub.Run(context.Background(), replay.Request{ReplayID: "r1", Subject: "t.replay.r1"}, src)
	require.ErrorIs(t, err, replay.ErrStreamUnsuitable)
	require.Zero(t, src.calls)
}

func TestReplayCancelPublishesFailedMarker(t *testing.T) {
	js := testenv.NATS(t)
	createStream(t, js, replayStream())
	pub, err := replay.NewPublisher(js, fastConfig())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &countingSource{total: 1000, batch: 100, onBatch: func(sent int) {
		if sent >= 300 {
			cancel()
		}
	}}
	_, err = pub.Run(ctx, replay.Request{ReplayID: "r1", Subject: "t.replay.r1"}, src)
	require.ErrorIs(t, err, context.Canceled)

	metas := readAll(t, js, "REPLAY_T", "t.replay.r1")
	require.Len(t, metas, 301)
	last := metas[len(metas)-1]
	require.True(t, last.End)
	require.Equal(t, replay.StatusFailed, last.Status)
	require.Equal(t, int64(300), last.Count)
}

func TestReplayRetryWithSameIDDedupsSnapshots(t *testing.T) {
	js := testenv.NATS(t)
	createStream(t, js, replayStream())
	pub, err := replay.NewPublisher(js, fastConfig())
	require.NoError(t, err)
	req := replay.Request{ReplayID: "r1", Subject: "t.replay.r1", EndPayload: []byte("end")}

	_, err = pub.Run(context.Background(), req, &countingSource{total: 10, batch: 3, failAt: 3})
	require.Error(t, err)

	res, err := pub.Run(context.Background(), req, &countingSource{total: 10, batch: 3})
	require.NoError(t, err)
	require.Equal(t, replay.StatusComplete, res.Status)
	require.Equal(t, int64(3), res.Duplicates)

	metas := readAll(t, js, "REPLAY_T", "t.replay.r1")
	var snaps, failed, complete int
	for _, m := range metas {
		switch {
		case !m.End:
			snaps++
		case m.Status == replay.StatusFailed:
			failed++
		case m.Status == replay.StatusComplete:
			complete++
		}
	}
	require.Equal(t, 10, snaps)
	require.Equal(t, 1, failed)
	require.Equal(t, 1, complete)

	_, err = pub.Run(context.Background(), req, &countingSource{total: 10, batch: 3})
	require.NoError(t, err)
	require.Len(t, readAll(t, js, "REPLAY_T", "t.replay.r1"), len(metas))
}
