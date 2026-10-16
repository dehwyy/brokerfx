//go:build integration

package timedactor_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/timedactor"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type meta struct {
	Kind string `json:"kind"`
}

type fired struct {
	mu   sync.Mutex
	keys map[string]int
}

func (f *fired) add(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[key]++
}

func (f *fired) snapshot() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	copied := make(
		map[string]int,
		len(f.keys),
	)
	for key, count := range f.keys {
		copied[key] = count
	}
	return copied
}

func newFired() *fired {
	return &fired{
		keys: map[string]int{},
	}
}

func config(bucket string) timedactor.Config {
	return timedactor.Config{
		BucketName:      bucket,
		CheckInterval:   time.Second,
		MaxHoldDuration: 10 * time.Second,
		BucketTTL:       time.Hour,
		FileStorage:     true,
		Replicas:        1,
	}
}

func newActor(
	t *testing.T,
	js jetstream.JetStream,
	cfg timedactor.Config,
) *timedactor.TimedActor[meta] {
	t.Helper()

	actor, err := timedactor.New[meta](
		timedactor.Deps{
			JS:     js,
			Config: cfg,
		},
	)
	require.NoError(
		t,
		err,
	)
	t.Cleanup(actor.Stop)

	return actor
}

func subscribe(
	actor *timedactor.TimedActor[meta],
	sink *fired,
) {
	actor.Subscribe(
		context.Background(),
		">",
		nil,
		func(_ context.Context, key string, _ meta) {
			sink.add(key)
		},
	)
}

func kvStreamInfo(
	t *testing.T,
	js jetstream.JetStream,
	bucket string,
) *jetstream.StreamInfo {
	t.Helper()

	stream, err := js.Stream(
		context.Background(),
		"KV_"+bucket,
	)
	require.NoError(
		t,
		err,
	)

	info, err := stream.Info(context.Background())
	require.NoError(
		t,
		err,
	)

	return info
}

func TestPurgeMarkerDisappearsAfterMarkerTTL(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	cfg := config("TTL_MARKER")
	cfg.MarkerTTL = time.Second
	actor := newActor(
		t,
		js,
		cfg,
	)
	sink := newFired()
	subscribe(
		actor,
		sink,
	)

	ctx := context.Background()
	require.NoError(
		t,
		actor.Add(
			ctx,
			"order.fired",
			meta{
				Kind: "timeout",
			},
			100*time.Millisecond,
		),
	)
	require.NoError(
		t,
		actor.Add(
			ctx,
			"order.cleared",
			meta{
				Kind: "timeout",
			},
			time.Hour,
		),
	)
	require.NoError(
		t,
		actor.Clear(
			ctx,
			"order.cleared",
		),
	)

	require.Eventually(
		t,
		func() bool {
			return sink.snapshot()["order.fired"] == 1
		},
		5*time.Second,
		20*time.Millisecond,
	)

	require.Eventually(
		t,
		func() bool {
			info := kvStreamInfo(
				t,
				js,
				"TTL_MARKER",
			)
			return info.State.Msgs == 0 && info.State.NumSubjects == 0
		},
		10*time.Second,
		100*time.Millisecond,
	)
	require.Equal(
		t,
		map[string]int{
			"order.fired": 1,
		},
		sink.snapshot(),
	)
}

func TestNewEnablesMarkerTTLOnExistingBucket(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	ctx := context.Background()

	_, err := js.CreateOrUpdateKeyValue(
		ctx,
		jetstream.KeyValueConfig{
			Bucket:   "TTL_ENABLE",
			Storage:  jetstream.FileStorage,
			TTL:      time.Hour,
			Replicas: 1,
		},
	)
	require.NoError(
		t,
		err,
	)
	require.False(
		t,
		kvStreamInfo(
			t,
			js,
			"TTL_ENABLE",
		).Config.AllowMsgTTL,
	)

	cfg := config("TTL_ENABLE")
	cfg.MarkerTTL = 2 * time.Second
	newActor(
		t,
		js,
		cfg,
	)

	info := kvStreamInfo(
		t,
		js,
		"TTL_ENABLE",
	)
	require.True(
		t,
		info.Config.AllowMsgTTL,
	)
	require.Equal(
		t,
		2*time.Second,
		info.Config.SubjectDeleteMarkerTTL,
	)
}

func TestNewWithZeroMarkerTTLStartsOnTTLBucket(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)

	enabled := config("TTL_KEEP")
	enabled.MarkerTTL = 2 * time.Second
	first := newActor(
		t,
		js,
		enabled,
	)
	first.Stop()

	newActor(
		t,
		js,
		config("TTL_KEEP"),
	)

	info := kvStreamInfo(
		t,
		js,
		"TTL_KEEP",
	)
	require.True(
		t,
		info.Config.AllowMsgTTL,
	)
	require.Equal(
		t,
		2*time.Second,
		info.Config.SubjectDeleteMarkerTTL,
	)
}

func TestBareCreateOrUpdateOnTTLBucketFails(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)

	enabled := config("TTL_TRAP")
	enabled.MarkerTTL = 2 * time.Second
	newActor(
		t,
		js,
		enabled,
	).Stop()

	_, err := js.CreateOrUpdateKeyValue(
		context.Background(),
		jetstream.KeyValueConfig{
			Bucket:   "TTL_TRAP",
			Storage:  jetstream.FileStorage,
			TTL:      time.Hour,
			Replicas: 1,
		},
	)
	require.ErrorContains(
		t,
		err,
		"message TTL status can not be disabled",
	)
}

func TestTwoActorsFireEachTimerOnce(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	cfg := config("TTL_RACE")
	cfg.MarkerTTL = 5 * time.Second
	cfg.RescanInterval = time.Minute

	sink := newFired()
	first := newActor(
		t,
		js,
		cfg,
	)
	second := newActor(
		t,
		js,
		cfg,
	)
	subscribe(
		first,
		sink,
	)
	subscribe(
		second,
		sink,
	)

	const timers = 30
	want := make(
		map[string]int,
		timers,
	)
	for i := 0; i < timers; i++ {
		key := fmt.Sprintf(
			"order.%d",
			i,
		)
		want[key] = 1
		require.NoError(
			t,
			first.Add(
				context.Background(),
				key,
				meta{
					Kind: "timeout",
				},
				200*time.Millisecond,
			),
		)
	}

	require.Eventually(
		t,
		func() bool {
			return len(sink.snapshot()) == timers
		},
		10*time.Second,
		20*time.Millisecond,
	)
	time.Sleep(2 * time.Second)
	require.Equal(
		t,
		want,
		sink.snapshot(),
	)
}

func TestOverdueKeyBeforeStartFiresOnRescan(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	cfg := config("TTL_OVERDUE")
	cfg.MarkerTTL = 5 * time.Second
	cfg.RescanInterval = time.Minute

	writer := newActor(
		t,
		js,
		cfg,
	)
	require.NoError(
		t,
		writer.Add(
			context.Background(),
			"order.overdue",
			meta{
				Kind: "timeout",
			},
			-time.Minute,
		),
	)
	writer.Stop()

	sink := newFired()
	reader := newActor(
		t,
		js,
		cfg,
	)
	subscribe(
		reader,
		sink,
	)

	require.Eventually(
		t,
		func() bool {
			return sink.snapshot()["order.overdue"] == 1
		},
		5*time.Second,
		20*time.Millisecond,
	)
}
