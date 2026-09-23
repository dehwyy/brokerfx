//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func newKVBucket(t *testing.T, js jetstream.JetStream, bucket string) jetstream.KeyValue {
	t.Helper()

	kv, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket: bucket,
	})
	require.NoError(t, err)

	return kv
}

func TestKVPutThroughRelayIsVisibleViaGetAndWatch(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	kv := newKVBucket(t, js, "KVPUT")

	watcher, err := kv.Watch(context.Background(), "key1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = watcher.Stop() })
	<-watcher.Updates()

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})

	require.NoError(t, harness.Store.SaveMessage(
		context.Background(),
		outbox.KVPut("KVPUT", "key1", []byte("value1")),
	))

	harness.Start(t)
	harness.Wake()

	var entry jetstream.KeyValueEntry
	require.Eventually(t, func() bool {
		var getErr error
		entry, getErr = kv.Get(context.Background(), "key1")
		return getErr == nil
	}, 10*time.Second, 100*time.Millisecond, "kv.Get should see the published value")

	require.Equal(t, "value1", string(entry.Value()))

	select {
	case update := <-watcher.Updates():
		require.NotNil(t, update)
		require.Equal(t, jetstream.KeyValuePut, update.Operation())
		require.Equal(t, "value1", string(update.Value()))
	case <-time.After(10 * time.Second):
		t.Fatal("watcher did not observe the put")
	}
}

func TestKVDeleteThroughRelayIsVisibleViaWatchAndGetFails(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	kv := newKVBucket(t, js, "KVDEL")

	_, err := kv.Put(context.Background(), "key1", []byte("initial"))
	require.NoError(t, err)

	watcher, err := kv.Watch(context.Background(), "key1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = watcher.Stop() })
	<-watcher.Updates()
	<-watcher.Updates()

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})

	require.NoError(t, harness.Store.SaveMessage(
		context.Background(),
		outbox.KVDelete("KVDEL", "key1"),
	))

	harness.Start(t)
	harness.Wake()

	select {
	case update := <-watcher.Updates():
		require.NotNil(t, update)
		require.Equal(t, jetstream.KeyValueDelete, update.Operation())
	case <-time.After(10 * time.Second):
		t.Fatal("watcher did not observe the delete")
	}

	require.Eventually(t, func() bool {
		_, getErr := kv.Get(context.Background(), "key1")
		return errors.Is(getErr, jetstream.ErrKeyNotFound)
	}, 10*time.Second, 100*time.Millisecond, "kv.Get should report key not found after delete")
}

func TestKVBatchWithMultipleRowsForSameKeyPublishesOnlyNewest(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	kv, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:  "KVFOLD",
		History: 3,
	})
	require.NoError(t, err)

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})

	for _, value := range []string{"v1", "v2", "v3"} {
		require.NoError(t, harness.Store.SaveMessage(
			context.Background(),
			outbox.KVPut("KVFOLD", "key1", []byte(value)),
		))
		time.Sleep(10 * time.Millisecond)
	}

	harness.Start(t)
	harness.Wake()

	var allDone bool
	require.Eventually(t, func() bool {
		var events []outbox.OutboxEvent
		require.NoError(t, db.Where("topic = ?", "$KV.KVFOLD.key1").Find(&events).Error)
		if len(events) != 3 {
			return false
		}
		allDone = true
		for _, ev := range events {
			if ev.State != outbox.StateDone {
				allDone = false
			}
		}
		return allDone
	}, 10*time.Second, 100*time.Millisecond, "all three rows should settle to DONE")

	entry, err := kv.Get(context.Background(), "key1")
	require.NoError(t, err)
	require.Equal(t, "v3", string(entry.Value()))

	history, err := kv.History(context.Background(), "key1")
	require.NoError(t, err)
	require.Len(t, history, 1, "only the newest row should have been published to the stream")

	stream, err := js.Stream(context.Background(), "KV_KVFOLD")
	require.NoError(t, err)

	info, err := stream.Info(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, info.State.LastSeq, "only one message should have reached the KV stream")
}

func TestNonKVRowsWithSameSubjectAreNotFolded(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "KVNOFOLD", "kv.nofold.subject")

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})

	for _, value := range []string{"m1", "m2", "m3"} {
		require.NoError(t, harness.Store.SaveMessage(
			context.Background(),
			outbox.Message{
				Subject: "kv.nofold.subject",
				Payload: []byte(value),
			},
		))
	}

	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		var count int64
		require.NoError(t, db.Model(&outbox.OutboxEvent{}).
			Where("topic = ? AND state = ?", "kv.nofold.subject", outbox.StateDone).
			Count(&count).Error)
		return count == 3
	}, 10*time.Second, 100*time.Millisecond, "all three non-kv rows should be individually published")

	stream, err := js.Stream(context.Background(), "KVNOFOLD")
	require.NoError(t, err)

	info, err := stream.Info(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 3, info.State.Msgs, "all three non-kv messages should have been published, not folded")
}
