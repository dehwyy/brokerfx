//go:build integration

package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/dehwyy/txmanagerfx/pkg/txmanager/gormtx"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newStoreForTest(t *testing.T, db *gorm.DB) *outbox.OutboxStore {
	t.Helper()

	txm, err := gormtx.New(gormtx.Opts{DB: db})
	require.NoError(t, err)

	return outbox.NewStore(outbox.StoreDeps{
		DB:        db,
		TxManager: txm,
	})
}

func TestSaveMessagePersistsHeadersAndRelayPublishesThem(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "HEADERS_SAVE", "headers.save")

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})

	require.NoError(t, harness.Store.SaveMessage(
		context.Background(),
		outbox.Message{
			Subject: "headers.save",
			Payload: []byte("payload-headers"),
		},
		outbox.WithHeaders(map[string]string{"X-A": "1"}),
	))

	harness.Start(t)
	harness.Wake()

	stream, err := js.Stream(context.Background(), "HEADERS_SAVE")
	require.NoError(t, err)

	var raw *jetstream.RawStreamMsg
	require.Eventually(t, func() bool {
		var getErr error
		raw, getErr = stream.GetLastMsgForSubject(context.Background(), "headers.save")
		return getErr == nil
	}, 10*time.Second, 100*time.Millisecond, "published message with custom headers should be visible in the stream")

	require.Equal(t, "1", raw.Header.Get("X-A"))
	require.NotEmpty(t, raw.Header.Get(jetstream.MsgIDHeader))
}

func TestSaveMessageWithHeadersOnV1SchemaReturnsErrSchemaOutdated(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	createV1OutboxEventsTable(t, db)

	store := newStoreForTest(t, db)

	err := store.SaveMessage(
		context.Background(),
		outbox.Message{
			Subject: "headers.v1.reject",
			Payload: []byte("payload"),
		},
		outbox.WithHeaders(map[string]string{"X-A": "1"}),
	)

	require.ErrorIs(t, err, outbox.ErrSchemaOutdated)

	var count int64
	require.NoError(t, db.Model(&outbox.OutboxEvent{}).Where("topic = ?", "headers.v1.reject").Count(&count).Error)
	require.Equal(t, int64(0), count)
}

func TestSaveMessageWithoutHeadersOnV1SchemaSucceeds(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	createV1OutboxEventsTable(t, db)

	store := newStoreForTest(t, db)

	require.NoError(t, store.SaveMessage(
		context.Background(),
		outbox.Message{
			Subject: "headers.v1.ok",
			Payload: []byte("payload"),
		},
	))

	var count int64
	require.NoError(t, db.Model(&outbox.OutboxEvent{}).Where("topic = ?", "headers.v1.ok").Count(&count).Error)
	require.Equal(t, int64(1), count)
}

func TestSaveMessageEmptyPayloadStoredAsEmptyBytea(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	store := newStoreForTest(t, db)

	require.NoError(t, store.SaveMessage(
		context.Background(),
		outbox.Message{
			Subject: "headers.empty.payload",
		},
	))

	var payload []byte
	var isNull bool
	row := db.Raw(`select payload, payload is null from outbox_events where topic = ?`, "headers.empty.payload").Row()
	require.NoError(t, row.Scan(&payload, &isNull))

	require.False(t, isNull)
	require.Empty(t, payload)
}
