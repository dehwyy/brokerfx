//go:build integration

package outbox_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/signature"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type failingSigner struct {
	err error
}

func (s failingSigner) Sign(subject string, headers map[string]string, payload []byte) error {
	return s.err
}

func signatureHeaders(raw *jetstream.RawStreamMsg) map[string]string {
	return map[string]string{
		signature.HeaderAPIKeyID:  raw.Header.Get(signature.HeaderAPIKeyID),
		signature.HeaderTimestamp: raw.Header.Get(signature.HeaderTimestamp),
		signature.HeaderSignature: raw.Header.Get(signature.HeaderSignature),
	}
}

func TestSignerSetRelayPublishedMessagePassesVerify(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "SIGNATURE_VERIFY", "signature.verify")

	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	}, testenv.WithSigner(signature.NewEd25519Signer("key-verify", privateKey)))

	require.NoError(t, harness.Store.SaveMessage(
		context.Background(),
		outbox.Message{
			Subject: "signature.verify",
			Payload: []byte("payload-verify"),
		},
	))

	harness.Start(t)
	harness.Wake()

	stream, err := js.Stream(context.Background(), "SIGNATURE_VERIFY")
	require.NoError(t, err)

	var raw *jetstream.RawStreamMsg
	require.Eventually(t, func() bool {
		var getErr error
		raw, getErr = stream.GetLastMsgForSubject(context.Background(), "signature.verify")
		return getErr == nil
	}, 10*time.Second, 100*time.Millisecond, "signed message should be visible in the stream")

	apiKeyID, err := signature.Verify(
		signatureHeaders(raw),
		publicKey,
		[]byte("signature.verify"),
		raw.Data,
		time.Now(),
		5*time.Minute,
	)
	require.NoError(t, err)
	require.Equal(t, "key-verify", apiKeyID)
}

func TestSignerTimestampSetAtPublishNotInsert(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "SIGNATURE_STALE_ROW", "signature.stale-row")

	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	}, testenv.WithSigner(signature.NewEd25519Signer("key-stale-row", privateKey)))

	require.NoError(t, harness.Store.SaveMessage(
		context.Background(),
		outbox.Message{
			Subject: "signature.stale-row",
			Payload: []byte("payload-stale-row"),
		},
	))

	var id string
	require.NoError(t, db.Model(&outbox.OutboxEvent{}).Where("topic = ?", "signature.stale-row").Pluck("id", &id).Error)

	require.NoError(
		t,
		db.Model(&outbox.OutboxEvent{}).
			Where("id = ?", id).
			UpdateColumns(map[string]any{
				"created_at": time.Now().Add(-10 * time.Minute),
				"updated_at": time.Now().Add(-10 * time.Minute),
			}).Error,
	)

	harness.Start(t)
	harness.Wake()

	stream, err := js.Stream(context.Background(), "SIGNATURE_STALE_ROW")
	require.NoError(t, err)

	var raw *jetstream.RawStreamMsg
	require.Eventually(t, func() bool {
		var getErr error
		raw, getErr = stream.GetLastMsgForSubject(context.Background(), "signature.stale-row")
		return getErr == nil
	}, 10*time.Second, 100*time.Millisecond, "signed message for a row inserted 10 minutes ago should still be visible")

	_, err = signature.Verify(
		signatureHeaders(raw),
		publicKey,
		[]byte("signature.stale-row"),
		raw.Data,
		time.Now(),
		5*time.Minute,
	)
	require.NoError(t, err, "Verify with a 5 minute window must succeed because the timestamp is set at publish time, not row insert time")
}

func TestSignerErrorRevertsEventToPendingAndRecordsRetry(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	sentinel := errors.New("simulated signer failure")
	producer := &testenv.FakeProducer{}

	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	}, testenv.WithSigner(failingSigner{err: sentinel}))

	require.NoError(t, harness.Store.SaveMessage(
		context.Background(),
		outbox.Message{
			Subject: "signature.failure",
			Payload: []byte("payload-failure"),
		},
	))

	var id string
	require.NoError(t, db.Model(&outbox.OutboxEvent{}).Where("topic = ?", "signature.failure").Pluck("id", &id).Error)

	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StatePending
	}, 10*time.Second, 100*time.Millisecond, "event should revert to PENDING after a failed signature")

	var retries []outbox.OutboxRetry
	require.Eventually(t, func() bool {
		require.NoError(t, db.Where("event_id = ?", id).Find(&retries).Error)
		return len(retries) == 1
	}, 10*time.Second, 100*time.Millisecond, "exactly one retry row should be recorded")

	require.Equal(t, sentinel.Error(), retries[0].Error)
	require.Equal(t, 0, producer.CallCount(), "producer must not be called when the signer fails")
	require.Equal(t, outbox.StatePending, loadEvent(t, db, id).State, "event should be PENDING again after the retry row was recorded")
}

func TestNoSignerRelayPublishesWithoutSignatureHeaders(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "SIGNATURE_ABSENT", "signature.absent")

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
			Subject: "signature.absent",
			Payload: []byte("payload-absent"),
		},
	))

	harness.Start(t)
	harness.Wake()

	stream, err := js.Stream(context.Background(), "SIGNATURE_ABSENT")
	require.NoError(t, err)

	var raw *jetstream.RawStreamMsg
	require.Eventually(t, func() bool {
		var getErr error
		raw, getErr = stream.GetLastMsgForSubject(context.Background(), "signature.absent")
		return getErr == nil
	}, 10*time.Second, 100*time.Millisecond, "unsigned message should be visible in the stream")

	require.Empty(t, raw.Header.Get(signature.HeaderAPIKeyID))
	require.Empty(t, raw.Header.Get(signature.HeaderTimestamp))
	require.Empty(t, raw.Header.Get(signature.HeaderSignature))
}
