//go:build integration

package frames_test

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/signature"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func relayConfig(subjects string) outbox.Config {
	return outbox.Config{
		Mode:            outbox.ModeDeleteAfterSend,
		BatchSize:       100,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
		Batched:         true,
		Linger:          20 * time.Millisecond,
		FrameSubjects:   subjects,
	}
}

func pendingRows(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Raw("select count(*) from outbox_events").Scan(&n).Error)
	return n
}

func relayMessages() ([]outbox.Message, []string, []string, []string) {
	var messages []outbox.Message
	var a, b, x []string
	for i := range 10 {
		a = append(a, fmt.Sprintf("a%d", i))
		messages = append(messages, outbox.Message{Subject: "FRAMES.cmd.a", Payload: []byte(a[i])})
		if i < 4 {
			payload := fmt.Sprintf("b%d", i)
			if i == 2 {
				payload = "b2-" + strings.Repeat("z", 3000)
			}
			b = append(b, payload)
			messages = append(messages, outbox.Message{Subject: "FRAMES.cmd.b", Payload: []byte(payload)})
		}
		if i < 2 {
			x = append(x, fmt.Sprintf("x%d", i))
			messages = append(messages, outbox.Message{Subject: "FRAMES.evt.x", Payload: []byte(x[i])})
		}
	}
	return messages, a, b, x
}

func TestRelayFramesReachEveryConsumerKindInOrder(t *testing.T) {
	t.Setenv("BROKERFX_FRAME_MAX_ELEMENTS", "4")
	t.Setenv("BROKERFX_FRAME_MAX_BYTES", "2048")

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))
	js, _ := testenv.EmbeddedNATS(t)
	st := newStream(t, js, "FRAMES")
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), relayConfig("FRAMES.cmd.>"), testenv.WithSigner(signature.NewEd25519Signer("key-frames", privateKey)))
	messages, a, b, x := relayMessages()
	require.NoError(t, harness.Store.SaveMessages(context.Background(), messages))
	harness.Start(t)
	harness.Wake()
	require.Eventually(t, func() bool { return pendingRows(t, db) == 0 }, 15*time.Second, 50*time.Millisecond)

	require.EqualValues(t, 8, streamMsgs(t, js, "FRAMES"))

	raw, err := js.CreateConsumer(context.Background(), "FRAMES", consumeroptsbuilder.NewDefault().WithName("raw", true).WithFilterSubject("FRAMES.>").Build())
	require.NoError(t, err)
	fetched, err := raw.Fetch(8, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)
	rawSeen := newRecorder()
	frames := 0
	for msg := range fetched.Messages() {
		if frame.IsFrame(msg) {
			frames++
			require.LessOrEqual(t, msg.Headers().Get(frame.HeaderCount), "4")
			require.Empty(t, msg.Headers().Get(signature.HeaderSignature))
		}
		elements, err := frame.Unpack(msg, time.Second)
		require.NoError(t, err)
		for _, element := range elements {
			headers := map[string]string{
				signature.HeaderAPIKeyID:  element.Headers().Get(signature.HeaderAPIKeyID),
				signature.HeaderTimestamp: element.Headers().Get(signature.HeaderTimestamp),
				signature.HeaderSignature: element.Headers().Get(signature.HeaderSignature),
			}
			keyID, err := signature.Verify(headers, publicKey, []byte(element.Subject()), element.Data(), time.Now(), 5*time.Minute)
			require.NoError(t, err)
			require.Equal(t, "key-frames", keyID)
			require.NotEmpty(t, element.Headers().Get(jetstream.MsgIDHeader))
			rawSeen.add(element)
			require.NoError(t, element.Ack())
		}
	}
	require.NoError(t, fetched.Error())
	require.Equal(t, 4, frames)
	require.Equal(t, a, rawSeen.subject("FRAMES.cmd.a"))
	require.Equal(t, b, rawSeen.subject("FRAMES.cmd.b"))
	require.Equal(t, x, rawSeen.subject("FRAMES.evt.x"))
	settledClean(t, js, "FRAMES", "raw")

	batchSeen := newRecorder()
	batchConsumer, err := batch.New(batch.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("batch", true).WithFilterSubject("FRAMES.>"),
		Handler: func(_ context.Context, msgs []jetstream.Msg) []batch.Result {
			results := make([]batch.Result, len(msgs))
			for i, msg := range msgs {
				batchSeen.add(msg)
				results[i] = batch.Ack()
			}
			return results
		},
	})
	require.NoError(t, err)

	singleSeen := newRecorder()
	_, err = consumer.New(consumer.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("single", true).WithFilterSubject("FRAMES.>"),
		MaxInFlight:         1,
		HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
			singleSeen.add(msg)
			return nil
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return batchSeen.count() == 16 && singleSeen.count() == 16 }, 15*time.Second, 50*time.Millisecond)
	for _, seen := range []*recorder{batchSeen, singleSeen} {
		require.Equal(t, a, seen.subject("FRAMES.cmd.a"))
		require.Equal(t, b, seen.subject("FRAMES.cmd.b"))
		require.Equal(t, x, seen.subject("FRAMES.evt.x"))
	}
	settledClean(t, js, "FRAMES", "batch")
	settledClean(t, js, "FRAMES", "single")
	require.Zero(t, consumerInfo(t, js, "FRAMES", "batch").NumRedelivered)
	require.Zero(t, consumerInfo(t, js, "FRAMES", "single").NumRedelivered)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, batchConsumer.Drain(ctx))
}

func TestEmptyFrameEnvPublishesPerMessage(t *testing.T) {
	t.Setenv("BROKERFX_FRAME_SUBJECTS", "")

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))
	js, _ := testenv.EmbeddedNATS(t)
	newStream(t, js, "NOFRAMES")

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), relayConfig("NOFRAMES.>"))
	messages := make([]outbox.Message, 0, 6)
	for i := range 6 {
		messages = append(messages, outbox.Message{Subject: "NOFRAMES.cmd", Payload: []byte(fmt.Sprintf("m%d", i))})
	}
	require.NoError(t, harness.Store.SaveMessages(context.Background(), messages))
	harness.Start(t)
	harness.Wake()
	require.Eventually(t, func() bool { return pendingRows(t, db) == 0 }, 15*time.Second, 50*time.Millisecond)

	require.EqualValues(t, 6, streamMsgs(t, js, "NOFRAMES"))
	s, err := js.Stream(context.Background(), "NOFRAMES")
	require.NoError(t, err)
	last, err := s.GetLastMsgForSubject(context.Background(), "NOFRAMES.cmd")
	require.NoError(t, err)
	require.Empty(t, last.Header.Get(frame.HeaderVersion))
}
