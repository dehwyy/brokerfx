//go:build integration

package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRelayPublishesTraceContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
	})

	cases := []struct {
		name    string
		batched bool
		stream  string
		subject string
	}{
		{
			name:    "regular relay",
			stream:  "TRACE_RELAY",
			subject: "trace.relay",
		},
		{
			name:    "batched relay",
			batched: true,
			stream:  "TRACE_RELAY_BATCHED",
			subject: "trace.relay.batched",
		},
	}
	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				db := testenv.Postgres(t)
				require.NoError(
					t,
					outbox.AutoMigrate(db),
				)

				js := testenv.NATS(t)
				newStream(
					t,
					js,
					tc.stream,
					tc.subject,
				)

				cfg := outbox.Config{
					Mode:            outbox.ModeUpdateAfterSend,
					BatchSize:       10,
					TickInterval:    time.Hour,
					DeleteOlderThan: time.Hour,
					StallThreshold:  5 * time.Minute,
				}
				if tc.batched {
					cfg = batchedConfig(outbox.ModeUpdateAfterSend)
				}
				harness := testenv.NewRelayHarness(
					t,
					db,
					outbox.NewJetStreamProducer(js),
					cfg,
				)

				ctx, traceID := tracedSaveContext(t)
				require.NoError(
					t,
					harness.Store.SaveMessage(
						ctx,
						outbox.Message{
							Subject: tc.subject,
							Payload: []byte("traced"),
						},
					),
				)

				harness.Start(t)
				harness.Wake()

				stream, err := js.Stream(
					context.Background(),
					tc.stream,
				)
				require.NoError(
					t,
					err,
				)

				var raw *jetstream.RawStreamMsg
				require.Eventually(
					t,
					func() bool {
						var getErr error
						raw, getErr = stream.GetLastMsgForSubject(
							context.Background(),
							tc.subject,
						)
						return getErr == nil
					},
					10*time.Second,
					100*time.Millisecond,
				)

				var relaySpanID string
				require.Eventually(
					t,
					func() bool {
						for _, span := range recorder.Ended() {
							if span.Name() == "outbox.relay" && hasDestination(
								span,
								tc.subject,
							) {
								relaySpanID = span.SpanContext().SpanID().String()
								return true
							}
						}
						return false
					},
					10*time.Second,
					50*time.Millisecond,
				)

				traceparent := raw.Header.Get("traceparent")
				require.Contains(
					t,
					traceparent,
					traceID,
				)
				require.Contains(
					t,
					traceparent,
					relaySpanID,
				)
				require.Contains(
					t,
					raw.Header.Get("baggage"),
					"pl.tx_id=tx-save",
				)
			},
		)
	}
}

func hasDestination(span sdktrace.ReadOnlySpan, subject string) bool {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == "messaging.destination.name" && kv.Value.AsString() == subject {
			return true
		}
	}

	return false
}
