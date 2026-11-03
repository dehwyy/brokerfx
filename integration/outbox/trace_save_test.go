//go:build integration

package outbox_test

import (
	"context"
	"testing"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/dehwyy/tracerfx/pkg/tracer/correlate"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func tracedSaveContext(t *testing.T) (context.Context, string) {
	t.Helper()

	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	ctx, span := provider.Tracer("test").Start(
		context.Background(),
		"save",
	)
	t.Cleanup(func() {
		span.End()
	})

	return correlate.WithTxID(
		ctx,
		"tx-save",
	), span.SpanContext().TraceID().String()
}

func TestSaveStoresTraceHeaders(t *testing.T) {
	t.Parallel()

	t.Run("save message and save messages under span", func(t *testing.T) {
		t.Parallel()

		db := testenv.Postgres(t)
		require.NoError(t, outbox.AutoMigrate(db))
		store := newStoreForTest(t, db)
		ctx, traceID := tracedSaveContext(t)

		require.NoError(t, store.SaveMessage(
			ctx,
			outbox.Message{
				Subject: "trace.save.one",
				Payload: []byte("one"),
			},
		))
		require.NoError(t, store.SaveMessages(
			ctx,
			[]outbox.Message{
				{
					Subject: "trace.save.many",
					Payload: []byte("a"),
				},
				{
					Subject: "trace.save.many",
					Payload: []byte("b"),
				},
			},
		))

		var headers []string
		require.NoError(t, db.Raw(`select headers::text from outbox_events where topic like 'trace.save.%' order by created_at`).Scan(&headers).Error)
		require.Len(t, headers, 3)
		for _, stored := range headers {
			require.Contains(t, stored, `"traceparent"`)
			require.Contains(t, stored, traceID)
			require.Contains(t, stored, "pl.tx_id=tx-save")
		}
	})

	t.Run("no span leaves headers null", func(t *testing.T) {
		t.Parallel()

		db := testenv.Postgres(t)
		require.NoError(t, outbox.AutoMigrate(db))
		store := newStoreForTest(t, db)

		require.NoError(t, store.SaveMessage(
			context.Background(),
			outbox.Message{
				Subject: "trace.nospan.one",
				Payload: []byte("one"),
			},
		))
		require.NoError(t, store.SaveMessages(
			context.Background(),
			[]outbox.Message{{
				Subject: "trace.nospan.many",
				Payload: []byte("a"),
			}},
		))

		var nulls int64
		require.NoError(t, db.Raw(`select count(*) from outbox_events where headers is null`).Scan(&nulls).Error)
		require.Equal(t, int64(2), nulls)
	})

	t.Run("table without headers column saves", func(t *testing.T) {
		t.Parallel()

		db := testenv.Postgres(t)
		createV1OutboxEventsTable(t, db)
		store := newStoreForTest(t, db)
		ctx, _ := tracedSaveContext(t)

		require.NoError(t, store.SaveMessage(
			ctx,
			outbox.Message{
				Subject: "trace.v1.one",
				Payload: []byte("one"),
			},
		))
		require.NoError(t, store.SaveMessages(
			ctx,
			[]outbox.Message{{
				Subject: "trace.v1.many",
				Payload: []byte("a"),
			}},
		))

		var count int64
		require.NoError(t, db.Model(&outbox.OutboxEvent{}).Where("topic like ?", "trace.v1.%").Count(&count).Error)
		require.Equal(t, int64(2), count)
	})
}
