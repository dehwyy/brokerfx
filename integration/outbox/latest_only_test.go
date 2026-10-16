//go:build integration

package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/stretchr/testify/require"
)

func TestLatestOnlyPrefixPublishesMaxVersionPerSubject(t *testing.T) {
	t.Parallel()

	for _, batched := range []bool{false, true} {
		batched := batched
		t.Run(
			map[bool]string{false: "unbatched", true: "batched"}[batched],
			func(t *testing.T) {
				t.Parallel()

				db := testenv.Postgres(t)
				require.NoError(t, outbox.AutoMigrate(db))

				cfg := batchedConfig(outbox.ModeDeleteAfterSend)
				cfg.Batched = batched
				cfg.LatestOnlyPrefixes = "snap."
				producer := &testenv.FakeBatchProducer{}
				harness := testenv.NewRelayHarness(
					t,
					db,
					producer,
					cfg,
				)

				messages := []outbox.Message{
					{
						Subject: "snap.a",
						Payload: []byte("v3"),
						Headers: map[string]string{
							outbox.LatestVersionHeader: "3",
						},
					},
					{
						Subject: "snap.a",
						Payload: []byte("v5"),
						Headers: map[string]string{
							outbox.LatestVersionHeader: "5",
						},
					},
					{
						Subject: "snap.a",
						Payload: []byte("v4"),
						Headers: map[string]string{
							outbox.LatestVersionHeader: "4",
						},
					},
					{
						Subject: "events.a",
						Payload: []byte("e1"),
					},
					{
						Subject: "events.a",
						Payload: []byte("e2"),
					},
				}
				require.NoError(t, harness.Store.SaveMessages(context.Background(), messages))

				harness.Start(t)
				harness.Wake()

				require.Eventually(
					t,
					func() bool {
						return countRows(t, db, "select count(*) from outbox_events") == 0
					},
					10*time.Second,
					50*time.Millisecond,
				)

				require.ElementsMatch(t, []string{"v5", "e1", "e2"}, producer.Payloads())
			},
		)
	}
}
