//go:build integration

package outbox_test

import (
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/stretchr/testify/require"
)

type retryGap struct {
	Attempts int
	Gap      float64
}

func envBackoff(attempts int, base time.Duration, ceiling time.Duration) time.Duration {
	delay := base
	for i := 1; i < attempts && delay < ceiling; i++ {
		delay *= 2
	}

	return min(
		delay,
		ceiling,
	)
}

func TestTxDrainRetryBackoffFromEnvWithJitter(t *testing.T) {
	base := time.Second
	ceiling := 2 * time.Second
	t.Setenv(
		"BROKERFX_OUTBOX_RETRY_BACKOFF_BASE",
		base.String(),
	)
	t.Setenv(
		"BROKERFX_OUTBOX_RETRY_BACKOFF_MAX",
		ceiling.String(),
	)
	t.Setenv(
		"BROKERFX_OUTBOX_RETRY_JITTER",
		"true",
	)

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js, _ := testenv.EmbeddedNATS(t)

	cfg := drainConfig(outbox.ModeDeleteAfterSend)
	cfg.RetryBackoffBase = 0
	cfg.RetryBackoffMax = 0
	harness := testenv.NewRelayHarness(
		t,
		db,
		outbox.NewJetStreamProducer(js),
		cfg,
	)
	seedDrain(
		t,
		harness.Store,
		"backoff",
		3,
	)
	harness.Start(t)
	harness.Wake()

	require.Eventually(
		t,
		func() bool {
			return countRows(t, db, "select count(*) from outbox_events where state = 'PENDING' and attempts >= 2") == 3
		},
		20*time.Second,
		50*time.Millisecond,
	)

	var gaps []retryGap
	require.NoError(t, db.Raw("select attempts, extract(epoch from next_attempt_at - updated_at)::float8 as gap from outbox_events where state = 'PENDING'").Scan(&gaps).Error)
	require.Len(t, gaps, 3)

	tolerance := time.Millisecond
	jittered := false
	for _, row := range gaps {
		delay := envBackoff(
			row.Attempts,
			base,
			ceiling,
		)
		gap := time.Duration(row.Gap * float64(time.Second))
		require.GreaterOrEqual(t, gap, delay-tolerance, "attempts %d", row.Attempts)
		require.Less(t, gap, delay+delay/2+tolerance, "attempts %d", row.Attempts)
		if gap > delay+tolerance {
			jittered = true
		}
	}
	require.True(t, jittered, "no row got jitter above the backoff")
	require.Zero(t, countRows(t, db, "select count(*) from outbox_events where state = 'PARKED'"))
}
