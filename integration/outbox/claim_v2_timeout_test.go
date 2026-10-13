//go:build integration

package outbox_test

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type slowClaimGate struct {
	enabled atomic.Bool
	delay   time.Duration
}

func (g *slowClaimGate) register(t *testing.T, db *gorm.DB) {
	t.Helper()

	require.NoError(
		t,
		db.Callback().Row().After("gorm:row").Register(
			"test:slow_claim_"+t.Name(),
			func(tx *gorm.DB) {
				if g.enabled.Load() && strings.Contains(
					tx.Statement.SQL.String(),
					"SET state = 'IN_FLIGHT'",
				) {
					time.Sleep(g.delay)
				}
			},
		),
	)
}

func TestClaimV2TimeoutAfterStatementLeavesRowsPending(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(
		t,
		outbox.AutoMigrate(db),
	)

	gate := &slowClaimGate{
		delay: 400 * time.Millisecond,
	}
	gate.register(
		t,
		db,
	)
	gate.enabled.Store(true)

	base := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	seeds := make(
		[]claimSeed,
		0,
		20,
	)
	for i := 0; i < 20; i++ {
		seeds = append(
			seeds,
			claimSeed{
				ID:        fmt.Sprintf("00000000-0000-0000-0001-%012d", i),
				Payload:   fmt.Sprintf("t-%02d", i),
				State:     outbox.StatePending,
				CreatedAt: base.Add(time.Duration(i) * time.Millisecond),
				UpdatedAt: base,
			},
		)
	}
	seedClaimRows(
		t,
		db,
		seeds,
	)

	cfg := claimV2Config(
		outbox.ModeUpdateAfterSend,
		true,
	)
	cfg.ClaimTimeout = 100 * time.Millisecond
	cfg.StallThreshold = time.Hour
	cfg.StallReclaimInterval = time.Hour

	producer := &testenv.FakeBatchProducer{}
	harness := testenv.NewRelayHarness(
		t,
		db,
		producer,
		cfg,
	)
	harness.Start(t)
	harness.Wake()
	time.Sleep(1500 * time.Millisecond)

	require.Zero(
		t,
		countRows(
			t,
			db,
			"select count(*) from outbox_events where state = ?",
			outbox.StateInFlight,
		),
	)
	require.Zero(
		t,
		producer.CallCount(),
	)

	gate.enabled.Store(false)
	require.Eventually(
		t,
		func() bool {
			harness.Wake()
			return producer.CallCount() == 20 && countRows(
				t,
				db,
				"select count(*) from outbox_events where state = ?",
				outbox.StatePending,
			) == 0
		},
		20*time.Second,
		50*time.Millisecond,
	)
}
