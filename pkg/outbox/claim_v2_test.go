package outbox

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigClaimV2Off(t *testing.T) {
	if DefaultConfig().ClaimV2 || RecommendedConfig().ClaimV2 {
		t.Fatal("defaults must not enable claim v2")
	}
}

func TestNewRelayClaimV2OffByDefault(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	if r.config.ClaimV2 {
		t.Fatal("claim v2 must be off without config or env")
	}
}

func TestNewRelayClaimV2ByEnv(t *testing.T) {
	cases := []struct {
		value string
		want  bool
	}{
		{
			value: "1",
			want:  true,
		},
		{
			value: "true",
			want:  true,
		},
		{
			value: " TRUE ",
			want:  true,
		},
		{
			value: "0",
			want:  false,
		},
		{
			value: "false",
			want:  false,
		},
		{
			value: "yes",
			want:  false,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.value,
			func(t *testing.T) {
				t.Setenv(
					envClaimV2,
					tc.value,
				)

				r := NewRelay(RelayDeps{
					Store: &OutboxStore{},
					Config: Config{
						Mode: ModeDeleteAfterSend,
					},
				})

				if r.config.ClaimV2 != tc.want {
					t.Fatalf("env %q: claim v2 %v", tc.value, r.config.ClaimV2)
				}
			},
		)
	}
}

func TestNewRelayClaimV2Defaults(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			ClaimV2: true,
		},
	})

	if !r.config.ClaimV2 {
		t.Fatal("explicit claim v2 must survive the empty-mode default")
	}
	if r.config.ClaimTimeout != defaultClaimTimeout {
		t.Fatalf("claim timeout %s", r.config.ClaimTimeout)
	}
	if r.config.StallReclaimInterval != defaultStallReclaimInterval {
		t.Fatalf("reclaim interval %s", r.config.StallReclaimInterval)
	}
}

func TestNewRelayClaimV2KeepsExplicitValues(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode:                 ModeDeleteAfterSend,
			ClaimV2:              true,
			ClaimTimeout:         time.Second,
			StallThreshold:       10 * time.Minute,
			StallReclaimInterval: 3 * time.Minute,
		},
	})

	if r.config.ClaimTimeout != time.Second {
		t.Fatalf("claim timeout %s", r.config.ClaimTimeout)
	}
	if r.config.StallReclaimInterval != 3*time.Minute {
		t.Fatalf("reclaim interval %s", r.config.StallReclaimInterval)
	}
}

func TestNewRelayReclaimIntervalNotAboveStallThreshold(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode:           ModeDeleteAfterSend,
			ClaimV2:        true,
			StallThreshold: 10 * time.Second,
		},
	})

	if r.config.StallReclaimInterval != 10*time.Second {
		t.Fatalf("reclaim interval %s", r.config.StallReclaimInterval)
	}
}

func TestClaimV2QueryShape(t *testing.T) {
	query := strings.Join(
		strings.Fields(claimBatchV2Query),
		" ",
	)

	for _, want := range []string{
		"WITH c AS MATERIALIZED (",
		"state <> 'DONE' AND state = 'PENDING'",
		"ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED",
		"SET state = 'IN_FLIGHT'",
		"FROM c WHERE o.id = c.id",
		"RETURNING o.id, o.topic, o.payload, o.headers, o.attempts, o.created_at",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("claim v2 query misses %q: %s", want, query)
		}
	}

	for _, banned := range []string{
		" OR state",
		"IN_FLIGHT' AND",
		"state = ?",
	} {
		if strings.Contains(query, banned) {
			t.Fatalf("claim v2 query must not contain %q: %s", banned, query)
		}
	}
}

func TestReclaimStalledQueryShape(t *testing.T) {
	query := strings.Join(
		strings.Fields(reclaimStalledQuery),
		" ",
	)

	for _, want := range []string{
		"WITH s AS MATERIALIZED (",
		"state = 'IN_FLIGHT' AND updated_at < ?",
		"FOR UPDATE SKIP LOCKED",
		"SET state = 'PENDING'",
		"FROM s WHERE o.id = s.id",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("reclaim query misses %q: %s", want, query)
		}
	}
}

func TestReclaimDue(t *testing.T) {
	now := time.Date(
		2026,
		10,
		12,
		2,
		0,
		0,
		0,
		time.UTC,
	)

	cases := []struct {
		name string
		last time.Time
		want bool
	}{
		{
			name: "never ran",
			last: time.Time{},
			want: true,
		},
		{
			name: "inside interval",
			last: now.Add(-30 * time.Second),
			want: false,
		},
		{
			name: "interval passed",
			last: now.Add(-time.Minute),
			want: true,
		},
		{
			name: "clock went back",
			last: now.Add(time.Hour),
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got := reclaimDue(
					tc.last,
					now,
					time.Minute,
				)
				if got != tc.want {
					t.Fatalf("reclaimDue = %v, want %v", got, tc.want)
				}
			},
		)
	}
}
