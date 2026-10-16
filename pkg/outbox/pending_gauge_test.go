package outbox

import (
	"testing"
	"time"
)

type statsOnlyObserver struct{}

func (statsOnlyObserver) OnPublished(int)                 {}
func (statsOnlyObserver) OnFailed(int)                    {}
func (statsOnlyObserver) OnParked(string, string, string) {}
func (statsOnlyObserver) OnStats(Stats)                   {}

type pendingObserver struct {
	statsOnlyObserver
}

func (pendingObserver) OnPending(PendingStats) {}

func TestPendingGaugeIntervalFromEnv(t *testing.T) {
	cases := []struct {
		value string
		want  time.Duration
	}{
		{
			value: "",
			want:  0,
		},
		{
			value: "15s",
			want:  15 * time.Second,
		},
		{
			value: "0",
			want:  0,
		},
		{
			value: "-1s",
			want:  0,
		},
		{
			value: "soon",
			want:  0,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.value,
			func(t *testing.T) {
				t.Setenv(
					envPendingGaugeInterval,
					tc.value,
				)
				r := NewRelay(RelayDeps{
					Store: &OutboxStore{},
				})
				if r.config.PendingGaugeInterval != tc.want {
					t.Fatalf("want %s got %s", tc.want, r.config.PendingGaugeInterval)
				}
			},
		)
	}
}

func TestPendingGaugeKeptWhenModeEmpty(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			PendingGaugeInterval: time.Minute,
		},
	})
	if r.config.PendingGaugeInterval != time.Minute {
		t.Fatalf("interval %s", r.config.PendingGaugeInterval)
	}
}

func TestPendingGaugeTickerNeedsPendingObserver(t *testing.T) {
	cases := []struct {
		name     string
		observer Observer
		interval time.Duration
		want     bool
	}{
		{
			name:     "off by default",
			observer: pendingObserver{},
			want:     false,
		},
		{
			name:     "observer without OnPending",
			observer: statsOnlyObserver{},
			interval: time.Second,
			want:     false,
		},
		{
			name:     "no observer",
			interval: time.Second,
			want:     false,
		},
		{
			name:     "pending observer",
			observer: pendingObserver{},
			interval: time.Second,
			want:     true,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				r := NewRelay(RelayDeps{
					Store:    &OutboxStore{},
					Observer: tc.observer,
					Config: Config{
						Mode:                 ModeDeleteAfterSend,
						PendingGaugeInterval: tc.interval,
					},
				})
				ticks, stop := r.pendingGaugeTicker()
				defer stop()
				if (ticks != nil) != tc.want {
					t.Fatalf("ticker %v, want %v", ticks != nil, tc.want)
				}
			},
		)
	}
}
