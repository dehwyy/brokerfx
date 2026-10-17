package batch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

type rateFetcher struct {
	clock    *fakeClock
	step     time.Duration
	rate     float64
	consumed int
	started  time.Time
	calls    int
}

func (f *rateFetcher) Fetch(count int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	f.calls++
	f.clock.now = f.clock.now.Add(f.step)
	arrived := int(f.clock.now.Sub(f.started).Seconds() * f.rate)
	ready := min(
		arrived-f.consumed,
		count,
	)
	msgs := make(
		[]*fakeMsg,
		0,
		ready,
	)
	for range ready {
		msgs = append(
			msgs,
			newMsg(
				"m",
				1,
			),
		)
	}
	f.consumed += ready

	return batchOf(
		msgs,
		nil,
	), nil
}

func minBatchConsumer(
	f *rateFetcher,
	minBatch int,
	lingerMax time.Duration,
) *Consumer {
	c := testConsumer(
		noopHandler,
		5,
		nil,
	)
	c.max = 64
	c.linger = 5 * time.Millisecond
	c.minBatch = minBatch
	c.lingerMax = lingerMax
	c.fetcher = f
	c.now = f.clock.Now

	return c
}

func newRateFetcher(rate float64) *rateFetcher {
	clock := &fakeClock{
		now: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC),
	}

	return &rateFetcher{
		clock:   clock,
		step:    5 * time.Millisecond,
		rate:    rate,
		started: clock.now,
	}
}

func averageBatch(
	t *testing.T,
	c *Consumer,
	rounds int,
) float64 {
	t.Helper()
	total := 0
	for range rounds {
		msgs, err := c.fetchBatch(
			context.Background(),
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
		total += len(msgs)
	}

	return float64(total) / float64(rounds)
}

func TestMinBatchGrowsBatchAt300PerSecond(t *testing.T) {
	plain := minBatchConsumer(
		newRateFetcher(300),
		0,
		0,
	)
	grown := minBatchConsumer(
		newRateFetcher(300),
		24,
		100*time.Millisecond,
	)

	plainAvg := averageBatch(
		t,
		plain,
		50,
	)
	grownAvg := averageBatch(
		t,
		grown,
		50,
	)

	if plainAvg > 2 {
		t.Fatalf("plain batch %.1f, want about 1.5", plainAvg)
	}
	if grownAvg < 20 {
		t.Fatalf("MinBatch batch %.1f, want at least 20", grownAvg)
	}
}

func TestMinBatchStopsAtLingerMax(t *testing.T) {
	f := newRateFetcher(100)
	c := minBatchConsumer(
		f,
		60,
		20*time.Millisecond,
	)
	started := f.clock.now

	msgs, err := c.fetchBatch(
		context.Background(),
		false,
	)
	if err != nil {
		t.Fatal(err)
	}

	waited := f.clock.now.Sub(started)
	if waited > 25*time.Millisecond {
		t.Fatalf("waited %s, want at most LingerMax plus one step", waited)
	}
	if len(msgs) >= 60 {
		t.Fatalf("got %d messages, want fewer than MinBatch", len(msgs))
	}
}

func TestMinBatchStopsWhenFull(t *testing.T) {
	f := newRateFetcher(100000)
	c := minBatchConsumer(
		f,
		32,
		50*time.Millisecond,
	)

	msgs, err := c.fetchBatch(
		context.Background(),
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 64 || f.calls != 1 {
		t.Fatalf("got %d messages in %d fetches, want 64 in 1", len(msgs), f.calls)
	}
}

func TestMinBatchOffKeepsSingleFetch(t *testing.T) {
	f := newRateFetcher(300)
	c := minBatchConsumer(
		f,
		0,
		0,
	)

	_, err := c.fetchBatch(
		context.Background(),
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 {
		t.Fatalf("got %d fetches, want 1", f.calls)
	}
}

func TestResolveMinBatch(t *testing.T) {
	cases := []struct {
		name          string
		env           map[string]string
		opts          Opts
		wantErr       error
		wantMinBatch  int
		wantLingerMax time.Duration
	}{
		{
			name: "off by default",
			opts: Opts{
				Handler: noopHandler,
			},
		},
		{
			name: "env defaults",
			env: map[string]string{
				envMinBatch:  "8",
				envLingerMax: "40ms",
			},
			opts: Opts{
				Handler: noopHandler,
			},
			wantMinBatch:  8,
			wantLingerMax: 40 * time.Millisecond,
		},
		{
			name: "opts win over env",
			env: map[string]string{
				envMinBatch:  "8",
				envLingerMax: "40ms",
			},
			opts: Opts{
				Handler:   noopHandler,
				MinBatch:  4,
				LingerMax: 10 * time.Millisecond,
			},
			wantMinBatch:  4,
			wantLingerMax: 10 * time.Millisecond,
		},
		{
			name: "min batch without linger max is off",
			opts: Opts{
				Handler:  noopHandler,
				MinBatch: 4,
			},
		},
		{
			name: "min batch above max",
			opts: Opts{
				Handler:   noopHandler,
				Max:       4,
				MinBatch:  8,
				LingerMax: 10 * time.Millisecond,
			},
			wantErr: ErrInvalidOpts,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				for key, value := range tc.env {
					t.Setenv(
						key,
						value,
					)
				}
				s, err := resolve(tc.opts)
				if !errors.Is(
					err,
					tc.wantErr,
				) {
					t.Fatalf("want %v got %v", tc.wantErr, err)
				}
				if s.minBatch != tc.wantMinBatch || s.lingerMax != tc.wantLingerMax {
					t.Fatalf("got %d %s", s.minBatch, s.lingerMax)
				}
			},
		)
	}
}

func TestValidateLingerMaxBelowAckWaitQuarter(t *testing.T) {
	cfg := jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   time.Second,
	}
	s := settings{
		max:         8,
		pipeline:    1,
		maxAttempts: 5,
		minBatch:    4,
		lingerMax:   250 * time.Millisecond,
	}

	_, err := validate(
		cfg,
		s,
	)
	if !errors.Is(
		err,
		ErrInvalidOpts,
	) {
		t.Fatalf("want ErrInvalidOpts got %v", err)
	}

	s.lingerMax = 200 * time.Millisecond
	_, err = validate(
		cfg,
		s,
	)
	if err != nil {
		t.Fatal(err)
	}
}
