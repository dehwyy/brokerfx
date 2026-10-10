package batch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func noopHandler(_ context.Context, msgs []jetstream.Msg) []Result {
	return make([]Result, len(msgs))
}

func TestResolve(t *testing.T) {
	cases := []struct {
		name         string
		env          map[string]string
		opts         Opts
		wantErr      error
		wantMax      int
		wantLinger   time.Duration
		wantPipeline int
		wantDelays   []time.Duration
		wantAttempts int
	}{
		{
			name:         "defaults",
			opts:         Opts{Handler: noopHandler},
			wantMax:      16,
			wantLinger:   5 * time.Millisecond,
			wantPipeline: 1,
			wantDelays:   DefaultDelays,
			wantAttempts: 6,
		},
		{
			name:         "env max",
			env:          map[string]string{envMax: "8"},
			opts:         Opts{Handler: noopHandler},
			wantMax:      8,
			wantLinger:   5 * time.Millisecond,
			wantPipeline: 1,
			wantDelays:   DefaultDelays,
			wantAttempts: 6,
		},
		{
			name:         "opts win over env",
			env:          map[string]string{envMax: "8", envLinger: "20ms", envPipeline: "3"},
			opts:         Opts{Handler: noopHandler, Max: 4, Linger: time.Millisecond, Pipeline: 2},
			wantMax:      4,
			wantLinger:   time.Millisecond,
			wantPipeline: 2,
			wantDelays:   DefaultDelays,
			wantAttempts: 6,
		},
		{
			name:         "env linger and pipeline",
			env:          map[string]string{envLinger: "20ms", envPipeline: "3"},
			opts:         Opts{Handler: noopHandler},
			wantMax:      16,
			wantLinger:   20 * time.Millisecond,
			wantPipeline: 3,
			wantDelays:   DefaultDelays,
			wantAttempts: 6,
		},
		{
			name:         "custom delays and attempts",
			opts:         Opts{Handler: noopHandler, Delays: []time.Duration{time.Millisecond}, MaxAttempts: 10},
			wantMax:      16,
			wantLinger:   5 * time.Millisecond,
			wantPipeline: 1,
			wantDelays:   []time.Duration{time.Millisecond},
			wantAttempts: 10,
		},
		{
			name:         "unlimited attempts",
			opts:         Opts{Handler: noopHandler, MaxAttempts: -1},
			wantMax:      16,
			wantLinger:   5 * time.Millisecond,
			wantPipeline: 1,
			wantDelays:   DefaultDelays,
			wantAttempts: -1,
		},
		{name: "broken env max", env: map[string]string{envMax: "x"}, opts: Opts{Handler: noopHandler}, wantErr: ErrInvalidEnv},
		{name: "broken env linger", env: map[string]string{envLinger: "x"}, opts: Opts{Handler: noopHandler}, wantErr: ErrInvalidEnv},
		{name: "env max out of range", env: map[string]string{envMax: "300"}, opts: Opts{Handler: noopHandler}, wantErr: ErrInvalidOpts},
		{name: "pipeline out of range", opts: Opts{Handler: noopHandler, Pipeline: 9}, wantErr: ErrInvalidOpts},
		{name: "linger out of range", opts: Opts{Handler: noopHandler, Linger: 2 * time.Second}, wantErr: ErrInvalidOpts},
		{name: "negative max", opts: Opts{Handler: noopHandler, Max: -1}, wantErr: ErrInvalidOpts},
		{name: "nil handler", opts: Opts{}, wantErr: ErrInvalidOpts},
		{name: "zero delay", opts: Opts{Handler: noopHandler, Delays: []time.Duration{time.Second, 0}}, wantErr: ErrInvalidOpts},
		{name: "negative delay", opts: Opts{Handler: noopHandler, Delays: []time.Duration{-time.Second}}, wantErr: ErrInvalidOpts},
		{name: "bad attempts", opts: Opts{Handler: noopHandler, MaxAttempts: -2}, wantErr: ErrInvalidOpts},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range []string{envMax, envLinger, envPipeline} {
				t.Setenv(name, tc.env[name])
			}

			s, err := resolve(tc.opts)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected %v", err)
			}
			if s.max != tc.wantMax || s.linger != tc.wantLinger || s.pipeline != tc.wantPipeline || s.maxAttempts != tc.wantAttempts {
				t.Fatalf("got %+v", s)
			}
			if len(s.delays) != len(tc.wantDelays) {
				t.Fatalf("delays %v", s.delays)
			}
			for i := range s.delays {
				if s.delays[i] != tc.wantDelays[i] {
					t.Fatalf("delays %v", s.delays)
				}
			}
		})
	}
}
