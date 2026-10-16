package timedactor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestTryFireCallbackRetriesUnexpectedDeleteError(t *testing.T) {
	cases := []struct {
		name         string
		rescan       time.Duration
		stopped      bool
		wantAttempts int32
		wantFired    int32
	}{
		{
			name:         "rescan interval set retries after check interval",
			rescan:       time.Hour,
			wantAttempts: 2,
			wantFired:    1,
		},
		{
			name:         "rescan interval zero keeps the old skip",
			wantAttempts: 1,
			wantFired:    0,
		},
		{
			name:         "stopped actor does not retry",
			rescan:       time.Hour,
			stopped:      true,
			wantAttempts: 1,
			wantFired:    0,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				var attempts atomic.Int32
				kv := &mockKV{
					deleteFn: func(context.Context, string, ...jetstream.KVDeleteOpt) error {
						if attempts.Add(1) == 1 {
							return errors.New("nats: timeout")
						}
						return nil
					},
				}
				actor, cancel := newTestActor(kv)
				defer cancel()
				actor.config.CheckInterval = 50 * time.Millisecond
				actor.config.RescanInterval = tc.rescan

				var fired atomic.Int32
				actor.subscribers = append(
					actor.subscribers,
					subscriber[testMeta]{
						Callback: func(context.Context, string, testMeta) {
							fired.Add(1)
						},
					},
				)
				if tc.stopped {
					cancel()
				}

				actor.tryFireCallback(
					"order.1",
					3,
					testMeta{},
				)
				time.Sleep(300 * time.Millisecond)

				if got := attempts.Load(); got != tc.wantAttempts {
					t.Fatalf(
						"delete attempts = %d, want %d",
						got,
						tc.wantAttempts,
					)
				}
				if got := fired.Load(); got != tc.wantFired {
					t.Fatalf(
						"callbacks = %d, want %d",
						got,
						tc.wantFired,
					)
				}
			},
		)
	}
}

func TestRetryLaterKeepsNewerTimer(t *testing.T) {
	var attempts atomic.Int32
	kv := &mockKV{
		deleteFn: func(context.Context, string, ...jetstream.KVDeleteOpt) error {
			attempts.Add(1)
			return nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	actor.config.CheckInterval = 50 * time.Millisecond
	actor.config.RescanInterval = time.Hour

	actor.scheduleTimer(
		"order.1",
		9,
		time.Hour,
		testMeta{},
	)
	actor.retryLater(
		"order.1",
		3,
		testMeta{},
	)
	time.Sleep(200 * time.Millisecond)

	if got := attempts.Load(); got != 0 {
		t.Fatalf(
			"delete attempts = %d, want 0",
			got,
		)
	}
	actor.stopTimer("order.1")
}
