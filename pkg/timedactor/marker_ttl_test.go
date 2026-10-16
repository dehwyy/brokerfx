package timedactor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type mockStream struct {
	jetstream.Stream
	info *jetstream.StreamInfo
}

func (s *mockStream) CachedInfo() *jetstream.StreamInfo {
	return s.info
}

func TestNewValidatesMarkerTTLAndRescan(t *testing.T) {
	cases := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "zero keeps old behaviour",
			config: Config{
				BucketName:    "b",
				CheckInterval: time.Second,
			},
		},
		{
			name: "one second is the minimum",
			config: Config{
				BucketName:    "b",
				CheckInterval: time.Second,
				MarkerTTL:     time.Second,
			},
		},
		{
			name: "below one second is rejected",
			config: Config{
				BucketName:    "b",
				CheckInterval: time.Second,
				MarkerTTL:     500 * time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "negative marker ttl is rejected",
			config: Config{
				BucketName:    "b",
				CheckInterval: time.Second,
				MarkerTTL:     -time.Second,
			},
			wantErr: true,
		},
		{
			name: "rescan without check interval is rejected",
			config: Config{
				BucketName:     "b",
				RescanInterval: time.Minute,
			},
			wantErr: true,
		},
		{
			name: "negative rescan is rejected",
			config: Config{
				BucketName:     "b",
				CheckInterval:  time.Second,
				RescanInterval: -time.Second,
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				js := &mockJetStream{
					createOrUpdateKVFn: func(context.Context, jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
						return &mockKV{}, nil
					},
				}

				_, err := New[testMeta](Deps{
					JS:     js,
					Config: tc.config,
				})
				if tc.wantErr {
					if !errors.Is(
						err,
						ErrInvalidConfig,
					) {
						t.Fatalf(
							"err = %v, want ErrInvalidConfig",
							err,
						)
					}
					return
				}
				if err != nil {
					t.Fatalf(
						"unexpected error: %v",
						err,
					)
				}
			},
		)
	}
}

func TestNewLimitMarkerTTL(t *testing.T) {
	cases := []struct {
		name       string
		markerTTL  time.Duration
		existing   time.Duration
		streamErr  error
		wantTTL    time.Duration
		wantErr    bool
		wantLookup bool
	}{
		{
			name:      "explicit ttl enables markers",
			markerTTL: 5 * time.Minute,
			wantTTL:   5 * time.Minute,
		},
		{
			name:       "zero on a new bucket stays off",
			streamErr:  jetstream.ErrStreamNotFound,
			wantLookup: true,
		},
		{
			name:       "zero keeps the ttl of an existing bucket",
			existing:   5 * time.Minute,
			wantTTL:    5 * time.Minute,
			wantLookup: true,
		},
		{
			name:       "zero on an existing bucket without ttl stays off",
			wantLookup: true,
		},
		{
			name:       "stream lookup error fails New",
			streamErr:  errors.New("nats: timeout"),
			wantErr:    true,
			wantLookup: true,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				var gotTTL time.Duration
				var lookedUp atomic.Bool
				js := &mockJetStream{
					createOrUpdateKVFn: func(_ context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
						gotTTL = cfg.LimitMarkerTTL
						return &mockKV{}, nil
					},
					streamFn: func(_ context.Context, name string) (jetstream.Stream, error) {
						lookedUp.Store(true)
						if name != "KV_timers" {
							t.Errorf(
								"stream name = %q, want KV_timers",
								name,
							)
						}
						if tc.streamErr != nil {
							return nil, tc.streamErr
						}
						return &mockStream{
							info: &jetstream.StreamInfo{
								Config: jetstream.StreamConfig{
									SubjectDeleteMarkerTTL: tc.existing,
								},
							},
						}, nil
					},
				}

				_, err := New[testMeta](Deps{
					JS: js,
					Config: Config{
						BucketName:    "timers",
						CheckInterval: time.Second,
						MarkerTTL:     tc.markerTTL,
					},
				})
				if tc.wantErr {
					if err == nil {
						t.Fatal("expected error")
					}
					return
				}
				if err != nil {
					t.Fatalf(
						"unexpected error: %v",
						err,
					)
				}
				if gotTTL != tc.wantTTL {
					t.Errorf(
						"LimitMarkerTTL = %s, want %s",
						gotTTL,
						tc.wantTTL,
					)
				}
				if lookedUp.Load() != tc.wantLookup {
					t.Errorf(
						"stream lookup = %v, want %v",
						lookedUp.Load(),
						tc.wantLookup,
					)
				}
			},
		)
	}
}

func TestRemoveKeyPurgesWithTTLWhenEnabled(t *testing.T) {
	var purged, deleted atomic.Int32
	kv := &mockKV{
		purgeFn: func(context.Context, string, ...jetstream.KVDeleteOpt) error {
			purged.Add(1)
			return nil
		},
		deleteFn: func(context.Context, string, ...jetstream.KVDeleteOpt) error {
			deleted.Add(1)
			return nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	actor.config.MarkerTTL = 5 * time.Minute

	var fired atomic.Int32
	actor.subscribers = append(
		actor.subscribers,
		subscriber[testMeta]{
			Callback: func(context.Context, string, testMeta) {
				fired.Add(1)
			},
		},
	)

	actor.tryFireCallback(
		"order.1",
		7,
		testMeta{},
	)
	if err := actor.Clear(
		context.Background(),
		"order.2",
	); err != nil {
		t.Fatal(err)
	}

	if purged.Load() != 2 || deleted.Load() != 0 {
		t.Fatalf(
			"purged = %d, deleted = %d, want 2 and 0",
			purged.Load(),
			deleted.Load(),
		)
	}
	if fired.Load() != 1 {
		t.Fatalf(
			"callbacks = %d, want 1",
			fired.Load(),
		)
	}
}

func TestRemoveKeyPurgeLosesRaceSkipsCallback(t *testing.T) {
	kv := &mockKV{
		purgeFn: func(context.Context, string, ...jetstream.KVDeleteOpt) error {
			return &jetstream.APIError{
				Code:        400,
				ErrorCode:   10071,
				Description: "wrong last sequence",
			}
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	actor.config.MarkerTTL = 5 * time.Minute

	var fired atomic.Int32
	actor.subscribers = append(
		actor.subscribers,
		subscriber[testMeta]{
			Callback: func(context.Context, string, testMeta) {
				fired.Add(1)
			},
		},
	)

	actor.tryFireCallback(
		"order.1",
		7,
		testMeta{},
	)
	if fired.Load() != 0 {
		t.Fatalf(
			"callbacks = %d, want 0",
			fired.Load(),
		)
	}
}

func TestRescanEvery(t *testing.T) {
	cases := []struct {
		name   string
		check  time.Duration
		rescan time.Duration
		want   time.Duration
	}{
		{
			name:  "zero rescans on every check",
			check: 15 * time.Second,
			want:  15 * time.Second,
		},
		{
			name:   "rescan interval wins when set",
			check:  15 * time.Second,
			rescan: 5 * time.Minute,
			want:   5 * time.Minute,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				actor := &TimedActor[testMeta]{
					config: Config{
						CheckInterval:  tc.check,
						RescanInterval: tc.rescan,
					},
				}
				if got := actor.rescanEvery(); got != tc.want {
					t.Fatalf(
						"rescanEvery = %s, want %s",
						got,
						tc.want,
					)
				}
			},
		)
	}
}

func TestRescanRunsAfterInitialValuesWhenIntervalSet(t *testing.T) {
	watchCh := make(
		chan jetstream.KeyValueEntry,
		1,
	)
	var listed atomic.Int32
	kv := &mockKV{
		watchFn: func(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return &mockWatcher{
				ch: watchCh,
			}, nil
		},
		listKeysFn: func(context.Context, ...jetstream.WatchOpt) (jetstream.KeyLister, error) {
			listed.Add(1)
			return closedKeyLister(), nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	actor.config.RescanInterval = time.Hour

	actor.Subscribe(
		context.Background(),
		">",
		nil,
		func(context.Context, string, testMeta) {},
	)
	watchCh <- nil

	deadline := time.Now().Add(2 * time.Second)
	for listed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	actor.Stop()

	if listed.Load() != 1 {
		t.Fatalf(
			"rescans = %d, want 1",
			listed.Load(),
		)
	}
}
