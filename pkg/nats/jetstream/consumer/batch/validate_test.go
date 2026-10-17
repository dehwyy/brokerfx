package batch

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestValidate(t *testing.T) {
	s := settings{
		max:         16,
		pipeline:    2,
		maxAttempts: 6,
	}

	cases := []struct {
		name        string
		cfg         jetstream.ConsumerConfig
		attempts    int
		wantErr     error
		wantPending int
	}{
		{name: "below in flight", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: 8}, wantErr: ErrMaxAckPending},
		{name: "unset gets pipeline*max", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy}, wantPending: 32},
		{name: "unlimited", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: -1}, wantPending: -1},
		{name: "exact", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: 32}, wantPending: 32},
		{name: "ack none", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy}, wantErr: ErrInvalidOpts},
		{name: "ack all", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckAllPolicy}, wantErr: ErrInvalidOpts},
		{name: "max deliver above attempts", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 7}, wantPending: 32},
		{name: "max deliver equals attempts", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 6}, wantErr: ErrMaxDeliver},
		{name: "max deliver unlimited", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: -1}, wantPending: 32},
		{name: "unlimited attempts need unlimited deliver", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 100}, attempts: -1, wantErr: ErrMaxDeliver},
		{name: "both filter fields", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: "a", FilterSubjects: []string{"b"}}, wantErr: ErrInvalidOpts},
		{name: "duplicate filters", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, FilterSubjects: []string{"b", "b"}}, wantErr: ErrInvalidOpts},
		{name: "wide filters", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, FilterSubjects: []string{"b", "b.>"}}, wantPending: 32},
		{name: "unlimited attempts and deliver", cfg: jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: -1}, attempts: -1, wantPending: 32},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			current := s
			if tc.attempts != 0 {
				current.maxAttempts = tc.attempts
			}

			cfg, err := validate(tc.cfg, current)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected %v", err)
			}
			if cfg.MaxAckPending != tc.wantPending {
				t.Fatalf("max ack pending %d", cfg.MaxAckPending)
			}
		})
	}
}
