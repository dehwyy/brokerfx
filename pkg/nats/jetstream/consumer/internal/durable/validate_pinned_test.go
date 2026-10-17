package durable

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestValidatePinned(t *testing.T) {
	pinned := func(ttl time.Duration, groups ...string) jetstream.ConsumerConfig {
		return jetstream.ConsumerConfig{PriorityPolicy: jetstream.PriorityPolicyPinned, PriorityGroups: groups, PinnedTTL: ttl}
	}
	cases := []struct {
		name    string
		cfg     jetstream.ConsumerConfig
		min     time.Duration
		wantErr bool
	}{
		{name: "not pinned", cfg: jetstream.ConsumerConfig{}, min: time.Hour},
		{name: "one group above min", cfg: pinned(20*time.Second, "lane"), min: 10 * time.Second},
		{name: "ttl equals min", cfg: pinned(10*time.Second, "lane"), min: 10 * time.Second, wantErr: true},
		{name: "default ttl", cfg: pinned(0, "lane"), min: time.Minute},
		{name: "default ttl below min", cfg: pinned(0, "lane"), min: 3 * time.Minute, wantErr: true},
		{name: "no group", cfg: pinned(time.Minute), wantErr: true},
		{name: "two groups", cfg: pinned(time.Minute, "a", "b"), wantErr: true},
		{name: "long group", cfg: pinned(time.Minute, "a-group-name-of-17"), wantErr: true},
		{name: "bad group", cfg: pinned(time.Minute, "a.b"), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePinned(tc.cfg, tc.min)
			if tc.wantErr != (err != nil) || (err != nil && !errors.Is(err, ErrInvalidPinned)) {
				t.Fatalf("err %v, want error %v", err, tc.wantErr)
			}
			if group := PinnedGroup(tc.cfg); !tc.wantErr && tc.cfg.PriorityPolicy == jetstream.PriorityPolicyPinned && group != "lane" {
				t.Fatalf("group %q", group)
			}
		})
	}
}
