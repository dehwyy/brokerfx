package consumeroptsbuilder

import (
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSubjectsOverlap(t *testing.T) {
	cases := []struct {
		first  string
		second string
		want   bool
	}{
		{"balance.command.hold.p0", "balance.command.hold.p1", false},
		{"balance.command.hold.p0", "balance.command.*.p0", true},
		{"balance.command.>", "balance.command.hold.p1", true},
		{"balance.command.hold", "balance.command.hold.p1", false},
		{"balance.command.*", "balance.command.hold.p1", false},
		{"a.>", "a", false},
		{"*.b", "a.*", true},
		{"a.b", "a.b", true},
	}
	for _, tc := range cases {
		if got := subjectsOverlap(tc.first, tc.second); got != tc.want {
			t.Fatalf("%s vs %s: %v", tc.first, tc.second, got)
		}
		if got := subjectsOverlap(tc.second, tc.first); got != tc.want {
			t.Fatalf("%s vs %s: %v", tc.second, tc.first, got)
		}
	}
}

func TestPartitionConfigs(t *testing.T) {
	start := time.Now()
	base := NewDefault().WithName("balance-hold", true).WithFilterSubject("balance.command.hold").WithMaxAckPending(64).WithDeliverPolicy(jetstream.DeliverByStartTimePolicy).Build()
	base.OptStartTime = &start
	base.AckPolicy = jetstream.AckAllPolicy

	configs, err := PartitionConfigs(Partitions{
		Base:      base,
		Count:     2,
		Filters:   func(i int) []string { return []string{"balance.command.hold.p" + strconv.Itoa(i)} },
		Group:     "lanes",
		PinnedTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, cfg := range configs {
		suffix := "-p" + strconv.Itoa(i)
		if cfg.Name != "balance-hold"+suffix || cfg.Durable != "balance-hold"+suffix {
			t.Fatalf("names %q %q", cfg.Name, cfg.Durable)
		}
		if cfg.AckPolicy != jetstream.AckExplicitPolicy || cfg.DeliverPolicy != jetstream.DeliverAllPolicy || cfg.OptStartTime != nil {
			t.Fatalf("policies %v %v %v", cfg.AckPolicy, cfg.DeliverPolicy, cfg.OptStartTime)
		}
		if cfg.FilterSubject != "" || !slices.Equal(cfg.FilterSubjects, []string{"balance.command.hold.p" + strconv.Itoa(i)}) {
			t.Fatalf("filters %q %v", cfg.FilterSubject, cfg.FilterSubjects)
		}
		if cfg.PriorityPolicy != jetstream.PriorityPolicyPinned || !slices.Equal(cfg.PriorityGroups, []string{"lanes"}) || cfg.PinnedTTL != time.Minute || cfg.MaxAckPending != 64 {
			t.Fatalf("pinned %v %v %v %d", cfg.PriorityPolicy, cfg.PriorityGroups, cfg.PinnedTTL, cfg.MaxAckPending)
		}
	}
	if base.Name != "balance-hold" || base.FilterSubject != "balance.command.hold" {
		t.Fatal("base config changed")
	}
}

func TestPartitionConfigsRejects(t *testing.T) {
	base := NewDefault().WithName("lane", true).Build()
	cases := []struct {
		name       string
		partitions Partitions
	}{
		{name: "no count", partitions: Partitions{Base: base, Filters: func(int) []string { return []string{"a"} }, Group: "g"}},
		{name: "no name", partitions: Partitions{Count: 1, Filters: func(int) []string { return []string{"a"} }, Group: "g"}},
		{name: "no filters", partitions: Partitions{Base: base, Count: 1, Filters: func(int) []string { return nil }, Group: "g"}},
		{name: "no group", partitions: Partitions{Base: base, Count: 1, Filters: func(int) []string { return []string{"a"} }}},
		{name: "overlap", partitions: Partitions{Base: base, Count: 2, Filters: func(i int) []string { return [][]string{{"a.0"}, {"a.*"}}[i] }, Group: "g"}},
		{name: "same filter", partitions: Partitions{Base: base, Count: 2, Filters: func(int) []string { return []string{"a"} }, Group: "g"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := PartitionConfigs(tc.partitions); !errors.Is(err, ErrInvalidPartitions) {
				t.Fatalf("err %v", err)
			}
		})
	}
}

func TestWithPinnedGroupAndFromConfig(t *testing.T) {
	cfg := NewDefault().WithName("lane", true).WithPinnedGroup("g", time.Minute).Build()
	if cfg.PriorityPolicy != jetstream.PriorityPolicyPinned || !slices.Equal(cfg.PriorityGroups, []string{"g"}) || cfg.PinnedTTL != time.Minute {
		t.Fatalf("pinned %v %v %v", cfg.PriorityPolicy, cfg.PriorityGroups, cfg.PinnedTTL)
	}
	if again := FromConfig(cfg).Build(); again.Name != "lane" || again.PinnedTTL != time.Minute {
		t.Fatalf("from config %v", again)
	}
}
