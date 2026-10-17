package durable

import (
	"errors"
	"slices"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestValidateFilters(t *testing.T) {
	cases := []struct {
		name string
		cfg  jetstream.ConsumerConfig
		ok   bool
	}{
		{name: "none", ok: true},
		{name: "single", cfg: jetstream.ConsumerConfig{FilterSubject: "a.b"}, ok: true},
		{name: "wide", cfg: jetstream.ConsumerConfig{FilterSubjects: []string{"a.b", "a.b.>"}}, ok: true},
		{name: "both", cfg: jetstream.ConsumerConfig{FilterSubject: "a.b", FilterSubjects: []string{"a.b.>"}}},
		{name: "empty element", cfg: jetstream.ConsumerConfig{FilterSubjects: []string{"a.b", " "}}},
		{name: "duplicate", cfg: jetstream.ConsumerConfig{FilterSubjects: []string{"a.b", "a.b"}}},
	}

	for _, tc := range cases {
		err := ValidateFilters(tc.cfg)
		if tc.ok != (err == nil) || (err != nil && !errors.Is(err, ErrInvalidFilters)) {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
}

func TestFiltersAreASet(t *testing.T) {
	got := Filters(jetstream.ConsumerConfig{FilterSubject: "b", FilterSubjects: []string{"a", "b"}})
	if !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("filters %v", got)
	}
	if got := Filters(jetstream.ConsumerConfig{}); len(got) != 0 {
		t.Fatalf("filters %v", got)
	}
}
