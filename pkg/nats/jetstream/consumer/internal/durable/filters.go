package durable

import (
	"slices"

	"github.com/nats-io/nats.go/jetstream"
)

func Filters(cfg jetstream.ConsumerConfig) []string {
	filters := make(
		[]string,
		0,
		len(cfg.FilterSubjects)+1,
	)
	if cfg.FilterSubject != "" {
		filters = append(
			filters,
			cfg.FilterSubject,
		)
	}
	filters = append(
		filters,
		cfg.FilterSubjects...,
	)
	slices.Sort(filters)

	return slices.Compact(filters)
}
