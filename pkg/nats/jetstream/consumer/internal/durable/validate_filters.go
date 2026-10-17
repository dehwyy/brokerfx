package durable

import (
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

func ValidateFilters(cfg jetstream.ConsumerConfig) error {
	if cfg.FilterSubject != "" && len(cfg.FilterSubjects) > 0 {
		return fmt.Errorf(
			"%w: FilterSubject %q and FilterSubjects %v are both set",
			ErrInvalidFilters,
			cfg.FilterSubject,
			cfg.FilterSubjects,
		)
	}

	seen := make(
		map[string]struct{},
		len(cfg.FilterSubjects),
	)
	for i, filter := range cfg.FilterSubjects {
		if strings.TrimSpace(filter) == "" {
			return fmt.Errorf(
				"%w: FilterSubjects[%d] is empty",
				ErrInvalidFilters,
				i,
			)
		}
		if _, dup := seen[filter]; dup {
			return fmt.Errorf(
				"%w: FilterSubjects has %q twice",
				ErrInvalidFilters,
				filter,
			)
		}
		seen[filter] = struct{}{}
	}

	return nil
}
