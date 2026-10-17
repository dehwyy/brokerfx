package consumeroptsbuilder

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/durable"
	"github.com/nats-io/nats.go/jetstream"
)

func PartitionConfigs(partitions Partitions) ([]jetstream.ConsumerConfig, error) {
	if partitions.Count < 1 || partitions.Filters == nil {
		return nil, fmt.Errorf(
			"%w: Count %d and Filters are required",
			ErrInvalidPartitions,
			partitions.Count,
		)
	}
	if durable.Name(partitions.Base) == "" {
		return nil, fmt.Errorf(
			"%w: Base has no Name or Durable",
			ErrInvalidPartitions,
		)
	}

	configs := make(
		[]jetstream.ConsumerConfig,
		0,
		partitions.Count,
	)
	owners := make(map[string]int)
	for partition := range partitions.Count {
		cfg := partitionConfig(
			partitions,
			partition,
		)
		if len(cfg.FilterSubjects) == 0 {
			return nil, fmt.Errorf(
				"%w: partition %d has no filters",
				ErrInvalidPartitions,
				partition,
			)
		}
		if err := durable.ValidateFilters(cfg); err != nil {
			return nil, fmt.Errorf(
				"%w: partition %d: %w",
				ErrInvalidPartitions,
				partition,
				err,
			)
		}
		if err := durable.ValidatePinned(
			cfg,
			0,
		); err != nil {
			return nil, fmt.Errorf(
				"%w: partition %d: %w",
				ErrInvalidPartitions,
				partition,
				err,
			)
		}
		if err := checkOverlap(
			owners,
			cfg.FilterSubjects,
			partition,
		); err != nil {
			return nil, err
		}
		configs = append(
			configs,
			cfg,
		)
	}

	return configs, nil
}

func partitionConfig(
	partitions Partitions,
	partition int,
) jetstream.ConsumerConfig {
	cfg := partitions.Base
	suffix := "-p" + strconv.Itoa(partition)
	if cfg.Name != "" {
		cfg.Name += suffix
	}
	if cfg.Durable != "" {
		cfg.Durable += suffix
	}
	cfg.AckPolicy = jetstream.AckExplicitPolicy
	cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	cfg.OptStartSeq = 0
	cfg.OptStartTime = nil
	cfg.FilterSubject = ""
	cfg.FilterSubjects = slices.Clone(partitions.Filters(partition))
	cfg.PriorityPolicy = jetstream.PriorityPolicyPinned
	cfg.PriorityGroups = []string{partitions.Group}
	cfg.PinnedTTL = partitions.PinnedTTL

	return cfg
}

func checkOverlap(
	owners map[string]int,
	filters []string,
	partition int,
) error {
	for _, filter := range filters {
		for owned, owner := range owners {
			if !subjectsOverlap(
				filter,
				owned,
			) {
				continue
			}

			return fmt.Errorf(
				"%w: filter %q of partition %d overlaps %q of partition %d",
				ErrInvalidPartitions,
				filter,
				partition,
				owned,
				owner,
			)
		}
		owners[filter] = partition
	}

	return nil
}
