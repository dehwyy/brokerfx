package batch

import (
	"sort"

	"github.com/nats-io/nats.go/jetstream"
)

type orderViolation struct {
	blocked []int
}

func (v *orderViolation) Error() string {
	return ErrOrderBlocked.Error()
}

func groupsOf(
	msgs []jetstream.Msg,
	subset []int,
	orderKey KeyFunc,
) [][]int {
	positions := make(map[string]int)
	var groups [][]int
	for _, index := range subset {
		key := orderKey(msgs[index])
		position, ok := positions[key]
		if !ok {
			position = len(groups)
			positions[key] = position
			groups = append(
				groups,
				nil,
			)
		}
		groups[position] = append(
			groups[position],
			index,
		)
	}

	return groups
}

func flattenGroups(groups [][]int) []int {
	var subset []int
	for _, group := range groups {
		subset = append(
			subset,
			group...,
		)
	}
	sort.Ints(subset)

	return subset
}

func withoutIndexes(groups [][]int, removed []int) [][]int {
	drop := make(
		map[int]struct{},
		len(removed),
	)
	for _, index := range removed {
		drop[index] = struct{}{}
	}

	kept := make(
		[][]int,
		0,
		len(groups),
	)
	for _, group := range groups {
		var rest []int
		for _, index := range group {
			if _, ok := drop[index]; !ok {
				rest = append(
					rest,
					index,
				)
			}
		}
		if len(rest) > 0 {
			kept = append(
				kept,
				rest,
			)
		}
	}

	return kept
}

func orderViolations(
	msgs []jetstream.Msg,
	indexes []int,
	applied []Result,
	orderKey KeyFunc,
) []int {
	firstFailure := make(map[string]int)
	violated := make(map[string]bool)
	for i, index := range indexes {
		key := orderKey(msgs[index])
		if _, failed := firstFailure[key]; failed {
			if settlesAsAck(applied[i].Verdict) {
				violated[key] = true
			}
			continue
		}
		if !settlesAsAck(applied[i].Verdict) {
			firstFailure[key] = i
		}
	}

	var blocked []int
	for i, index := range indexes {
		key := orderKey(msgs[index])
		if !violated[key] {
			continue
		}
		if i > firstFailure[key] {
			blocked = append(
				blocked,
				index,
			)
		}
	}

	return blocked
}

func (h *inTx) block(indexes []int, results []Result) {
	for _, index := range indexes {
		results[index] = NakDelay(
			h.transientDelay,
			ErrOrderBlocked,
		)
	}
}

func failedIn(group []int, outcome map[int]Result) bool {
	for _, index := range group {
		if result, ok := outcome[index]; ok && !settlesAsAck(result.Verdict) {
			return true
		}
	}

	return false
}
