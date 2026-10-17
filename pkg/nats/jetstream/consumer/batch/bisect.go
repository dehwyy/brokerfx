package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func (h *inTx) bisect(
	ctx context.Context,
	msgs []jetstream.Msg,
	subset []int,
	results []Result,
	budget *int,
) {
	for {
		outcome, err := h.attempt(
			ctx,
			msgs,
			subset,
		)
		if err == nil {
			h.record(
				subset,
				outcome,
				results,
			)
			return
		}

		if h.transient(err) {
			if *budget > 0 {
				*budget--
				continue
			}
			for _, index := range subset {
				results[index] = h.transientResult(err)
			}
			return
		}

		if len(subset) == 1 {
			results[subset[0]] = Nak(err)
			return
		}

		h.observer.OnBisect(BisectEvent{
			Messages: len(subset),
			Groups:   1,
			Err:      err,
		})

		middle := len(subset) / 2
		h.bisect(
			ctx,
			msgs,
			subset[:middle],
			results,
			budget,
		)
		h.bisect(
			ctx,
			msgs,
			subset[middle:],
			results,
			budget,
		)
		return
	}
}

func (h *inTx) record(
	subset []int,
	outcome map[int]Result,
	results []Result,
) {
	for _, index := range subset {
		if result, ok := outcome[index]; ok {
			results[index] = result
			continue
		}
		results[index] = Ack()
	}
}

func (h *inTx) attempt(
	ctx context.Context,
	msgs []jetstream.Msg,
	subset []int,
) (map[int]Result, error) {
	var outcome map[int]Result
	started := time.Now()
	fresh := 0

	err := h.tx.Do(
		ctx,
		h.txName,
		func(ctx context.Context) error {
			outcome = nil

			indexes, keys, err := h.fresh(
				ctx,
				msgs,
				subset,
			)
			if err != nil {
				return err
			}
			fresh = len(indexes)
			if len(indexes) == 0 {
				return nil
			}

			selected := make(
				[]jetstream.Msg,
				len(indexes),
			)
			for i, index := range indexes {
				selected[i] = msgs[index]
			}

			if h.applyResults == nil {
				return h.apply(
					ctx,
					selected,
				)
			}

			applied, err := h.applyResults(
				ctx,
				selected,
			)
			if err != nil {
				return err
			}
			if len(applied) != len(selected) {
				return fmt.Errorf(
					"%w: ApplyResults returned %d results for %d messages",
					ErrMissingResult,
					len(applied),
					len(selected),
				)
			}

			byIndex, err := h.verdicts(
				ctx,
				msgs,
				indexes,
				keys,
				applied,
			)
			if err != nil {
				return err
			}
			outcome = byIndex

			return nil
		},
	)

	h.observer.OnApply(ApplyEvent{
		Messages: len(subset),
		Fresh:    fresh,
		Duration: time.Since(started),
		Err:      err,
	})

	if err != nil {
		return nil, err
	}

	return outcome, nil
}

func (h *inTx) verdicts(
	ctx context.Context,
	msgs []jetstream.Msg,
	indexes []int,
	keys []string,
	applied []Result,
) (map[int]Result, error) {
	byIndex := make(
		map[int]Result,
		len(indexes),
	)
	var released []string
	for i, index := range indexes {
		byIndex[index] = applied[i]
		if settlesAsAck(applied[i].Verdict) {
			continue
		}
		if keys != nil {
			released = append(
				released,
				keys[i],
			)
		}
	}

	if len(released) > 0 {
		if err := unclaimKeys(
			h.tx.GetConnection(ctx),
			released,
		); err != nil {
			return nil, err
		}
	}

	return byIndex, nil
}

func settlesAsAck(verdict Verdict) bool {
	return verdict == VerdictAck || verdict == VerdictSkip
}

func (h *inTx) fresh(
	ctx context.Context,
	msgs []jetstream.Msg,
	subset []int,
) ([]int, []string, error) {
	if h.noDedup {
		return append(
			[]int(nil),
			subset...,
		), nil, nil
	}

	selected := make(
		[]int,
		0,
		len(subset),
	)
	keys := make(
		[]string,
		0,
		len(subset),
	)
	seen := make(
		map[string]struct{},
		len(subset),
	)
	for _, index := range subset {
		key := h.key(msgs[index])
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		keys = append(
			keys,
			key,
		)
		selected = append(
			selected,
			index,
		)
	}

	selectedMsgs := make(
		[]jetstream.Msg,
		len(selected),
	)
	for i, index := range selected {
		selectedMsgs[i] = msgs[index]
	}

	claimed, err := claimKeys(
		h.tx.GetConnection(ctx),
		claimRows(
			keys,
			selectedMsgs,
		),
	)
	if err != nil {
		return nil, nil, err
	}

	freshIndexes := selected[:0]
	freshKeys := make(
		[]string,
		0,
		len(selected),
	)
	for i, index := range selected {
		if _, ok := claimed[keys[i]]; ok {
			freshIndexes = append(
				freshIndexes,
				index,
			)
			freshKeys = append(
				freshKeys,
				keys[i],
			)
		}
	}

	return freshIndexes, freshKeys, nil
}
