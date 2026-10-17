package batch

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

func (h *inTx) bisectGroups(
	ctx context.Context,
	msgs []jetstream.Msg,
	groups [][]int,
	results []Result,
	budget *int,
) {
	for {
		subset := flattenGroups(groups)
		if len(subset) == 0 {
			return
		}

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

		var violation *orderViolation
		if errors.As(
			err,
			&violation,
		) {
			h.block(
				violation.blocked,
				results,
			)
			groups = withoutIndexes(
				groups,
				violation.blocked,
			)
			continue
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

		if len(groups) == 1 {
			h.ordered(
				ctx,
				msgs,
				groups[0],
				err,
				results,
				budget,
			)
			return
		}

		h.observer.OnBisect(BisectEvent{
			Messages: len(subset),
			Groups:   len(groups),
			Err:      err,
		})

		middle := len(groups) / 2
		h.bisectGroups(
			ctx,
			msgs,
			groups[:middle],
			results,
			budget,
		)
		h.bisectGroups(
			ctx,
			msgs,
			groups[middle:],
			results,
			budget,
		)
		return
	}
}

func (h *inTx) ordered(
	ctx context.Context,
	msgs []jetstream.Msg,
	group []int,
	known error,
	results []Result,
	budget *int,
) bool {
	err := known
	failed := false
	for err == nil {
		if len(group) == 0 {
			return failed
		}

		outcome, attemptErr := h.attempt(
			ctx,
			msgs,
			group,
		)
		if attemptErr == nil {
			h.record(
				group,
				outcome,
				results,
			)
			return failed || failedIn(
				group,
				outcome,
			)
		}

		var violation *orderViolation
		if errors.As(
			attemptErr,
			&violation,
		) {
			h.block(
				violation.blocked,
				results,
			)
			remaining := withoutIndexes(
				[][]int{
					group,
				},
				violation.blocked,
			)
			failed = true
			if len(remaining) == 0 {
				return true
			}
			group = remaining[0]
			continue
		}

		if h.transient(attemptErr) {
			if *budget > 0 {
				*budget--
				continue
			}
			for _, index := range group {
				results[index] = h.transientResult(attemptErr)
			}
			return true
		}

		err = attemptErr
	}

	if len(group) == 1 {
		results[group[0]] = Nak(err)
		return true
	}

	h.observer.OnBisect(BisectEvent{
		Messages: len(group),
		Groups:   1,
		Err:      err,
	})

	middle := len(group) / 2
	if h.ordered(
		ctx,
		msgs,
		group[:middle],
		nil,
		results,
		budget,
	) {
		h.block(
			group[middle:],
			results,
		)
		return true
	}

	suffixFailed := h.ordered(
		ctx,
		msgs,
		group[middle:],
		nil,
		results,
		budget,
	)

	return suffixFailed || failed
}
