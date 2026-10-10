package batch

import (
	"context"

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
		err := h.attempt(
			ctx,
			msgs,
			subset,
		)
		if err == nil {
			for _, index := range subset {
				results[index] = Ack()
			}
			return
		}

		if h.transient(err) {
			if *budget > 0 {
				*budget--
				continue
			}
			for _, index := range subset {
				results[index] = NakDelay(
					h.transientDelay,
					err,
				)
			}
			return
		}

		if len(subset) == 1 {
			results[subset[0]] = Nak(err)
			return
		}

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

func (h *inTx) attempt(
	ctx context.Context,
	msgs []jetstream.Msg,
	subset []int,
) error {
	return h.tx.Do(
		ctx,
		h.txName,
		func(ctx context.Context) error {
			fresh, err := h.fresh(
				ctx,
				msgs,
				subset,
			)
			if err != nil {
				return err
			}
			if len(fresh) == 0 {
				return nil
			}

			return h.apply(
				ctx,
				fresh,
			)
		},
	)
}

func (h *inTx) fresh(
	ctx context.Context,
	msgs []jetstream.Msg,
	subset []int,
) ([]jetstream.Msg, error) {
	selected := make(
		[]jetstream.Msg,
		0,
		len(subset),
	)
	if h.noDedup {
		for _, index := range subset {
			selected = append(
				selected,
				msgs[index],
			)
		}
		return selected, nil
	}

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
			msgs[index],
		)
	}

	claimed, err := claimKeys(
		h.tx.GetConnection(ctx),
		claimRows(
			keys,
			selected,
		),
	)
	if err != nil {
		return nil, err
	}

	fresh := selected[:0]
	for i, msg := range selected {
		if _, ok := claimed[keys[i]]; ok {
			fresh = append(
				fresh,
				msg,
			)
		}
	}

	return fresh, nil
}
