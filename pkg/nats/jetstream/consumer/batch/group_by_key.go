package batch

import "github.com/nats-io/nats.go/jetstream"

func GroupByKey(
	msgs []jetstream.Msg,
	key KeyFunc,
) []Group {
	groups, _ := groupIndexed(
		msgs,
		key,
	)

	return groups
}

func groupIndexed(
	msgs []jetstream.Msg,
	key KeyFunc,
) ([]Group, [][]int) {
	positions := make(map[string]int)
	groups := make(
		[]Group,
		0,
		len(msgs),
	)
	indexes := make(
		[][]int,
		0,
		len(msgs),
	)

	for i, msg := range msgs {
		k := key(msg)
		position, ok := positions[k]
		if !ok {
			position = len(groups)
			positions[k] = position
			groups = append(
				groups,
				Group{
					Key: k,
				},
			)
			indexes = append(
				indexes,
				nil,
			)
		}

		groups[position].Msgs = append(
			groups[position].Msgs,
			msg,
		)
		indexes[position] = append(
			indexes[position],
			i,
		)
	}

	return groups, indexes
}
