package batch

import (
	"sort"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/nats-io/nats.go/jetstream"
	"gorm.io/gorm"
)

func claimRows(
	keys []string,
	msgs []jetstream.Msg,
) []consumer.ProcessedMessage {
	rows := make(
		[]consumer.ProcessedMessage,
		len(keys),
	)
	for i, key := range keys {
		rows[i] = consumer.ProcessedMessage{
			MessageID: key,
			Subject:   msgs[i].Subject(),
		}
	}

	return rows
}

var claimKeys = func(
	conn *gorm.DB,
	rows []consumer.ProcessedMessage,
) (map[string]struct{}, error) {
	claimed := make(
		map[string]struct{},
		len(rows),
	)
	if len(rows) == 0 {
		return claimed, nil
	}

	ordered := append(
		[]consumer.ProcessedMessage(nil),
		rows...,
	)
	sort.Slice(
		ordered,
		func(i, j int) bool {
			return ordered[i].MessageID < ordered[j].MessageID
		},
	)

	query, args, err := claimQuery(
		conn,
		ordered,
	)
	if err != nil {
		return nil, err
	}

	var inserted []string
	if err := conn.Raw(
		query,
		args...,
	).Scan(&inserted).Error; err != nil {
		return nil, err
	}

	for _, id := range inserted {
		claimed[id] = struct{}{}
	}

	return claimed, nil
}
