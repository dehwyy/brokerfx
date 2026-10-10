package batch

import (
	"sort"
	"strings"

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

	var query strings.Builder
	query.WriteString("INSERT INTO processed_messages (message_id, subject, processed_at) VALUES ")
	args := make(
		[]any,
		0,
		len(ordered)*2,
	)
	for i, row := range ordered {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString("(?, ?, NOW())")
		args = append(
			args,
			row.MessageID,
			row.Subject,
		)
	}
	query.WriteString(" ON CONFLICT (message_id) DO NOTHING RETURNING message_id")

	var inserted []string
	if err := conn.Raw(
		query.String(),
		args...,
	).Scan(&inserted).Error; err != nil {
		return nil, err
	}

	for _, id := range inserted {
		claimed[id] = struct{}{}
	}

	return claimed, nil
}
