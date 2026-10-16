package batch

import (
	"database/sql"
	"strings"
	"sync"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"gorm.io/gorm"
)

const messageIDTypeQuery = `select data_type from information_schema.columns where table_schema = current_schema() and table_name = 'processed_messages' and column_name = 'message_id'`

var messageIDTypes sync.Map

func claimQuery(
	conn *gorm.DB,
	ordered []consumer.ProcessedMessage,
) (string, []any, error) {
	elementType, err := messageIDType(conn)
	if err != nil {
		return "", nil, err
	}

	if elementType == "" {
		return claimValuesQuery(ordered)
	}

	keys := make(
		[]string,
		len(ordered),
	)
	subjects := make(
		[]string,
		len(ordered),
	)
	for i, row := range ordered {
		keys[i] = row.MessageID
		subjects[i] = row.Subject
	}

	query := "INSERT INTO processed_messages (message_id, subject, processed_at) SELECT t.k, t.s, NOW() FROM unnest(?::" + elementType + "[], ?::text[]) WITH ORDINALITY AS t(k, s, o) ORDER BY t.o ON CONFLICT (message_id) DO NOTHING RETURNING message_id"

	return query, []any{
		textArrayLiteral(keys),
		textArrayLiteral(subjects),
	}, nil
}

func messageIDType(conn *gorm.DB) (string, error) {
	if cached, ok := messageIDTypes.Load(conn.Config); ok {
		return cached.(string), nil
	}

	var dataType sql.NullString
	if err := conn.Raw(messageIDTypeQuery).Scan(&dataType).Error; err != nil {
		return "", err
	}

	elementType := ""
	switch dataType.String {
	case "uuid":
		elementType = "uuid"
	case "text", "character varying":
		elementType = "text"
	}

	messageIDTypes.Store(
		conn.Config,
		elementType,
	)

	return elementType, nil
}

func claimValuesQuery(ordered []consumer.ProcessedMessage) (string, []any, error) {
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

	return query.String(), args, nil
}

func textArrayLiteral(values []string) string {
	var literal strings.Builder
	literal.WriteByte('{')
	for i, value := range values {
		if i > 0 {
			literal.WriteByte(',')
		}
		literal.WriteByte('"')
		for _, r := range value {
			if r == '"' || r == '\\' {
				literal.WriteByte('\\')
			}
			literal.WriteRune(r)
		}
		literal.WriteByte('"')
	}
	literal.WriteByte('}')

	return literal.String()
}
