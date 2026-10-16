package outbox

import (
	"encoding/json"
	"time"
)

func buildMessageRows(
	messages []Message,
	headers map[string]string,
	v2 bool,
	now time.Time,
) ([]outboxEventRow, error) {
	sharedHeaders, err := encodeHeaders(headers)
	if err != nil {
		return nil, err
	}

	base := now.Truncate(time.Microsecond)
	rows := make(
		[]outboxEventRow,
		len(messages),
	)
	for i, message := range messages {
		if message.err != nil {
			return nil, message.err
		}

		own, err := message.ownHeaders()
		if err != nil {
			return nil, err
		}

		rowHeaders := sharedHeaders
		if len(own) > 0 {
			rowHeaders, err = encodeHeaders(mergeHeaders(
				headers,
				own,
			))
			if err != nil {
				return nil, err
			}
		}

		if rowHeaders != nil && !v2 {
			return nil, ErrSchemaOutdated
		}

		payload := message.Payload
		if payload == nil {
			payload = []byte{}
		}

		eventID, err := newEventID()
		if err != nil {
			return nil, err
		}

		createdAt := base.Add(time.Duration(i) * time.Microsecond)
		rows[i] = outboxEventRow{
			ID:        eventID,
			Topic:     message.Subject,
			Payload:   payload,
			State:     StatePending,
			Headers:   rowHeaders,
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}
	}

	return rows, nil
}

func mergeHeaders(shared, own map[string]string) map[string]string {
	merged := make(
		map[string]string,
		len(shared)+len(own),
	)
	for key, value := range shared {
		merged[key] = value
	}
	for key, value := range own {
		merged[key] = value
	}

	return merged
}

func encodeHeaders(headers map[string]string) ([]byte, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	return json.Marshal(headers)
}

func legacyEvents(rows []outboxEventRow) []OutboxEvent {
	events := make(
		[]OutboxEvent,
		len(rows),
	)
	for i, row := range rows {
		events[i] = OutboxEvent{
			ID:        row.ID,
			Topic:     row.Topic,
			Payload:   row.Payload,
			State:     row.State,
			CreatedAt: row.CreatedAt,
			UpdatedAt: row.UpdatedAt,
		}
	}

	return events
}
