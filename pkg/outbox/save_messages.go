package outbox

import (
	"context"
	"time"
)

const saveMessagesChunk = 500

func (s *OutboxStore) SaveMessages(ctx context.Context, messages []Message, opts ...MessageOption) error {
	if len(messages) == 0 {
		return nil
	}

	for _, message := range messages {
		if message.err != nil {
			return message.err
		}
	}

	options, err := newMessageOptions(opts)
	if err != nil {
		return err
	}

	caps, err := s.ensureSchemaCaps(ctx)
	if err != nil {
		return err
	}

	rows, err := buildMessageRows(
		messages,
		options.headers,
		caps.V2,
		time.Now(),
	)
	if err != nil {
		return err
	}

	connection := s.txmanager.GetConnection(ctx)
	if !caps.V2 {
		events := legacyEvents(rows)
		return connection.CreateInBatches(
			&events,
			saveMessagesChunk,
		).Error
	}

	return connection.CreateInBatches(
		&rows,
		saveMessagesChunk,
	).Error
}
