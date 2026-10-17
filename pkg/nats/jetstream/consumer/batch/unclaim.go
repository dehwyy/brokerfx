package batch

import "gorm.io/gorm"

var unclaimKeys = func(
	conn *gorm.DB,
	keys []string,
) error {
	if len(keys) == 0 {
		return nil
	}

	elementType, err := messageIDType(conn)
	if err != nil {
		return err
	}

	if elementType == "" {
		return conn.Exec(
			"DELETE FROM processed_messages WHERE message_id IN ?",
			keys,
		).Error
	}

	return conn.Exec(
		"DELETE FROM processed_messages WHERE message_id = ANY(?::"+elementType+"[])",
		textArrayLiteral(keys),
	).Error
}
