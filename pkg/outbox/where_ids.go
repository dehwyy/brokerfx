package outbox

import (
	"strings"

	"gorm.io/gorm"
)

const (
	idTypeUUID = "uuid"
	idTypeText = "text"
)

func arrayElementType(dataType string) string {
	switch dataType {
	case "uuid":
		return idTypeUUID
	case "text", "character varying":
		return idTypeText
	}

	return ""
}

func whereIDs(
	query *gorm.DB,
	idType string,
	ids []string,
) *gorm.DB {
	switch idType {
	case idTypeUUID:
		return query.Where(
			"id = ANY(?::uuid[])",
			textArrayLiteral(ids),
		)
	case idTypeText:
		return query.Where(
			"id = ANY(?::text[])",
			textArrayLiteral(ids),
		)
	}

	return query.Where(
		"id IN ?",
		ids,
	)
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
