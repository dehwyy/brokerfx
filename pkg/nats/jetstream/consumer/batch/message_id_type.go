package batch

import (
	"database/sql"
	"reflect"
	"sync"

	"gorm.io/gorm"
)

var messageIDTypes sync.Map

var loadMessageIDType = func(conn *gorm.DB) (sql.NullString, error) {
	var dataType sql.NullString
	if err := conn.Raw(messageIDTypeQuery).Scan(&dataType).Error; err != nil {
		return sql.NullString{}, err
	}

	return dataType, nil
}

func messageIDType(conn *gorm.DB) (string, error) {
	pool := conn.Config.ConnPool
	cacheable := pool != nil && reflect.TypeOf(pool).Comparable()
	if cacheable {
		if cached, ok := messageIDTypes.Load(pool); ok {
			return cached.(string), nil
		}
	}

	dataType, err := loadMessageIDType(conn)
	if err != nil {
		return "", err
	}
	if !dataType.Valid {
		return "", nil
	}

	elementType := ""
	switch dataType.String {
	case "uuid":
		elementType = "uuid"
	case "text", "character varying":
		elementType = "text"
	}

	if cacheable {
		messageIDTypes.LoadOrStore(
			pool,
			elementType,
		)
	}

	return elementType, nil
}
