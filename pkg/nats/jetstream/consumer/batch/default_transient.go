package batch

import (
	"context"
	"database/sql/driver"
	"errors"
	"net"
)

func DefaultTransient(err error) bool {
	if err == nil {
		return false
	}

	var coded interface{ SQLState() string }
	if errors.As(
		err,
		&coded,
	) && transientSQLState(coded.SQLState()) {
		return true
	}

	if errors.Is(
		err,
		driver.ErrBadConn,
	) || errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		return true
	}

	var netErr net.Error
	return errors.As(
		err,
		&netErr,
	) && netErr.Timeout()
}

func transientSQLState(code string) bool {
	switch code {
	case "40001", "40P01", "55P03", "57014", "57P01", "57P02", "57P03":
		return true
	}

	if len(code) < 2 {
		return false
	}

	switch code[:2] {
	case "08", "53":
		return true
	}

	return false
}
