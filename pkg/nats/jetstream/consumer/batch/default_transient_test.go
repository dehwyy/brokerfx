package batch

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestDefaultTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "serialization", err: pgErr{code: "40001"}, want: true},
		{name: "deadlock", err: pgErr{code: "40P01"}, want: true},
		{name: "lock not available", err: pgErr{code: "55P03"}, want: true},
		{name: "query canceled", err: pgErr{code: "57014"}, want: true},
		{name: "admin shutdown", err: pgErr{code: "57P01"}, want: true},
		{name: "connection failure", err: pgErr{code: "08006"}, want: true},
		{name: "too many connections", err: pgErr{code: "53300"}, want: true},
		{name: "unique violation", err: pgErr{code: "23505"}, want: false},
		{name: "invalid text", err: pgErr{code: "22P02"}, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "bad conn", err: driver.ErrBadConn, want: true},
		{name: "net timeout", err: timeoutErr{}, want: true},
		{name: "plain", err: errors.New("x"), want: false},
		{name: "nil", err: nil, want: false},
		{name: "wrapped", err: fmt.Errorf("apply: %w", pgErr{code: "40001"}), want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DefaultTransient(tc.err); got != tc.want {
				t.Fatalf("want %v got %v", tc.want, got)
			}
		})
	}
}
