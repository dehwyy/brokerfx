package pgtimer

import (
	"context"
	"time"
)

type Timer struct {
	Key        string
	Name       string
	Attempt    string
	DueAt      time.Time
	LeaseDueAt time.Time
	ClaimedAt  time.Time
}

type HandlerFunc func(ctx context.Context, timer Timer) error
