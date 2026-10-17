package pgtimer

import "time"

type Config struct {
	Table          Table
	Lease          time.Duration
	HandlerTimeout time.Duration
	Poll           time.Duration
	Batch          int
	Concurrency    int
	Observer       Observer
}
