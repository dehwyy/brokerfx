package batch

import "time"

type Result struct {
	Verdict Verdict
	Delay   time.Duration
	Err     error
}
