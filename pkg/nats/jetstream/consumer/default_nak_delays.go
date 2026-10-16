package consumer

import "time"

var DefaultNakDelays = []time.Duration{
	time.Second,
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	5 * time.Minute,
}
