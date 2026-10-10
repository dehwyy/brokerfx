package batch

import "time"

func NakDelay(
	delay time.Duration,
	err error,
) Result {
	return Result{
		Verdict: VerdictNakDelay,
		Delay:   delay,
		Err:     err,
	}
}
