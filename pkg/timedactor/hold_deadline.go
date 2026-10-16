package timedactor

import "time"

func holdDeadline(
	original time.Time,
	now time.Time,
	maxHold time.Duration,
) time.Time {
	held := now.Add(maxHold)
	if original.After(held) {
		return original
	}

	return held
}
