package timedactor

import "time"

func (ta *TimedActor[T]) rescanEvery() time.Duration {
	if ta.config.RescanInterval > 0 {
		return ta.config.RescanInterval
	}

	return ta.config.CheckInterval
}
