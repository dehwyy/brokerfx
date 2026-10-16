package timedactor

import "time"

func (ta *TimedActor[T]) retryLater(
	key string,
	rev uint64,
	metadata T,
) {
	if ta.config.RescanInterval <= 0 {
		return
	}

	ta.mu.Lock()
	defer ta.mu.Unlock()

	if ta.ctx.Err() != nil {
		return
	}

	if _, scheduled := ta.timers[key]; scheduled {
		return
	}

	ta.timers[key] = time.AfterFunc(
		ta.config.CheckInterval,
		func() {
			ta.tryFireCallback(
				key,
				rev,
				metadata,
			)
		},
	)

	ta.logger.Warn().
		Str(
			"key",
			key,
		).
		Uint64(
			"rev",
			rev,
		).
		Dur(
			"retry_in",
			ta.config.CheckInterval,
		).
		Msg("timer key removal retry scheduled")
}
