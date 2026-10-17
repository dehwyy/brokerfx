package outbox

func (r *OutboxRelay) warnLegacyFrames() {
	if !r.frames.active() || !r.config.Batched {
		return
	}

	r.frames.legacyOnce.Do(func() {
		r.logger.Warn().
			Strs(
				"frame_subjects",
				r.frames.patterns,
			).
			Msg("outbox: relay fell back to the per-message path, frames are off until the batched path is back")
	})
}
