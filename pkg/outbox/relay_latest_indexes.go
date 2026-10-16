package outbox

func (r *OutboxRelay) latestIndexes(events []relayEvent) map[string]int {
	indexes := latestIndexes(
		events,
		r.latestOnly,
	)

	superseded := 0
	for i, event := range events {
		if latest, ok := indexes[event.Topic]; ok && latest != i {
			superseded++
		}
	}
	if superseded == 0 {
		return indexes
	}

	r.logger.Debug().
		Int("count", superseded).
		Msg("outbox events superseded by a later snapshot in the batch, closed without publish")
	if observer, ok := r.observer.(SupersededObserver); ok {
		observer.OnSuperseded(superseded)
	}

	return indexes
}
