package outbox

func (r *OutboxRelay) publishFramed(
	events []ProducerEvent,
	publish func([]ProducerEvent) []error,
) []error {
	wire, members, framed := r.frameEvents(events)
	if !framed {
		return publish(events)
	}

	wireErrs := publish(wire)
	errs := make(
		[]error,
		len(events),
	)
	for position, group := range members {
		if position >= len(wireErrs) {
			continue
		}
		for _, index := range group {
			errs[index] = wireErrs[position]
		}
	}

	return errs
}
