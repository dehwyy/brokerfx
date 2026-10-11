package batch

func (h *inTx) transientResult(err error) Result {
	if h.transientDelay > 0 {
		return NakDelay(
			h.transientDelay,
			err,
		)
	}

	return Nak(err)
}
