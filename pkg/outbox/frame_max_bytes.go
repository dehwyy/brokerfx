package outbox

import "github.com/rs/zerolog/log"

type maxPayloader interface {
	MaxPayload() int64
}

func (r *OutboxRelay) frameMaxBytes() int {
	limit := r.frames.maxBytes
	sized, ok := r.producer.(maxPayloader)
	if !ok {
		return limit
	}

	payload := int(sized.MaxPayload())
	if payload <= 0 || limit <= payload-frameHeaderReserve {
		return limit
	}

	clamped := max(
		payload-frameHeaderReserve,
		minFrameMaxBytes,
	)
	r.frames.clampOnce.Do(func() {
		log.Warn().
			Int(
				"frame_max_bytes",
				limit,
			).
			Int(
				"max_payload",
				payload,
			).
			Int(
				"clamped",
				clamped,
			).
			Msg("outbox: frame max bytes clamped below the server max payload")
	})

	return clamped
}
