package outbox

import (
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

func resolveFraming(cfg Config) *framing {
	raw := cfg.FrameSubjects
	if value, ok := os.LookupEnv(envFrameSubjects); ok {
		raw = value
	}

	frames := &framing{
		maxElements: defaultFrameMaxElement,
		maxBytes:    defaultFrameMaxBytes,
	}
	for _, pattern := range splitList(raw) {
		pattern = strings.TrimSpace(pattern)
		if !validSubjectPattern(pattern) {
			log.Warn().
				Str(
					"env",
					envFrameSubjects,
				).
				Str(
					"pattern",
					pattern,
				).
				Msg("outbox: ignoring invalid frame subject pattern")
			continue
		}
		frames.patterns = append(
			frames.patterns,
			pattern,
		)
	}

	if value, ok := envNonNegativeInt(envFrameMaxElements); ok {
		if value < 2 || value > limitFrameMaxElements {
			log.Warn().
				Int(
					envFrameMaxElements,
					value,
				).
				Int(
					"default",
					defaultFrameMaxElement,
				).
				Msg("outbox: frame max elements out of range 2..1024, keeping the default")
		} else {
			frames.maxElements = value
		}
	}
	if value, ok := envNonNegativeInt(envFrameMaxBytes); ok {
		if value < minFrameMaxBytes {
			log.Warn().
				Int(
					envFrameMaxBytes,
					value,
				).
				Int(
					"default",
					defaultFrameMaxBytes,
				).
				Msg("outbox: frame max bytes below 1024, keeping the default")
		} else {
			frames.maxBytes = value
		}
	}

	if frames.active() {
		log.Info().
			Strs(
				"frame_subjects",
				frames.patterns,
			).
			Int(
				"frame_max_elements",
				frames.maxElements,
			).
			Int(
				"frame_max_bytes",
				frames.maxBytes,
			).
			Msg("outbox: frames on")
	}

	return frames
}

func validSubjectPattern(pattern string) bool {
	if pattern == "" {
		return false
	}
	tokens := strings.Split(
		pattern,
		".",
	)
	for i, token := range tokens {
		if token == "" || strings.ContainsAny(
			token,
			" \t\r\n",
		) {
			return false
		}
		if token == ">" && i != len(tokens)-1 {
			return false
		}
		if len(token) > 1 && strings.ContainsAny(
			token,
			"*>",
		) {
			return false
		}
	}

	return true
}
