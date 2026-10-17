package durable

import (
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

const envStrictUpdate = "BROKERFX_CONSUMER_STRICT_UPDATE"

func Strict(inCode bool) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(envStrictUpdate)))
	switch raw {
	case "":
		return inCode
	case "0", "false", "off", "no":
		return false
	case "1", "true", "on", "yes":
		return inCode
	}

	log.Warn().
		Str(
			"env",
			envStrictUpdate,
		).
		Str(
			"value",
			raw,
		).
		Bool(
			"strict",
			inCode,
		).
		Msg("invalid consumer strict update, keeping the value from code")

	return inCode
}
