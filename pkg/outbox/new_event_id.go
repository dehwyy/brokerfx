package outbox

import (
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const envIDV7 = "BROKERFX_OUTBOX_ID_V7"

var (
	idV7Once sync.Once
	idV7     bool
)

func newEventID() (string, error) {
	idV7Once.Do(func() {
		idV7 = idV7FromEnv()
	})
	if !idV7 {
		return uuid.NewString(), nil
	}

	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}

	return id.String(), nil
}

func idV7FromEnv() bool {
	raw := strings.TrimSpace(os.Getenv(envIDV7))
	if raw == "" {
		return true
	}

	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		log.Warn().
			Str(
				"env",
				envIDV7,
			).
			Str(
				"value",
				raw,
			).
			Msg("invalid outbox id version switch, keeping uuid v7")
		return true
	}

	return enabled
}
