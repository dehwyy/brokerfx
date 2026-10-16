package timedactor

import (
	"errors"
	"fmt"
	"time"
)

var ErrInvalidConfig = errors.New("timedactor: invalid config")

const minMarkerTTL = time.Second

func (c Config) validate() error {
	if c.MarkerTTL != 0 && c.MarkerTTL < minMarkerTTL {
		return fmt.Errorf(
			"%w: MarkerTTL %s, want 0 or at least %s",
			ErrInvalidConfig,
			c.MarkerTTL,
			minMarkerTTL,
		)
	}

	if c.RescanInterval < 0 {
		return fmt.Errorf(
			"%w: RescanInterval %s, want 0 or positive",
			ErrInvalidConfig,
			c.RescanInterval,
		)
	}

	return nil
}
