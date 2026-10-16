package consumer

import (
	"context"

	"github.com/rs/zerolog/log"
)

type Drainer struct {
	consumers []Consumer
}

func NewDrainer() *Drainer {
	return &Drainer{
		consumers: make(
			[]Consumer,
			0,
		),
	}
}

func (d *Drainer) Append(consumers ...Consumer) {
	d.consumers = append(
		d.consumers,
		consumers...,
	)
}

func (d *Drainer) Drain() {
	if err := d.drainAll(context.Background()); err != nil {
		log.Warn().
			Err(err).
			Msg("jetstream consumers drain stopped early")
	}
}
