package consumer

import (
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samber/lo"
)

type Drainer struct {
	consumerCtxs []jetstream.ConsumeContext
}

func NewDrainer() *Drainer {
	return &Drainer{
		consumerCtxs: make([]jetstream.ConsumeContext, 0),
	}
}

func (d *Drainer) Append(consumers ...Consumer) {
	d.consumerCtxs = append(
		d.consumerCtxs,
		lo.Map(
			consumers,
			func(consumer Consumer, _ int) jetstream.ConsumeContext {
				return consumer.consumeCtx
			},
		)...,
	)
}

func (d *Drainer) Drain() {
	var wg sync.WaitGroup
	for _, consumerCtx := range d.consumerCtxs {
		wg.Add(1)
		go func(consumerCtx jetstream.ConsumeContext) {
			defer wg.Done()
			consumerCtx.Drain()
			<-consumerCtx.Closed()
		}(consumerCtx)
	}
	wg.Wait()
}
