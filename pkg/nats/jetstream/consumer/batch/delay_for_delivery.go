package batch

import "time"

func (c *Consumer) delayForDelivery(delivered uint64) time.Duration {
	index := 0
	if delivered > 1 {
		index = int(delivered) - 1
	}
	if index >= len(c.delays) {
		index = len(c.delays) - 1
	}

	return c.delays[index]
}
