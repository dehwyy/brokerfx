package outbox

func keepThroughputKnobs(dst Config, src Config) Config {
	dst.PendingGaugeInterval = src.PendingGaugeInterval
	dst.CleanupBatch = src.CleanupBatch

	return dst
}
