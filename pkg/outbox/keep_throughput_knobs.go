package outbox

func keepThroughputKnobs(dst Config, src Config) Config {
	dst.PendingGaugeInterval = src.PendingGaugeInterval
	dst.CleanupBatch = src.CleanupBatch
	dst.LatestOnlyPrefixes = src.LatestOnlyPrefixes
	dst.TxDrain = src.TxDrain
	dst.TxDrainPublishTimeout = src.TxDrainPublishTimeout

	return dst
}
