package consumer

import "time"

const minHeartbeat = 100 * time.Millisecond

func heartbeatEvery(opts Opts) time.Duration {
	if opts.inProgressEvery > 0 {
		return opts.inProgressEvery
	}

	return inProgressInterval
}

func heartbeatForAckWait(ackWait time.Duration) time.Duration {
	every := ackWait / 3
	if ackWait <= 0 || every >= inProgressInterval {
		return inProgressInterval
	}
	if every < minHeartbeat {
		return minHeartbeat
	}

	return every
}
