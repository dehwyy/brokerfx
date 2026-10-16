package consumer

const defaultPullMaxMessages = 50

func pullMaxMessages(maxInFlight int) int {
	if maxInFlight > 0 {
		return maxInFlight
	}

	return defaultPullMaxMessages
}
