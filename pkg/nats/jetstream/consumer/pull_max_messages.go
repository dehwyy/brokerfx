package consumer

const (
	defaultPullMaxMessages = 50
	boundedPullMaxMessages = 1
)

func pullMaxMessages(maxInFlight int) int {
	if maxInFlight > 0 {
		return boundedPullMaxMessages
	}

	return defaultPullMaxMessages
}
