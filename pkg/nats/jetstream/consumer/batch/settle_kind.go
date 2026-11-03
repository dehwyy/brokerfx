package batch

type settleKind int

const (
	settledAck settleKind = iota
	settledSkip
	settledRetry
	settledPoison
	settledBlocked
)

func (e SettleEvent) with(kind settleKind) SettleEvent {
	switch kind {
	case settledAck:
		e.Ack++
	case settledSkip:
		e.Skip++
	case settledRetry:
		e.Retry++
	case settledPoison:
		e.Poison++
	case settledBlocked:
		e.Blocked++
	}

	return e
}

func (k settleKind) failure() string {
	switch k {
	case settledRetry:
		return "nak"
	case settledPoison:
		return "term"
	case settledBlocked:
		return "blocked"
	}

	return ""
}

func (e SettleEvent) failed() int {
	return e.Retry + e.Poison + e.Blocked
}

func (e SettleEvent) plus(other SettleEvent) SettleEvent {
	return SettleEvent{
		Ack:     e.Ack + other.Ack,
		Skip:    e.Skip + other.Skip,
		Retry:   e.Retry + other.Retry,
		Poison:  e.Poison + other.Poison,
		Blocked: e.Blocked + other.Blocked,
	}
}
