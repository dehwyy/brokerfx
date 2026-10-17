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
