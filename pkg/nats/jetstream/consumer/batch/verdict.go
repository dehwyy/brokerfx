package batch

type Verdict int

const (
	VerdictNak Verdict = iota
	VerdictAck
	VerdictNakDelay
	VerdictTerm
	VerdictSkip
)
