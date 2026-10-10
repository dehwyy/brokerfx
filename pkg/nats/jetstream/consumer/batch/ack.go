package batch

func Ack() Result {
	return Result{
		Verdict: VerdictAck,
	}
}
