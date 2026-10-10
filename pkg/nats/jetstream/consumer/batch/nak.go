package batch

func Nak(err error) Result {
	return Result{
		Verdict: VerdictNak,
		Err:     err,
	}
}
