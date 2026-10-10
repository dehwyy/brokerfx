package batch

func Term(err error) Result {
	return Result{
		Verdict: VerdictTerm,
		Err:     err,
	}
}
