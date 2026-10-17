package batch

func Skip() Result {
	return Result{
		Verdict: VerdictSkip,
	}
}
