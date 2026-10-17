package batch

func MaxAckPendingFor(
	replicas int,
	pipeline int,
	maxSize int,
) int {
	perPod := pipeline * maxSize
	if replicas <= 0 {
		return perPod
	}

	return (3*replicas*perPod + 1) / 2
}
