package batch

func (s settings) perPod() settings {
	s.replicas = 0

	return s
}
