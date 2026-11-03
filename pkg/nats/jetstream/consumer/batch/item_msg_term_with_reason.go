package batch

func (m *itemMsg) TermWithReason(reason string) error {
	err := m.Msg.TermWithReason(reason)
	m.record(
		settledPoison,
		err,
	)

	return err
}
