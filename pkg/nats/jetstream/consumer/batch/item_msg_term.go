package batch

func (m *itemMsg) Term() error {
	err := m.Msg.Term()
	m.record(
		settledPoison,
		err,
	)

	return err
}
