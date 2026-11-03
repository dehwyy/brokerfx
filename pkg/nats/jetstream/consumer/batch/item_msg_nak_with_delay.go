package batch

import "time"

func (m *itemMsg) NakWithDelay(delay time.Duration) error {
	err := m.Msg.NakWithDelay(delay)
	m.record(
		settledRetry,
		err,
	)

	return err
}
