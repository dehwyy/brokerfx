package outbox

func (m Message) ownHeaders() (map[string]string, error) {
	if err := checkReservedHeaders(m.Headers); err != nil {
		return nil, err
	}

	if len(m.Headers) == 0 {
		return m.headers, nil
	}

	return mergeHeaders(
		m.Headers,
		m.headers,
	), nil
}
