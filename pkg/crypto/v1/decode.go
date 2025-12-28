package cryptov1

import "github.com/vmihailenco/msgpack/v5"

func Decode[T any](data []byte) (T, error) {
	var t T
	if err := msgpack.Unmarshal(data, &t); err != nil {
		return t, err
	}

	return t, nil
}
