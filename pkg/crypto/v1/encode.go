package cryptov1

import "github.com/vmihailenco/msgpack/v5"

func Encode(data any) ([]byte, error) {
	return msgpack.Marshal(data)
}
