package frame

import "github.com/vmihailenco/msgpack/v5"

func Marshal(element Element) ([]byte, error) {
	return msgpack.Marshal(&element)
}
