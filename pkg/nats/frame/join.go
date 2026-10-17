package frame

import (
	"math"

	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

func Join(encoded [][]byte) []byte {
	size := MaxArrayHeader
	for _, element := range encoded {
		size += len(element)
	}

	data := arrayHeader(
		make(
			[]byte,
			0,
			size,
		),
		len(encoded),
	)
	for _, element := range encoded {
		data = append(
			data,
			element...,
		)
	}

	return data
}

func arrayHeader(
	data []byte,
	n int,
) []byte {
	switch {
	case n <= 15:
		return append(
			data,
			msgpcode.FixedArrayLow|byte(n),
		)
	case n <= math.MaxUint16:
		return append(
			data,
			msgpcode.Array16,
			byte(n>>8),
			byte(n),
		)
	}

	return append(
		data,
		msgpcode.Array32,
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}
