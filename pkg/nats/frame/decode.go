package frame

import (
	"bytes"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

func Decode(data []byte) ([]Element, error) {
	reader := bytes.NewReader(data)
	decoder := msgpack.NewDecoder(reader)

	count, err := decoder.DecodeArrayLen()
	if err != nil {
		return nil, fmt.Errorf(
			"%w: %w",
			ErrBadFrame,
			err,
		)
	}
	if count <= 0 {
		return nil, fmt.Errorf(
			"%w: %d elements",
			ErrBadFrame,
			count,
		)
	}

	elements := make(
		[]Element,
		0,
		min(
			count,
			len(data),
		),
	)
	for i := range count {
		var element Element
		if err := decoder.Decode(&element); err != nil {
			return nil, fmt.Errorf(
				"%w: element %d: %w",
				ErrBadFrame,
				i,
				err,
			)
		}
		elements = append(
			elements,
			element,
		)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf(
			"%w: %d trailing bytes",
			ErrBadFrame,
			reader.Len(),
		)
	}

	return elements, nil
}
