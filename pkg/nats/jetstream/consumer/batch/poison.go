package batch

import "fmt"

func Poison(err error) error {
	return fmt.Errorf(
		"%w: %w",
		ErrPoison,
		err,
	)
}
