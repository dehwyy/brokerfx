package consumeroptsbuilder

import "strings"

func subjectsOverlap(
	first string,
	second string,
) bool {
	left := strings.Split(
		first,
		".",
	)
	right := strings.Split(
		second,
		".",
	)
	for i := 0; i < len(left) && i < len(right); i++ {
		if left[i] == ">" || right[i] == ">" {
			return true
		}
		if left[i] != "*" && right[i] != "*" && left[i] != right[i] {
			return false
		}
	}

	return len(left) == len(right)
}
