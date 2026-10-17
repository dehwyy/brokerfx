package outbox

import "strings"

func subjectMatches(
	pattern string,
	subject string,
) bool {
	patternTokens := strings.Split(
		pattern,
		".",
	)
	subjectTokens := strings.Split(
		subject,
		".",
	)
	for i, token := range patternTokens {
		if token == ">" {
			return len(subjectTokens) > i
		}
		if i >= len(subjectTokens) {
			return false
		}
		if token != "*" && token != subjectTokens[i] {
			return false
		}
	}

	return len(patternTokens) == len(subjectTokens)
}

func matchesAnyPattern(
	subject string,
	patterns []string,
) bool {
	for _, pattern := range patterns {
		if subjectMatches(
			pattern,
			subject,
		) {
			return true
		}
	}

	return false
}
