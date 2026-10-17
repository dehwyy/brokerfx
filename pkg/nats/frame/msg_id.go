package frame

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
)

func MsgID(ids []string) string {
	sorted := slices.Clone(ids)
	slices.Sort(sorted)
	sum := sha256.Sum256([]byte(strings.Join(
		sorted,
		"\n",
	)))

	return msgIDPrefix + hex.EncodeToString(sum[:])
}
