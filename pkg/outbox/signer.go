package outbox

import "strings"

const kvSubjectPrefix = "$KV."

type Signer interface {
	Sign(subject string, headers map[string]string, payload []byte) error
}

func applySignature(signer Signer, subject string, headers map[string]string, payload []byte) error {
	if signer == nil {
		return nil
	}

	if strings.HasPrefix(subject, kvSubjectPrefix) {
		return nil
	}

	return signer.Sign(subject, headers, payload)
}
