package signature

import "crypto/ed25519"

type KeyedResigner struct {
	keys map[string]ed25519.PrivateKey
}

func NewKeyedResigner(keys map[string]ed25519.PrivateKey) *KeyedResigner {
	return &KeyedResigner{
		keys: keys,
	}
}

func (r *KeyedResigner) Sign(subject string, headers map[string]string, payload []byte) error {
	keyID := headers[HeaderAPIKeyID]
	if keyID == "" {
		return nil
	}

	privateKey, ok := r.keys[keyID]
	if !ok {
		return nil
	}

	return Sign(headers, keyID, privateKey, []byte(subject), payload)
}
