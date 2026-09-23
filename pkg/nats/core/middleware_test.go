package core

import (
	"crypto/ed25519"
	"testing"
	"time"
)

func TestSignThenVerifyDelegatesToSignaturePackage(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	headers := make(map[string]string)
	subject := []byte("orders.created")
	body := []byte("payload")

	if err := Sign(headers, "key-1", privateKey, subject, body); err != nil {
		t.Fatalf("Sign returned error: %v", err)
	}

	if headers[HeaderAPIKeyID] != "key-1" {
		t.Fatalf("expected %s header to be set", HeaderAPIKeyID)
	}

	apiKeyID, err := Verify(headers, publicKey, subject, body, time.Now(), 5*time.Minute)
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}

	if apiKeyID != "key-1" {
		t.Fatalf("expected apiKeyID %q, got %q", "key-1", apiKeyID)
	}
}

func TestVerifyMissingHeaderReturnsErrMissingHeader(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	_, err = Verify(map[string]string{}, publicKey, []byte("subj"), []byte("body"), time.Now(), time.Minute)
	if err != ErrMissingHeader {
		t.Fatalf("expected ErrMissingHeader, got %v", err)
	}
}
