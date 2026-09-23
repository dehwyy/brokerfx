package signature

import (
	"crypto/ed25519"
	"testing"
	"time"
)

func TestSignThenVerifyRoundTrip(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	headers := make(map[string]string)
	subject := []byte("orders.created")
	body := []byte(`{"order_id":"1"}`)

	if err := Sign(headers, "key-1", privateKey, subject, body); err != nil {
		t.Fatalf("Sign returned error: %v", err)
	}

	apiKeyID, err := Verify(headers, publicKey, subject, body, time.Now(), 5*time.Minute)
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}

	if apiKeyID != "key-1" {
		t.Fatalf("expected apiKeyID %q, got %q", "key-1", apiKeyID)
	}
}

func TestVerifyOutsideWindowReturnsErrSignatureExpired(t *testing.T) {
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

	future := time.Now().Add(10 * time.Minute)

	_, err = Verify(headers, publicKey, subject, body, future, 5*time.Minute)
	if err != ErrSignatureExpired {
		t.Fatalf("expected ErrSignatureExpired, got %v", err)
	}
}

func TestVerifyWithinWindowAfterDelaySucceeds(t *testing.T) {
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

	laterButInWindow := time.Now().Add(4 * time.Minute)

	if _, err := Verify(headers, publicKey, subject, body, laterButInWindow, 5*time.Minute); err != nil {
		t.Fatalf("expected no error within window, got %v", err)
	}
}

func TestVerifyMissingHeaderReturnsErrMissingHeader(t *testing.T) {
	_, publicKey, _ := testKeys(t)

	_, err := Verify(map[string]string{}, publicKey, []byte("subj"), []byte("body"), time.Now(), time.Minute)
	if err != ErrMissingHeader {
		t.Fatalf("expected ErrMissingHeader, got %v", err)
	}
}

func TestVerifyTamperedBodyReturnsErrInvalidSignature(t *testing.T) {
	privateKey, publicKey, _ := testKeys(t)

	headers := make(map[string]string)
	subject := []byte("orders.created")

	if err := Sign(headers, "key-1", privateKey, subject, []byte("original")); err != nil {
		t.Fatalf("Sign returned error: %v", err)
	}

	_, err := Verify(headers, publicKey, subject, []byte("tampered"), time.Now(), time.Minute)
	if err != ErrInvalidSignature {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestVerifyWrongPublicKeyReturnsErrInvalidSignature(t *testing.T) {
	privateKey, _, _ := testKeys(t)
	_, otherPublicKey, _ := testKeys(t)

	headers := make(map[string]string)
	subject := []byte("orders.created")
	body := []byte("payload")

	if err := Sign(headers, "key-1", privateKey, subject, body); err != nil {
		t.Fatalf("Sign returned error: %v", err)
	}

	_, err := Verify(headers, otherPublicKey, subject, body, time.Now(), time.Minute)
	if err != ErrInvalidSignature {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestSignNilHeadersReturnsError(t *testing.T) {
	privateKey, _, _ := testKeys(t)

	if err := Sign(nil, "key-1", privateKey, []byte("subj"), []byte("body")); err == nil {
		t.Fatalf("expected error for nil headers map")
	}
}

func TestNewEd25519SignerSignThenVerifyReturnsAPIKeyID(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	signer := NewEd25519Signer("key-widgetapi", privateKey)

	headers := make(map[string]string)
	payload := []byte(`{"order_id":"1"}`)

	if err := signer.Sign("orders.created", headers, payload); err != nil {
		t.Fatalf("Sign returned error: %v", err)
	}

	apiKeyID, err := Verify(headers, publicKey, []byte("orders.created"), payload, time.Now(), 5*time.Minute)
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}

	if apiKeyID != "key-widgetapi" {
		t.Fatalf("expected apiKeyID %q, got %q", "key-widgetapi", apiKeyID)
	}
}

func testKeys(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	return privateKey, publicKey, err
}
