package signature

import (
	"crypto/ed25519"
	"maps"
	"strconv"
	"testing"
	"time"
)

const resignSubject = "provider.command.p1.payout"

func TestKeyedResignerRefreshesTimestampForKnownKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{}
	if err := Sign(headers, "payout-processing", privateKey, []byte(resignSubject), []byte("body")); err != nil {
		t.Fatal(err)
	}
	stale := strconv.FormatInt(time.Now().Add(-10*time.Minute).UnixMilli(), 10)
	headers[HeaderTimestamp] = stale

	resigner := NewKeyedResigner(map[string]ed25519.PrivateKey{
		"payout-processing": privateKey,
	})
	if err := resigner.Sign(resignSubject, headers, []byte("body")); err != nil {
		t.Fatal(err)
	}

	if headers[HeaderTimestamp] == stale {
		t.Fatal("timestamp was not refreshed")
	}
	if _, err := Verify(headers, publicKey, []byte(resignSubject), []byte("body"), time.Now(), 5*time.Minute); err != nil {
		t.Fatalf("verify after resign: %v", err)
	}
}

func TestKeyedResignerLeavesUnknownKeyUntouched(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{
		HeaderAPIKeyID:  "balance-key",
		HeaderTimestamp: "1",
		HeaderSignature: "sig",
	}
	before := maps.Clone(headers)

	if err := NewKeyedResigner(map[string]ed25519.PrivateKey{"payout-processing": privateKey}).Sign(resignSubject, headers, []byte("body")); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(before, headers) {
		t.Fatalf("headers changed: %v", headers)
	}
}

func TestKeyedResignerLeavesUnsignedUntouched(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{
		"Paylonium-Envelope": "v1",
	}

	if err := NewKeyedResigner(map[string]ed25519.PrivateKey{"payout-processing": privateKey}).Sign("payout.event.order.updated", headers, []byte("body")); err != nil {
		t.Fatal(err)
	}
	if len(headers) != 1 {
		t.Fatalf("unsigned message got headers: %v", headers)
	}
}
