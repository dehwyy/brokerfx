package outbox

import "testing"

type fakeSigner struct {
	calls   int
	subject string
	err     error
}

func (f *fakeSigner) Sign(subject string, headers map[string]string, payload []byte) error {
	f.calls++
	f.subject = subject
	if f.err != nil {
		return f.err
	}
	headers["X-Signature"] = "signed"
	return nil
}

func TestApplySignatureNilSignerIsNoop(t *testing.T) {
	headers := map[string]string{}

	if err := applySignature(nil, "orders.created", headers, []byte("payload")); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, ok := headers["X-Signature"]; ok {
		t.Fatalf("expected no signature header when signer is nil")
	}
}

func TestApplySignatureSkipsKVSubjects(t *testing.T) {
	signer := &fakeSigner{}
	headers := map[string]string{}

	if err := applySignature(signer, "$KV.bucket.key", headers, []byte("value")); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if signer.calls != 0 {
		t.Fatalf("expected signer not to be called for KV subject, got %d calls", signer.calls)
	}

	if _, ok := headers["X-Signature"]; ok {
		t.Fatalf("expected no signature header on KV subject")
	}
}

func TestApplySignatureCallsSignerForRegularSubject(t *testing.T) {
	signer := &fakeSigner{}
	headers := map[string]string{}
	payload := []byte("payload")

	if err := applySignature(signer, "orders.created", headers, payload); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if signer.calls != 1 {
		t.Fatalf("expected signer to be called once, got %d", signer.calls)
	}

	if signer.subject != "orders.created" {
		t.Fatalf("expected subject %q, got %q", "orders.created", signer.subject)
	}

	if headers["X-Signature"] != "signed" {
		t.Fatalf("expected signature header to be set by signer")
	}
}

func TestApplySignaturePropagatesSignerError(t *testing.T) {
	sentinel := errSentinel{"signer failed"}
	signer := &fakeSigner{err: sentinel}
	headers := map[string]string{}

	err := applySignature(signer, "orders.created", headers, []byte("payload"))
	if err != sentinel {
		t.Fatalf("expected sentinel error to propagate, got %v", err)
	}
}

type errSentinel struct {
	msg string
}

func (e errSentinel) Error() string {
	return e.msg
}
