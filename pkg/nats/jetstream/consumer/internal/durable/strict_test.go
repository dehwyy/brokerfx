package durable

import "testing"

func TestStrictEnv(t *testing.T) {
	cases := []struct {
		env    string
		inCode bool
		want   bool
	}{
		{env: "", inCode: true, want: true},
		{env: "", inCode: false, want: false},
		{env: "false", inCode: true, want: false},
		{env: " OFF ", inCode: true, want: false},
		{env: "0", inCode: true, want: false},
		{env: "true", inCode: false, want: false},
		{env: "true", inCode: true, want: true},
		{env: "maybe", inCode: true, want: true},
	}

	for _, tc := range cases {
		t.Setenv(envStrictUpdate, tc.env)
		if got := Strict(tc.inCode); got != tc.want {
			t.Fatalf("env %q in code %v: got %v", tc.env, tc.inCode, got)
		}
	}
}

func TestIsTransientSkipsRejections(t *testing.T) {
	if isTransient(errAckPolicy) {
		t.Fatal("api 400 is not transient")
	}
}
