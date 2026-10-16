package jetstream

import "testing"

func TestPublishAsyncMaxPending(t *testing.T) {
	cases := []struct {
		name   string
		value  string
		want   int
		wantOK bool
	}{
		{
			name: "unset keeps default",
		},
		{
			name:   "positive value",
			value:  "16000",
			want:   16000,
			wantOK: true,
		},
		{
			name:  "zero is ignored",
			value: "0",
		},
		{
			name:  "garbage is ignored",
			value: "lots",
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				t.Setenv(
					envPublishAsyncMaxPending,
					tc.value,
				)

				got, ok := publishAsyncMaxPending()
				if got != tc.want || ok != tc.wantOK {
					t.Fatalf("want %d %v got %d %v", tc.want, tc.wantOK, got, ok)
				}
				wantOptions := 0
				if tc.wantOK {
					wantOptions = 1
				}
				if len(publishOptions()) != wantOptions {
					t.Fatalf("options mismatch for %q", tc.value)
				}
			},
		)
	}
}
