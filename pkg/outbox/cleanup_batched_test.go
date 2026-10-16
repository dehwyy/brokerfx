package outbox

import "testing"

func TestCleanupBatchFromEnv(t *testing.T) {
	cases := []struct {
		value string
		want  int
	}{
		{
			value: "",
			want:  0,
		},
		{
			value: "5000",
			want:  5000,
		},
		{
			value: "-1",
			want:  0,
		},
		{
			value: "many",
			want:  0,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.value,
			func(t *testing.T) {
				t.Setenv(
					envCleanupBatch,
					tc.value,
				)
				r := NewRelay(RelayDeps{
					Store: &OutboxStore{},
					Config: Config{
						Mode: ModeUpdateAfterSend,
					},
				})
				if r.config.CleanupBatch != tc.want {
					t.Fatalf("want %d got %d", tc.want, r.config.CleanupBatch)
				}
			},
		)
	}
}
