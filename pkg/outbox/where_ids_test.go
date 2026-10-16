package outbox

import "testing"

func TestTextArrayLiteral(t *testing.T) {
	got := textArrayLiteral([]string{
		"0199f3c2-0000-7000-8000-000000000001",
		`a"b`,
		`c\d`,
		"",
	})
	want := `{"0199f3c2-0000-7000-8000-000000000001","a\"b","c\\d",""}`
	if got != want {
		t.Fatalf("want %s got %s", want, got)
	}
}

func TestArrayElementType(t *testing.T) {
	cases := map[string]string{
		"uuid":              idTypeUUID,
		"text":              idTypeText,
		"character varying": idTypeText,
		"bigint":            "",
		"":                  "",
	}
	for dataType, want := range cases {
		if got := arrayElementType(dataType); got != want {
			t.Fatalf("%q: want %q got %q", dataType, want, got)
		}
	}
}
