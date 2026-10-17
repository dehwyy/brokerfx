package consumeroptsbuilder

import (
	"slices"
	"testing"
)

func TestFilterSetterClearsTheOtherField(t *testing.T) {
	wide := []string{"order.updated", "order.updated.>"}
	cfg := NewDefault().WithFilterSubject("order.updated").WithFilterSubjects(wide...).Build()
	if cfg.FilterSubject != "" || !slices.Equal(cfg.FilterSubjects, wide) {
		t.Fatalf("filters %q %v", cfg.FilterSubject, cfg.FilterSubjects)
	}

	wide[0] = "changed"
	if cfg.FilterSubjects[0] != "order.updated" {
		t.Fatal("builder must copy the slice")
	}

	narrow := NewDefault().WithFilterSubjects(wide...).WithFilterSubject("order.updated").Build()
	if narrow.FilterSubject != "order.updated" || narrow.FilterSubjects != nil {
		t.Fatalf("filters %q %v", narrow.FilterSubject, narrow.FilterSubjects)
	}
}
