package outbox

import (
	"errors"
	"strings"
	"testing"
)

func TestErrSchemaOutdatedHasMessage(t *testing.T) {
	if !errors.Is(ErrSchemaOutdated, ErrSchemaOutdated) {
		t.Fatalf("ErrSchemaOutdated must be comparable via errors.Is")
	}

	if ErrSchemaOutdated.Error() == "" {
		t.Fatalf("ErrSchemaOutdated must carry a non-empty message")
	}
}

func TestMigrationStatementsAddV2ColumnsIdempotently(t *testing.T) {
	statements := migrationStatements()

	for _, column := range []string{"attempts", "last_error", "next_attempt_at", "headers"} {
		found := false
		for _, stmt := range statements {
			if strings.Contains(stmt, "add column if not exists "+column) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected idempotent add-column statement for %q, got %v", column, statements)
		}
	}
}

func TestMigrationStatementsCreateStateUpdatedAtIndexIdempotently(t *testing.T) {
	statements := migrationStatements()

	found := false
	for _, stmt := range statements {
		if strings.Contains(stmt, "create index if not exists") && strings.Contains(stmt, "(state, updated_at)") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected idempotent index creation on (state, updated_at), got %v", statements)
	}
}

func TestSchemaCapsZeroValueIsAllFalse(t *testing.T) {
	var caps schemaCaps

	if caps.V2 {
		t.Fatalf("zero-value schemaCaps.V2 must be false")
	}
	if caps.Retries {
		t.Fatalf("zero-value schemaCaps.Retries must be false")
	}
}

func TestOutboxEventRowTableNameMatchesOutboxEvents(t *testing.T) {
	if got := (outboxEventRow{}).TableName(); got != "outbox_events" {
		t.Fatalf("expected outboxEventRow.TableName() = %q, got %q", "outbox_events", got)
	}
}
