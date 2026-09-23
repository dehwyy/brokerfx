package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"gorm.io/gorm"
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

func TestV2ColumnsQueryScopesToCurrentSchema(t *testing.T) {
	if !strings.Contains(v2ColumnsQuery, "table_schema = current_schema()") {
		t.Fatalf("v2ColumnsQuery must scope to current_schema() to avoid counting columns from another schema's outbox_events, got %q", v2ColumnsQuery)
	}
}

func TestEnsureSchemaCapsRetriesDetectionAfterError(t *testing.T) {
	r := &OutboxRelay{
		store:  &OutboxStore{},
		logger: zerolog.Nop(),
	}

	calls := 0
	wantErr := errors.New("transient db failure")
	r.detectSchemaFn = func(context.Context, *gorm.DB) (schemaCaps, error) {
		calls++
		if calls == 1 {
			return schemaCaps{}, wantErr
		}
		return schemaCaps{V2: true, Retries: true}, nil
	}

	first := r.ensureSchemaCaps(context.Background())
	if first.Retries {
		t.Fatalf("on detection error, ensureSchemaCaps must not claim outbox_retries support it hasn't verified, got %+v", first)
	}
	if first.V2 {
		t.Fatalf("on detection error, ensureSchemaCaps must not report V2 support, got %+v", first)
	}
	if r.schemaCapsResolved {
		t.Fatalf("a failed detection must not be treated as resolved, otherwise legacy caps stick forever")
	}

	second := r.ensureSchemaCaps(context.Background())
	if !second.V2 || !second.Retries {
		t.Fatalf("expected detection to succeed on retry, got %+v", second)
	}
	if calls != 2 {
		t.Fatalf("expected detectSchemaFn to run twice (initial failure + retry), ran %d times", calls)
	}

	third := r.ensureSchemaCaps(context.Background())
	if calls != 2 {
		t.Fatalf("a resolved schema must not be re-detected, detectSchemaFn ran %d times", calls)
	}
	if third != second {
		t.Fatalf("expected cached caps %+v, got %+v", second, third)
	}
}
