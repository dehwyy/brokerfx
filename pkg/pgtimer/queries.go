package pgtimer

import (
	"fmt"
	"regexp"
	"strings"
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type queries struct {
	claim  string
	remove string
}

func buildQueries(table Table) (queries, error) {
	name, err := quoteTable(table.Name)
	if err != nil {
		return queries{}, err
	}

	columns := map[string]string{}
	for label, column := range map[string]string{
		"Key":       table.Key,
		"TimerName": table.TimerName,
		"Attempt":   table.Attempt,
		"DueAt":     table.DueAt,
	} {
		quoted, err := quoteColumn(
			label,
			column,
		)
		if err != nil {
			return queries{}, err
		}
		columns[label] = quoted
	}

	order := columns["DueAt"]
	if table.CreatedAt != "" {
		created, err := quoteColumn(
			"CreatedAt",
			table.CreatedAt,
		)
		if err != nil {
			return queries{}, err
		}
		order += ", " + created
	}

	touch := ""
	if table.UpdatedAt != "" {
		updated, err := quoteColumn(
			"UpdatedAt",
			table.UpdatedAt,
		)
		if err != nil {
			return queries{}, err
		}
		touch = ", " + updated + " = now()"
	}

	claim := fmt.Sprintf(
		`WITH due AS MATERIALIZED (
	SELECT ctid AS row_id, %[3]s AS original_due_at
	FROM %[1]s
	WHERE %[3]s <= now() AND %[4]s IN ?
	ORDER BY %[6]s
	LIMIT ?
	FOR UPDATE SKIP LOCKED
)
UPDATE %[1]s AS t
SET %[3]s = now() + ? * interval '1 millisecond'%[7]s
FROM due
WHERE t.ctid = due.row_id
RETURNING t.%[2]s::text AS timer_key, t.%[4]s::text AS timer_name, t.%[5]s::text AS timer_attempt, due.original_due_at, t.%[3]s AS lease_due_at, now() AS claimed_at`,
		name,
		columns["Key"],
		columns["DueAt"],
		columns["TimerName"],
		columns["Attempt"],
		order,
		touch,
	)

	remove := fmt.Sprintf(
		`DELETE FROM %s WHERE %s = ? AND %s = ? AND %s = ? AND %s = ?`,
		name,
		columns["Key"],
		columns["TimerName"],
		columns["Attempt"],
		columns["DueAt"],
	)

	return queries{
		claim:  claim,
		remove: remove,
	}, nil
}

func quoteTable(name string) (string, error) {
	parts := strings.Split(
		name,
		".",
	)
	if len(parts) > 2 {
		return "", fmt.Errorf(
			"%w: table %q, want name or schema.name",
			ErrInvalidConfig,
			name,
		)
	}

	quoted := make(
		[]string,
		0,
		len(parts),
	)
	for _, part := range parts {
		column, err := quoteColumn(
			"Table",
			part,
		)
		if err != nil {
			return "", err
		}
		quoted = append(
			quoted,
			column,
		)
	}

	return strings.Join(
		quoted,
		".",
	), nil
}

func quoteColumn(
	label string,
	name string,
) (string, error) {
	if !identifier.MatchString(name) {
		return "", fmt.Errorf(
			"%w: %s %q is not a plain identifier",
			ErrInvalidConfig,
			label,
			name,
		)
	}

	return `"` + name + `"`, nil
}
