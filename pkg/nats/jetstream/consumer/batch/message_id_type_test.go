package batch

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type fakePool struct {
	id int
}

func (*fakePool) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, errors.New("fake")
}

func (*fakePool) ExecContext(context.Context, string, ...interface{}) (sql.Result, error) {
	return nil, errors.New("fake")
}

func (*fakePool) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("fake")
}

func (*fakePool) QueryRowContext(context.Context, string, ...interface{}) *sql.Row {
	return nil
}

type sliceFakePool struct {
	ids []int
}

func (sliceFakePool) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, errors.New("fake")
}

func (sliceFakePool) ExecContext(context.Context, string, ...interface{}) (sql.Result, error) {
	return nil, errors.New("fake")
}

func (sliceFakePool) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("fake")
}

func (sliceFakePool) QueryRowContext(context.Context, string, ...interface{}) *sql.Row {
	return nil
}

type stubDialector struct {
	pool gorm.ConnPool
}

func (d stubDialector) Name() string { return "stub" }

func (d stubDialector) Initialize(db *gorm.DB) error {
	db.ConnPool = d.pool

	return nil
}

func (d stubDialector) Migrator(*gorm.DB) gorm.Migrator { return nil }

func (d stubDialector) DataTypeOf(*schema.Field) string { return "" }

func (d stubDialector) DefaultValueOf(*schema.Field) clause.Expression { return nil }

func (d stubDialector) BindVarTo(writer clause.Writer, _ *gorm.Statement, _ interface{}) {
	writer.WriteByte('?')
}

func (d stubDialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteString(str)
}

func (d stubDialector) Explain(sql string, _ ...interface{}) string { return sql }

type loaderStub struct {
	calls atomic.Int64
	value func(*gorm.DB) (sql.NullString, error)
}

func stubLoader(t *testing.T, value func(*gorm.DB) (sql.NullString, error)) *loaderStub {
	t.Helper()

	stub := &loaderStub{value: value}
	prev := loadMessageIDType
	loadMessageIDType = func(conn *gorm.DB) (sql.NullString, error) {
		stub.calls.Add(1)

		return stub.value(conn)
	}
	t.Cleanup(func() { loadMessageIDType = prev })

	return stub
}

func fixedType(dataType string) func(*gorm.DB) (sql.NullString, error) {
	return func(*gorm.DB) (sql.NullString, error) {
		return sql.NullString{String: dataType, Valid: true}, nil
	}
}

func rawConn(pool gorm.ConnPool) *gorm.DB {
	return &gorm.DB{Config: &gorm.Config{ConnPool: pool}}
}

func TestMessageIDTypeReadOncePerPoolAcrossWithContext(t *testing.T) {
	stub := stubLoader(t, fixedType("uuid"))

	db, err := gorm.Open(
		stubDialector{pool: &fakePool{id: 1}},
		&gorm.Config{DisableAutomaticPing: true},
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		got, err := messageIDType(db.WithContext(context.Background()))
		if err != nil {
			t.Fatal(err)
		}
		if got != "uuid" {
			t.Fatalf("type %q", got)
		}
	}

	if stub.calls.Load() != 1 {
		t.Fatalf("loader calls %d", stub.calls.Load())
	}
}

func TestMessageIDTypeMapping(t *testing.T) {
	cases := map[string]string{
		"uuid":              "uuid",
		"text":              "text",
		"character varying": "text",
		"bigint":            "",
	}

	for dataType, want := range cases {
		stub := stubLoader(t, fixedType(dataType))
		conn := rawConn(&fakePool{})

		for i := 0; i < 2; i++ {
			got, err := messageIDType(conn)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("%s: type %q want %q", dataType, got, want)
			}
		}

		if stub.calls.Load() != 1 {
			t.Fatalf("%s: loader calls %d", dataType, stub.calls.Load())
		}
	}
}

func TestMessageIDTypeMissingColumnNotCached(t *testing.T) {
	stub := stubLoader(t, func(*gorm.DB) (sql.NullString, error) {
		return sql.NullString{}, nil
	})
	conn := rawConn(&fakePool{})

	for i := 0; i < 2; i++ {
		got, err := messageIDType(conn)
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("type %q", got)
		}
	}

	if stub.calls.Load() != 2 {
		t.Fatalf("loader calls %d", stub.calls.Load())
	}
}

func TestMessageIDTypeErrorNotCached(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	stub := stubLoader(t, func(*gorm.DB) (sql.NullString, error) {
		if fail.Load() {
			return sql.NullString{}, errors.New("boom")
		}

		return sql.NullString{String: "uuid", Valid: true}, nil
	})
	conn := rawConn(&fakePool{})

	if _, err := messageIDType(conn); err == nil {
		t.Fatal("expected error")
	}

	fail.Store(false)

	for i := 0; i < 2; i++ {
		got, err := messageIDType(conn)
		if err != nil {
			t.Fatal(err)
		}
		if got != "uuid" {
			t.Fatalf("type %q", got)
		}
	}

	if stub.calls.Load() != 2 {
		t.Fatalf("loader calls %d", stub.calls.Load())
	}
}

func TestMessageIDTypeSeparatesPools(t *testing.T) {
	stub := stubLoader(t, func(conn *gorm.DB) (sql.NullString, error) {
		if conn.Config.ConnPool.(*fakePool).id == 1 {
			return sql.NullString{String: "uuid", Valid: true}, nil
		}

		return sql.NullString{String: "text", Valid: true}, nil
	})
	first := rawConn(&fakePool{id: 1})
	second := rawConn(&fakePool{id: 2})

	for i := 0; i < 2; i++ {
		gotFirst, err := messageIDType(first)
		if err != nil {
			t.Fatal(err)
		}
		gotSecond, err := messageIDType(second)
		if err != nil {
			t.Fatal(err)
		}
		if gotFirst != "uuid" || gotSecond != "text" {
			t.Fatalf("types %q %q", gotFirst, gotSecond)
		}
	}

	if stub.calls.Load() != 2 {
		t.Fatalf("loader calls %d", stub.calls.Load())
	}
}

func TestMessageIDTypeIncomparablePoolNoPanic(t *testing.T) {
	stub := stubLoader(t, fixedType("uuid"))
	conn := rawConn(sliceFakePool{ids: []int{1}})

	for i := 0; i < 3; i++ {
		got, err := messageIDType(conn)
		if err != nil {
			t.Fatal(err)
		}
		if got != "uuid" {
			t.Fatalf("type %q", got)
		}
	}

	if stub.calls.Load() != 3 {
		t.Fatalf("loader calls %d", stub.calls.Load())
	}
}

func TestMessageIDTypeConcurrentCopies(t *testing.T) {
	stubLoader(t, fixedType("uuid"))

	db, err := gorm.Open(
		stubDialector{pool: &fakePool{id: 7}},
		&gorm.Config{DisableAutomaticPing: true},
	)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([]string, 32)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := messageIDType(db.WithContext(context.Background()))
			if err != nil {
				t.Error(err)

				return
			}
			results[i] = got
		}(i)
	}
	wg.Wait()

	for i, got := range results {
		if got != "uuid" {
			t.Fatalf("result %d type %q", i, got)
		}
	}
}
