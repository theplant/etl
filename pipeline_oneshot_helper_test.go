package etl

import (
	"fmt"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-que"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helperFilter is a minimal Source-defined filter schema for exercising the
// SQL builder, independent of the reference example in
// pipeline_optimized_test.go. All tests in this file are pure — no databases
// involved.
type helperFilter struct {
	IDs []string `json:"ids,omitempty"`
}

func TestBuildOneShotJobSQL(t *testing.T) {
	t.Run("renders the full statement deterministically", func(t *testing.T) {
		sqlText, err := BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, helperFilter]{
			QueueName:  "q1",
			PageSize:   4,
			SeedCursor: &Cursor{},
			Filter:     helperFilter{IDs: []string{"a"}},
			RetryPolicy: &que.RetryPolicy{
				InitialInterval:        time.Second,
				NextIntervalMultiplier: 1,
				MaxRetryCount:          1,
			},
		})
		require.NoError(t, err)

		want := fmt.Sprintf(
			`INSERT INTO goque_jobs (queue, run_at, args, retry_policy, unique_id, unique_lifecycle)
VALUES ('q1', now(), '[{"After":{"at":"0001-01-01T00:00:00Z","id":""},"First":4,"FromAt":"0001-01-01T00:00:00Z","BeforeAt":"0001-01-01T00:00:00Z","OneShotFilter":{"ids":["a"]}}]', '{"initialInterval":1000000000,"maxInterval":0,"nextIntervalMultiplier":1,"intervalRandomPercent":0,"maxRetryCount":1}', 'etl_oneshot', %d);`,
			que.Lockable,
		)
		assert.Equal(t, want, sqlText)
	})

	t.Run("escapes quotes and backslashes in rendered values", func(t *testing.T) {
		sqlText, err := BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, helperFilter]{
			QueueName:   "q'1",
			PageSize:    4,
			SeedCursor:  &Cursor{},
			Filter:      helperFilter{IDs: []string{"o'brien", `back\slash`}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		require.NoError(t, err)
		assert.Contains(t, sqlText, `'q''1'`, "queue name quote must be doubled")
		assert.Contains(t, sqlText, `o''brien`, "filter content quote must be doubled")
		assert.Contains(t, sqlText, ` E'`, "backslash content must switch args to the E-form literal")
	})

	t.Run("validates its input", func(t *testing.T) {
		_, err := BuildOneShotJobSQL[*Cursor, helperFilter](nil)
		assert.Error(t, err, "nil input must be rejected")

		_, err = BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, helperFilter]{
			PageSize:    4,
			Filter:      helperFilter{IDs: []string{"a"}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "missing QueueName must be rejected")

		_, err = BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, helperFilter]{
			QueueName:   "q1",
			Filter:      helperFilter{IDs: []string{"a"}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "missing PageSize must be rejected")

		_, err = BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, helperFilter]{
			QueueName: "q1",
			PageSize:  4,
			Filter:    helperFilter{IDs: []string{"a"}},
		})
		assert.Error(t, err, "missing RetryPolicy must be rejected")

		_, err = BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, helperFilter]{
			QueueName:   "q1",
			PageSize:    4,
			Filter:      helperFilter{IDs: []string{"a"}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "nil SeedCursor must be rejected")

		_, err = BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, *helperFilter]{
			QueueName:   "q1",
			PageSize:    4,
			SeedCursor:  &Cursor{},
			Filter:      nil,
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "nil filter must be rejected")

		_, err = BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, nullFilter]{
			QueueName:   "q1",
			PageSize:    4,
			SeedCursor:  &Cursor{},
			Filter:      nullFilter{},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "filter encoding to JSON null must be rejected")

		_, err = BuildOneShotJobSQL(&OneShotJobSQLInput[*Cursor, chan int]{
			QueueName:   "q1",
			PageSize:    4,
			SeedCursor:  &Cursor{},
			Filter:      make(chan int),
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "unmarshalable filters must surface the marshal error")
	})
}

// nullFilter marshals to JSON null despite being a non-nil value, reaching
// the encodes-to-null guard (plain nil values are caught earlier by the nil
// check).
type nullFilter struct{}

func (nullFilter) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

// TestQuoteLiteral pins the PostgreSQL literal-quoting algorithm that
// BuildOneShotJobSQL relies on to keep rendered values inert: quote doubling,
// and the E'...' escape-string form (backslashes doubled) whenever a
// backslash is present, so the literal parses identically regardless of the
// server's standard_conforming_strings setting.
func TestQuoteLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "hello", `'hello'`},
		{"empty", "", `''`},
		{"single quote doubled", "o'brien", `'o''brien'`},
		{"multiple quotes", "a'b'c", `'a''b''c'`},
		{"backslash switches to E-form", `back\slash`, ` E'back\\slash'`},
		{"backslash and quote combined", `a'b\c`, ` E'a''b\\c'`},
		{"only a backslash", `\`, ` E'\\'`},
		{"multibyte passes through", "名字", `'名字'`},
		// The classic injection shape stays inert data inside the literal.
		{"injection shape stays inside the literal",
			`x'); DROP TABLE goque_jobs; --`,
			`'x''); DROP TABLE goque_jobs; --'`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, quoteLiteral(tt.in))
		})
	}
}
