package etl_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/theplant/etl"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-que"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helperFilter is a minimal Source-defined filter schema for exercising the
// helpers, independent of the reference example in pipeline_optimized_test.go.
// All tests in this file are pure — no databases involved.
type helperFilter struct {
	IDs []string `json:"ids,omitempty"`
}

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
			assert.Equal(t, tt.want, etl.QuoteLiteral(tt.in))
		})
	}
}

func TestMarshalOneShotFilter(t *testing.T) {
	raw, err := etl.MarshalOneShotFilter(helperFilter{IDs: []string{"a", "b"}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"ids":["a","b"]}`, string(raw))

	_, err = etl.MarshalOneShotFilter(nil)
	assert.Error(t, err, "untyped nil must be rejected")

	_, err = etl.MarshalOneShotFilter((*helperFilter)(nil))
	assert.Error(t, err, "typed nil pointer (encodes to JSON null) must be rejected")

	_, err = etl.MarshalOneShotFilter(make(chan int))
	assert.Error(t, err, "unmarshalable values must surface the marshal error")
}

func TestUnmarshalOneShotFilter(t *testing.T) {
	f, err := etl.UnmarshalOneShotFilter[helperFilter](json.RawMessage(`{"ids":["a","b"]}`))
	require.NoError(t, err)
	assert.Equal(t, &helperFilter{IDs: []string{"a", "b"}}, f)

	// Marshal → Unmarshal round-trips, including quote/backslash content.
	raw, err := etl.MarshalOneShotFilter(helperFilter{IDs: []string{"o'brien", `back\slash`}})
	require.NoError(t, err)
	back, err := etl.UnmarshalOneShotFilter[helperFilter](raw)
	require.NoError(t, err)
	assert.Equal(t, []string{"o'brien", `back\slash`}, back.IDs)

	for name, in := range map[string]json.RawMessage{
		"empty":                nil,
		"invalid JSON":         json.RawMessage(`{`),
		"unknown field (typo)": json.RawMessage(`{"idz":["a"]}`),
		"trailing data":        json.RawMessage(`{"ids":["a"]}{"junk":1}`),
		"bare null":            json.RawMessage(`null`),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := etl.UnmarshalOneShotFilter[helperFilter](in)
			assert.Error(t, err)
		})
	}
}

func TestOneShotUniqueID(t *testing.T) {
	filter := json.RawMessage(`{"ids":["a"]}`)

	id := etl.OneShotUniqueID(filter)
	sum := sha256.Sum256(filter)
	assert.Equal(t, "etl_oneshot_"+hex.EncodeToString(sum[:]), id)
	assert.Len(t, id, len("etl_oneshot_")+64, "prefix + sha256 hex must fit varchar(255)")

	assert.Equal(t, id, etl.OneShotUniqueID(json.RawMessage(`{"ids":["a"]}`)),
		"same bytes must map to the same id")
	assert.NotEqual(t, id, etl.OneShotUniqueID(json.RawMessage(`{"ids":["b"]}`)))

	// The mapping is deliberately byte-level: semantically equal but
	// differently encoded filters (e.g. reordered ids) produce different ids.
	assert.NotEqual(t,
		etl.OneShotUniqueID(json.RawMessage(`{"ids":["a","b"]}`)),
		etl.OneShotUniqueID(json.RawMessage(`{"ids":["b","a"]}`)))
}

func TestBuildOneShotJobSQL(t *testing.T) {
	t.Run("renders the full statement deterministically", func(t *testing.T) {
		sqlText, err := etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, helperFilter]{
			QueueName:  "q1",
			PageSize:   4,
			SeedCursor: &etl.Cursor{},
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
VALUES ('q1', now(), '[{"After":{"at":"0001-01-01T00:00:00Z","id":""},"First":4,"FromAt":"0001-01-01T00:00:00Z","BeforeAt":"0001-01-01T00:00:00Z","OneShotFilter":{"ids":["a"]}}]', '{"initialInterval":1000000000,"maxInterval":0,"nextIntervalMultiplier":1,"intervalRandomPercent":0,"maxRetryCount":1}', %s, %d);`,
			etl.QuoteLiteral(etl.OneShotUniqueID(json.RawMessage(`{"ids":["a"]}`))),
			que.Lockable,
		)
		assert.Equal(t, want, sqlText)
	})

	t.Run("escapes quotes and backslashes in rendered values", func(t *testing.T) {
		sqlText, err := etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, helperFilter]{
			QueueName:   "q'1",
			PageSize:    4,
			SeedCursor:  &etl.Cursor{},
			Filter:      helperFilter{IDs: []string{"o'brien", `back\slash`}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		require.NoError(t, err)
		assert.Contains(t, sqlText, `'q''1'`, "queue name quote must be doubled")
		assert.Contains(t, sqlText, `o''brien`, "filter content quote must be doubled")
		assert.Contains(t, sqlText, ` E'`, "backslash content must switch args to the E-form literal")
	})

	t.Run("validates its input", func(t *testing.T) {
		_, err := etl.BuildOneShotJobSQL[*etl.Cursor, helperFilter](nil)
		assert.Error(t, err, "nil input must be rejected")

		_, err = etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, helperFilter]{
			PageSize:    4,
			Filter:      helperFilter{IDs: []string{"a"}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "missing QueueName must be rejected")

		_, err = etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, helperFilter]{
			QueueName:   "q1",
			Filter:      helperFilter{IDs: []string{"a"}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "missing PageSize must be rejected")

		_, err = etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, helperFilter]{
			QueueName: "q1",
			PageSize:  4,
			Filter:    helperFilter{IDs: []string{"a"}},
		})
		assert.Error(t, err, "missing RetryPolicy must be rejected")

		_, err = etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, helperFilter]{
			QueueName:   "q1",
			PageSize:    4,
			Filter:      helperFilter{IDs: []string{"a"}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "nil SeedCursor must be rejected")

		_, err = etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, *helperFilter]{
			QueueName:   "q1",
			PageSize:    4,
			Filter:      nil,
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "nil (JSON null) filter must be rejected")
	})
}
