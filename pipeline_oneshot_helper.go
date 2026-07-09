package etl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/qor5/go-que"
	"github.com/samber/lo"
)

// OneShotJobSQLInput describes the one-shot job to submit.
type OneShotJobSQLInput struct {
	// QueueName is the one-shot pipeline's queue.
	QueueName string
	// PageSize should match the pipeline's PageSize.
	PageSize int
	// SeedCursor is the pagination start, usually the cursor zero value
	// (e.g. &etl.Cursor{}). Its concrete type must be the pipeline's cursor
	// type T — the worker decodes the job's After back into it. Must not be
	// nil: a nil cursor would render "After":null into the job document, and
	// its meaning would then depend entirely on the Source's nil handling.
	SeedCursor any
	// Filter selects the records to sync: an instance of the Source-defined
	// filter schema (authored as a struct literal, which the compiler checks
	// on its own), rendered into the SQL document via json.Marshal.
	Filter any
	// RetryPolicy for the job, e.g. bus.DefaultRetryPolicyFactory().
	RetryPolicy *que.RetryPolicy
}

// BuildOneShotJobSQL renders the INSERT statement that submits a one-shot
// job — the submission path for one-shot sync, typically handed to an
// operator for execution against the queue database. The job carries the
// args document (ExtractRequest incl. OneShotFilter), the fixed one-shot
// unique id (at most one task per queue can be in flight: executing another
// statement meanwhile violates the unique constraint) and the Lockable
// lifecycle expected by the one-shot worker.
func BuildOneShotJobSQL(in *OneShotJobSQLInput) (string, error) {
	if in == nil {
		return "", errors.New("input is nil")
	}
	if in.QueueName == "" {
		return "", errors.New("QueueName is required")
	}
	if in.PageSize <= 0 {
		return "", errors.New("PageSize must be greater than 0")
	}
	if in.RetryPolicy == nil {
		return "", errors.New("RetryPolicy is required")
	}
	if lo.IsNil(in.SeedCursor) {
		return "", errors.New("SeedCursor is required; use the cursor zero value (e.g. &etl.Cursor{})")
	}

	if lo.IsNil(in.Filter) {
		return "", errors.New("filter is nil")
	}
	filter, err := json.Marshal(in.Filter)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal filter")
	}
	// A typed nil pointer marshals to "null" without an error; a null filter
	// would decode into a zero-value struct downstream, so reject it here.
	if bytes.Equal(filter, []byte("null")) {
		return "", errors.New("filter must not encode to null")
	}

	req := &ExtractRequest[any]{
		After:         in.SeedCursor,
		First:         in.PageSize,
		OneShotFilter: filter,
	}
	args := que.Args(req)

	retryPolicy, err := json.Marshal(in.RetryPolicy)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal retry policy")
	}

	// Values are deliberately rendered into the SQL text: the output is a
	// standalone document executed later by an operator, so no parameter
	// binding channel exists at generation time. Every injection-shaped
	// failure is closed at the interpolation points instead: all string
	// values go through QuoteLiteral, the unique id is a fixed constant,
	// unique_lifecycle is rendered as an integer, and no identifiers are
	// interpolated. Round-trip byte-exactness (quotes and backslashes in the
	// payload included) is pinned by tests.
	sql := fmt.Sprintf(
		`INSERT INTO goque_jobs (queue, run_at, args, retry_policy, unique_id, unique_lifecycle)
VALUES (%s, now(), %s, %s, %s, %d);`,
		quoteLiteral(in.QueueName),
		quoteLiteral(string(args)),
		quoteLiteral(string(retryPolicy)),
		quoteLiteral(OneShotUniqueID),
		que.Lockable,
	)
	return sql, nil
}

// quoteLiteral quotes a 'literal' (e.g. a parameter, often used to pass literal
// to DDL and other statements that do not accept parameters) to be used as part
// of an SQL statement.  For example:
//
//	exp_date := quoteLiteral("2023-01-05 15:00:00Z")
//	err := db.Exec(fmt.Sprintf("CREATE ROLE my_user VALID UNTIL %s", exp_date))
//
// Any single quotes in name will be escaped. Any backslashes (i.e. "\") will be
// replaced by two backslashes (i.e. "\\") and the C-style escape identifier
// that PostgreSQL provides ('E') will be prepended to the string.
//
// Copied from github.com/lib/pq's QuoteLiteral (strings.Replace modernized to
// strings.ReplaceAll) — vendoring the spec-frozen 8-line algorithm instead of
// importing the maintenance-mode driver.
func quoteLiteral(literal string) string {
	// This follows the PostgreSQL internal algorithm for handling quoted literals
	// from libpq, which can be found in the "PQEscapeStringInternal" function,
	// which is found in the libpq/fe-exec.c source file:
	// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/interfaces/libpq/fe-exec.c
	//
	// substitute any single-quotes (') with two single-quotes ('')
	literal = strings.ReplaceAll(literal, `'`, `''`)
	// determine if the string has any backslashes (\) in it.
	// if it does, replace any backslashes (\) with two backslashes (\\)
	// then, we need to wrap the entire string with a PostgreSQL
	// C-style escape. Per how "PQEscapeStringInternal" handles this case, we
	// also add a space before the "E"
	if strings.Contains(literal, `\`) {
		literal = strings.ReplaceAll(literal, `\`, `\\`)
		literal = ` E'` + literal + `'`
	} else {
		// otherwise, we can just wrap the literal with a pair of single quotes
		literal = `'` + literal + `'`
	}
	return literal
}
