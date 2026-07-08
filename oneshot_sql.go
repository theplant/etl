package etl

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/qor5/go-que"
)

// OneShotJobSQLInput describes the one-shot job an operator wants to submit.
// F is the Source-defined filter schema: the filter is authored as a typed
// struct in code (compile-checked), then rendered into the SQL document.
type OneShotJobSQLInput[T any, F any] struct {
	// QueueName is the one-shot pipeline's queue.
	QueueName string
	// PageSize should match the pipeline's PageSize.
	PageSize int
	// SeedCursor is the pagination start, usually the cursor zero value
	// (e.g. &etl.Cursor{}).
	SeedCursor T
	// Filter selects the records to sync.
	Filter F
	// RetryPolicy for the job, e.g. bus.DefaultRetryPolicyFactory().
	RetryPolicy *que.RetryPolicy
	// UniqueID optionally overrides the filter-derived unique id, e.g. a
	// human-readable ticket number ("etl_oneshot_KGM-1234"). When empty,
	// OneShotUniqueID(filter) is used, so executing the same generated SQL
	// twice while the task is in flight violates the unique constraint —
	// the same duplicate-insert protection EnqueueOneShot has.
	UniqueID string
}

// BuildOneShotJobSQL renders a ready-to-run INSERT statement for goque_jobs,
// for the production workflow where a one-shot sync is submitted by handing a
// SQL statement to an operator instead of calling EnqueueOneShot from code.
// The generated job is byte-identical in behavior to one submitted via
// EnqueueOneShot: same args document, same filter-derived unique id, same
// Lockable lifecycle.
func BuildOneShotJobSQL[T any, F any](in *OneShotJobSQLInput[T, F]) (string, error) {
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

	filter, err := MarshalOneShotFilter(in.Filter)
	if err != nil {
		return "", err
	}

	req := &ExtractRequest[T]{
		After:         in.SeedCursor,
		First:         in.PageSize,
		OneShotFilter: filter,
	}
	args := que.Args(req)

	retryPolicy, err := json.Marshal(in.RetryPolicy)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal retry policy")
	}

	uniqueID := in.UniqueID
	if uniqueID == "" {
		uniqueID = OneShotUniqueID(filter)
	}

	// PostgreSQL string literals: escape by doubling single quotes.
	quote := func(s string) string {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}

	sql := fmt.Sprintf(
		`INSERT INTO goque_jobs (queue, run_at, args, retry_policy, unique_id, unique_lifecycle)
VALUES (%s, now(), %s, %s, %s, %d);`,
		quote(in.QueueName),
		quote(string(args)),
		quote(string(retryPolicy)),
		quote(uniqueID),
		que.Lockable,
	)
	return sql, nil
}
