package etl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/qor5/go-que"
)

// MarshalOneShotFilter encodes a source-defined filter struct into the opaque form
// carried by ExtractRequest.OneShotFilter. The framework never interprets the
// content; each Source defines its own filter schema.
func MarshalOneShotFilter(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, errors.New("filter is nil")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal filter")
	}
	// A typed nil pointer marshals to "null" without an error; a null filter
	// would decode into a zero-value struct downstream, so reject it here.
	if bytes.Equal(b, []byte("null")) {
		return nil, errors.New("filter must not encode to null")
	}
	return b, nil
}

// UnmarshalOneShotFilter decodes ExtractRequest.OneShotFilter into the source-defined
// filter struct F. Unknown fields and trailing data are rejected so that a
// typo in a manually crafted job (e.g. "idz" instead of "ids") fails loudly
// instead of silently matching nothing.
func UnmarshalOneShotFilter[F any](filter json.RawMessage) (*F, error) {
	if len(filter) == 0 {
		return nil, errors.New("filter is empty")
	}
	dec := json.NewDecoder(bytes.NewReader(filter))
	dec.DisallowUnknownFields()
	f := new(F)
	if err := dec.Decode(f); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal filter")
	}
	if dec.More() {
		return nil, errors.New("unexpected trailing data after filter")
	}
	return f, nil
}

// OneShotUniqueID derives the queue-level unique id of a one-shot job from
// its filter bytes: "etl_oneshot_" + hex(sha256(filter)). Identical filters
// map to the same id, so with que.Lockable an identical task cannot be
// double-fired while one is in flight; completion or expiry releases the id
// and the same filter can be fired again. The mapping is byte-level:
// semantically equal but differently encoded filters (e.g. reordered ids)
// produce different ids.
func OneShotUniqueID(filter json.RawMessage) string {
	sum := sha256.Sum256(filter)
	return "etl_oneshot_" + hex.EncodeToString(sum[:])
}

// OneShotJobSQLInput describes the one-shot job to submit.
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
}

// BuildOneShotJobSQL renders the INSERT statement that submits a one-shot
// job — the submission path for one-shot sync, typically handed to an
// operator for execution against the queue database. The job carries the
// args document (ExtractRequest incl. OneShotFilter), the filter-derived
// unique id (executing the same statement twice while the task is in flight
// violates the unique constraint) and the Lockable lifecycle expected by the
// one-shot worker.
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
		quote(OneShotUniqueID(filter)),
		que.Lockable,
	)
	return sql, nil
}
