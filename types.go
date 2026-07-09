package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Source defines the interface for reading data with type-safe cursors
type Source[T any] interface {
	// Extract reads data from source and returns a response with target and cursor
	Extract(ctx context.Context, req *ExtractRequest[T]) (*ExtractResponse[T], error)
}

// Target represents a data target with write capabilities
type Target interface {
	// Load processes and writes the data to target system
	// This internally handles prepare, write, commit operations
	Load(ctx context.Context) error

	// Cleanup cleans up resources (staging tables, temp files, etc.)
	// Only called on successful completion to allow error data debugging
	Cleanup(ctx context.Context) error
}

var TimeLayout = "20060102t150405"

// ExtractRequest represents a request to read data from source with type-safe cursor
type ExtractRequest[T any] struct {
	After    T
	First    int
	FromAt   time.Time // time interval start (inclusive), never zero for consistency
	BeforeAt time.Time // time interval end (exclusive), never zero for consistency

	// OneShotFilter carries the targeting criteria of a one-shot job (see
	// PipelineConfig.OneShot). It is opaque to the framework: the submitter
	// (see BuildOneShotJobSQL) provides it and the Source decodes and
	// interprets it, replacing the FromAt/BeforeAt time-window predicate
	// with its own filter predicate. After decoding, the Source must reject
	// a filter that yields an empty predicate (e.g. no ids), so an unusable
	// job fails loudly instead of silently syncing nothing.
	// It stays constant across all pages of one one-shot task while After
	// advances. It is empty on incremental pipeline jobs; conversely,
	// FromAt/BeforeAt are zero on one-shot jobs.
	OneShotFilter json.RawMessage `json:",omitempty"`
}

// String generates a deterministic string representation for the ExtractRequest.
// pgtarget and bqtarget derive staging table names from it, so requests that
// can run concurrently against one target must map to distinct strings: a
// one-shot request (OneShotFilter set) is prefixed with "os_", because its
// first page always carries the zero seed cursor and would otherwise collide
// with an incremental request whose cursor is still zero (a chain that has
// not synced any data yet). Incremental requests keep the historical
// cursor-only format byte-for-byte.
//
// Caution: if multiple pipelines write to the same target table, this
// cursor-derived name alone cannot tell their requests apart — two pipelines
// at the same cursor position (e.g. two one-shot tasks on different queues,
// both at the zero seed cursor) map to the same staging name. Harmless with
// session-scoped TEMP staging tables (pgtarget default), but with real shared
// staging tables (bqtarget, or pgtarget with UseUnloggedTable) concurrent
// jobs would truncate each other's staged rows — namespace the staging names
// per pipeline via the staging table hook in that case.
func (req *ExtractRequest[T]) String() string {
	var after string
	if stringer, ok := any(req.After).(fmt.Stringer); ok {
		after = stringer.String()
	} else {
		after = fmt.Sprint(req.After)
	}
	if len(req.OneShotFilter) > 0 {
		return "os_" + after
	}
	return after
}

// ExtractResponse represents the response from source data read
type ExtractResponse[T any] struct {
	Target      Target // target ready for loading data
	EndCursor   T      // cursor marking the end of current page
	HasNextPage bool   // indicates if there are more pages to read
}
