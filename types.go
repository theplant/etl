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
	// (see BuildOneShotJobSQL) provides it and the Source interprets it,
	// replacing the FromAt/BeforeAt time-window predicate with its own
	// filter predicate — use UnmarshalOneShotFilter to decode it strictly.
	// It stays constant across all pages of one one-shot task while After
	// advances. It is empty on incremental pipeline jobs; conversely,
	// FromAt/BeforeAt are zero on one-shot jobs.
	OneShotFilter json.RawMessage `json:",omitempty"`
}

// String generates a deterministic string representation for the ExtractRequest
func (req *ExtractRequest[T]) String() string {
	var after string
	if stringer, ok := any(req.After).(fmt.Stringer); ok {
		after = stringer.String()
	} else {
		after = fmt.Sprint(req.After)
	}
	return after
}

// ExtractResponse represents the response from source data read
type ExtractResponse[T any] struct {
	Target      Target // target ready for loading data
	EndCursor   T      // cursor marking the end of current page
	HasNextPage bool   // indicates if there are more pages to read
}
