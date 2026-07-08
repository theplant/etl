package etl_test

import (
	"encoding/json"
	"testing"

	"github.com/theplant/etl"

	"github.com/stretchr/testify/assert"
)

// TestExtractRequestString pins the staging-name derivation contract:
// pgtarget/bqtarget name staging tables from ExtractRequest.String(), so
// requests that may run concurrently must map to distinct strings. In
// particular, every one-shot task's first page carries the zero seed cursor —
// only the filter digest distinguishes it from other tasks' first pages and
// from the incremental chain's initial sweep.
func TestExtractRequestString(t *testing.T) {
	incremental := &etl.ExtractRequest[*etl.Cursor]{After: &etl.Cursor{}}
	assert.Equal(t, "00010101t000000_", incremental.String(),
		"incremental requests must keep the historical cursor-only format")

	oneShot := &etl.ExtractRequest[*etl.Cursor]{
		After:         &etl.Cursor{},
		OneShotFilter: json.RawMessage(`{"ids":["a"]}`),
	}
	assert.Equal(t, incremental.String()+"_os72cdd75a", oneShot.String(),
		"a one-shot request appends a deterministic digest of its filter")

	otherFilter := &etl.ExtractRequest[*etl.Cursor]{
		After:         &etl.Cursor{},
		OneShotFilter: json.RawMessage(`{"ids":["b"]}`),
	}
	assert.NotEqual(t, oneShot.String(), otherFilter.String(),
		"concurrent tasks with different filters must not share staging names")
}
