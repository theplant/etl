package etl_test

import (
	"encoding/json"
	"testing"

	"github.com/theplant/etl"

	"github.com/stretchr/testify/assert"
)

// TestExtractRequestString pins the staging-name derivation contract:
// pgtarget/bqtarget name staging tables from ExtractRequest.String(), so
// requests that can run concurrently against one target must map to distinct
// strings. A one-shot task's first page always carries the zero seed cursor,
// which an incremental chain also carries until it has synced its first
// record — the mode prefix keeps the two apart deterministically.
func TestExtractRequestString(t *testing.T) {
	incremental := &etl.ExtractRequest[*etl.Cursor]{After: &etl.Cursor{}}
	assert.Equal(t, "00010101t000000_", incremental.String(),
		"incremental requests must keep the historical cursor-only format")

	oneShot := &etl.ExtractRequest[*etl.Cursor]{
		After:         &etl.Cursor{},
		OneShotFilter: json.RawMessage(`{"ids":["a"]}`),
	}
	assert.Equal(t, "os_"+incremental.String(), oneShot.String(),
		"one-shot requests are prefixed so they never share a staging name with the chain")
}
