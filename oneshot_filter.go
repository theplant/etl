package etl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/pkg/errors"
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
