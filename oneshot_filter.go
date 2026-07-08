package etl

import (
	"bytes"
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
	return b, nil
}

// UnmarshalOneShotFilter decodes ExtractRequest.OneShotFilter into the source-defined
// filter struct F. Unknown fields are rejected so that a typo in a manually
// crafted job (e.g. "idz" instead of "ids") fails loudly instead of silently
// matching nothing.
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
	return f, nil
}
