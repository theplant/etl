package etl

import "encoding/json"

// TableRecordCount represents the record count for a table during ETL load
type TableRecordCount struct {
	Table        string `json:"table"`
	StagingTable string `json:"staging_table"`
	RecordCount  int    `json:"record_count"`
}

// MarshalTableRecordCounts marshals table record counts to JSON string for tracing
// Returns empty string if counts is empty or marshal fails
func MarshalTableRecordCounts(counts []TableRecordCount) string {
	if len(counts) == 0 {
		return ""
	}
	b, err := json.Marshal(counts)
	if err != nil {
		return ""
	}
	return string(b)
}
