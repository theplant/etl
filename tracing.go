package etl

// TableRecordCount represents the record count for a table during ETL load
type TableRecordCount struct {
	Table        string `json:"table"`
	StagingTable string `json:"stagingTable"`
	RecordCount  int    `json:"recordCount"`
}
