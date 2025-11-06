package bqtarget

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"github.com/qor5/x/v3/hook"
	"github.com/theplant/etl"
	"google.golang.org/api/googleapi"
)

// CommitInput represents the input for committing staging tables to target tables
type CommitInput[T any] struct {
	*Target[T]
	StagingTables map[string]string
}

// CommitOutput represents the output of committing staging tables to target tables
type CommitOutput[T any] struct{}

// CommitFunc defines the function signature for committing staging tables to target tables
type CommitFunc[T any] func(ctx context.Context, input *CommitInput[T]) (*CommitOutput[T], error)

// CreateStagingTableInput represents the input for creating staging tables
type CreateStagingTableInput[T any] struct {
	*Target[T]
	TargetTable  string // Target table name
	StagingTable string // Staging table name
}

// CreateStagingTableOutput represents the output of creating staging tables
type CreateStagingTableOutput struct {
	StagingTable string // Staging table name
}

// CreateStagingTableFunc defines the function signature for creating staging tables
type CreateStagingTableFunc[T any] func(ctx context.Context, input *CreateStagingTableInput[T]) (*CreateStagingTableOutput, error)

// Config represents the configuration for creating a BigQuery target
type Config[T any] struct {
	Client          *bigquery.Client
	DatasetID       string
	Req             *etl.ExtractRequest[T]
	Datas           etl.TargetDatas
	CommitFunc      CommitFunc[T]
	StagingTableTTL time.Duration // TTL for staging tables (default: 24 hours)
}

// Target implements the Target interface for BigQuery
type Target[T any] struct {
	Config                 *Config[T]
	StagingTables          map[string]string // Track staging tables for cleanup
	createStagingTableHook hook.Hook[CreateStagingTableFunc[T]]
}

var _ etl.Target = (*Target[any])(nil)

// New creates a new BigQuery target with the given configuration
func New[T any](cfg *Config[T]) (*Target[T], error) {
	if cfg == nil {
		return nil, errors.New("config is required")
	}

	if cfg.Client == nil {
		return nil, errors.New("client is required")
	}

	if cfg.DatasetID == "" {
		return nil, errors.New("datasetID is required")
	}

	if cfg.Req == nil {
		return nil, errors.New("req is required")
	}

	if err := cfg.Datas.Validate(); err != nil {
		return nil, err
	}

	if cfg.CommitFunc == nil {
		return nil, errors.New("commitFunc is required")
	}

	// Set default TTL if not specified
	if cfg.StagingTableTTL == 0 {
		cfg.StagingTableTTL = 24 * time.Hour // Default TTL: 24 hours
	}

	return &Target[T]{
		Config:        cfg,
		StagingTables: make(map[string]string),
	}, nil
}

// WithCreateStagingTableHook adds a hook to the target for creating staging tables
func (t *Target[T]) WithCreateStagingTableHook(hooks ...hook.Hook[CreateStagingTableFunc[T]]) *Target[T] {
	t.createStagingTableHook = hook.Prepend(t.createStagingTableHook, hooks...)
	return t
}

// TruncateExistTable truncates a table if it already exists, or returns the original error if it's not an "Already Exists" error
func TruncateExistTable(ctx context.Context, err error, client *bigquery.Client, datasetID, tableName string) error {
	if err == nil {
		return nil
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		// Error is not a googleapi.Error, return it as-is
		return errors.Wrapf(err, "error is not 'Already Exists' for table %s", tableName)
	}

	alreadyExists := apiErr.Code == http.StatusConflict

	if alreadyExists {
		truncateQuery := fmt.Sprintf(`TRUNCATE TABLE %s.%s.%s`, client.Project(), datasetID, tableName)
		q := client.Query(truncateQuery)
		job, err := q.Run(ctx)
		if err != nil {
			return errors.Wrapf(err, "failed to truncate table %s: job run failed", tableName)
		}

		st, err := job.Wait(ctx)
		if err != nil {
			return errors.Wrapf(err, "failed to truncate table %s: job wait failed", tableName)
		}

		if st.Err() != nil {
			return errors.Wrapf(st.Err(), "failed to truncate table %s: job status failed", tableName)
		}
		// Successfully truncated, return nil
		return nil
	}

	// Error is not "Already Exists", return the original error with context
	return errors.Wrapf(err, "error is not 'Already Exists' for table %s", tableName)
}

func createStagingTable[T any](ctx context.Context, input *CreateStagingTableInput[T]) (*CreateStagingTableOutput, error) {
	// Validate staging table name
	if err := validateTableName(input.StagingTable); err != nil {
		return nil, errors.Wrapf(err, "invalid staging table name: %s", input.StagingTable)
	}

	// Validate target table name
	if err := validateTableName(input.TargetTable); err != nil {
		return nil, errors.Wrapf(err, "invalid target table name: %s", input.TargetTable)
	}

	client := input.Target.Config.Client
	projectID := client.Project()
	datasetID := input.Target.Config.DatasetID

	// Create staging table using CREATE TABLE LIKE
	// BigQuery supports CREATE TABLE LIKE which copies schema, partitioning, clustering, and options
	// We don't use TEMP TABLE because for BigQuery, session management is not convenient
	// Add expiration_timestamp option to automatically clean up staging tables
	createQuery := fmt.Sprintf(`CREATE TABLE %s.%s.%s LIKE %s.%s.%s`,
		projectID, datasetID, input.StagingTable,
		projectID, datasetID, input.TargetTable)

	createQuery += fmt.Sprintf(` OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL %d SECOND))`,
		int(input.Target.Config.StagingTableTTL.Seconds()))

	q := client.Query(createQuery)
	job, err := q.Run(ctx)
	if err != nil {
		if err := TruncateExistTable(ctx, err, client, datasetID, input.StagingTable); err != nil {
			return nil, err
		}
		return &CreateStagingTableOutput{StagingTable: input.StagingTable}, nil
	}

	// Wait for job to complete
	st, err := job.Wait(ctx)
	if err != nil {
		if err := TruncateExistTable(ctx, err, client, datasetID, input.StagingTable); err != nil {
			return nil, err
		}
		return &CreateStagingTableOutput{StagingTable: input.StagingTable}, nil
	}

	// Check job status error
	if st.Err() != nil {
		if err := TruncateExistTable(ctx, st.Err(), client, datasetID, input.StagingTable); err != nil {
			return nil, err
		}
	}

	return &CreateStagingTableOutput{StagingTable: input.StagingTable}, nil
}

// Load processes and writes the data to BigQuery target system
func (t *Target[T]) Load(ctx context.Context) error {
	if len(t.Config.Datas) == 0 {
		return nil // Nothing to write
	}

	// Prepare: create or reuse staging tables in data order
	stagingSuffix := strings.ToLower(fmt.Sprintf("_stg_%s", t.Config.Req.String()))

	t.StagingTables = make(map[string]string)
	for _, data := range t.Config.Datas {
		createStagingTableFunc := createStagingTable[T]
		if t.createStagingTableHook != nil {
			createStagingTableFunc = t.createStagingTableHook(createStagingTableFunc)
		}

		output, err := createStagingTableFunc(ctx, &CreateStagingTableInput[T]{
			Target:       t,
			TargetTable:  data.Table,
			StagingTable: data.Table + stagingSuffix,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to create staging table for %s", data.Table)
		}

		t.StagingTables[data.Table] = output.StagingTable
	}

	// Write: insert data into staging tables
	for _, data := range t.Config.Datas {
		stagingTableName := t.StagingTables[data.Table]

		if data.Records == nil {
			continue
		}

		records := reflect.ValueOf(data.Records)
		recordCount := records.Len()
		if recordCount == 0 {
			continue
		}

		// Convert records to []any for BigQuery Inserter
		recordSlice := make([]any, recordCount)
		for i := 0; i < recordCount; i++ {
			recordSlice[i] = records.Index(i).Interface()
		}

		// Use Inserter to insert data
		inserter := t.Config.Client.Dataset(t.Config.DatasetID).Table(stagingTableName).Inserter()

		// Put data
		if err := inserter.Put(ctx, recordSlice); err != nil {
			return errors.Wrapf(err, "failed to insert into staging table %s", stagingTableName)
		}
	}

	// Execute commit function (required)
	// Note: commitFunc may modify staging table data (e.g., deduplication, incremental updates)
	if _, err := t.Config.CommitFunc(ctx, &CommitInput[T]{
		Target:        t,
		StagingTables: t.StagingTables,
	}); err != nil {
		return errors.Wrap(err, "commit function failed")
	}

	return nil
}

// Cleanup cleans up staging tables (only called on successful completion)
func (t *Target[T]) Cleanup(ctx context.Context) error {
	// Drop staging tables in reverse order (reverse dependency order)
	for i := len(t.Config.Datas) - 1; i >= 0; i-- {
		stagingTableName := t.StagingTables[t.Config.Datas[i].Table]

		// Delete staging table
		// BigQuery doesn't support DROP TABLE IF EXISTS, so we try to delete and ignore not found errors
		table := t.Config.Client.Dataset(t.Config.DatasetID).Table(stagingTableName)
		if err := table.Delete(ctx); err != nil {
			// Check if error is "not found" - if so, table doesn't exist, which is fine
			if IsNotFound(err) {
				// Table doesn't exist, which is fine - continue cleanup
				continue
			}
			return errors.Wrapf(err, "failed to cleanup staging table %s", stagingTableName)
		}
	}

	// Clear the staging tables map after cleanup
	t.StagingTables = make(map[string]string)
	return nil
}

// IsNotFound checks if the error is a "not found" error (404)
// This is useful for checking if a BigQuery resource (table, dataset, etc.) doesn't exist
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Code == http.StatusNotFound
}

// tableNameRegex validates that table name contains only letters, numbers, and underscores
var tableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// validateTableName validates that a table name contains only letters, numbers, and underscores
// - Must be between 1 and 1024 UTF-8 bytes
func validateTableName(name string) error {
	if len(name) == 0 {
		return errors.New("table name cannot be empty")
	}

	// Check UTF-8 byte length (not character count)
	// In Go, len(string) returns the byte length, not the character count
	if len(name) > 1024 {
		return errors.Errorf("table name exceeds maximum length of 1024 UTF-8 bytes: %d", len(name))
	}

	// Check that all characters are letters, numbers, or underscores using regex
	if !tableNameRegex.MatchString(name) {
		return errors.Errorf("table name contains invalid characters: %s (only letters, numbers, and underscores are allowed)", name)
	}

	return nil
}
