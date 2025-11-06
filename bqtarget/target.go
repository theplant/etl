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

// Target implements the Target interface for BigQuery
type Target[T any] struct {
	client                 *bigquery.Client
	dataset                *bigquery.Dataset
	datas                  etl.TargetDatas
	commitFunc             CommitFunc[T]
	req                    *etl.ExtractRequest[T]
	stagingTables          map[string]string // Track staging tables for cleanup
	createStagingTableHook hook.Hook[CreateStagingTableFunc[T]]
	stagingTableTTL        time.Duration // TTL for staging tables (default: 24 hours)
}

var _ etl.Target = (*Target[any])(nil)

// TODO: 搞个 *Config struct 来初始化 target ，并且依赖于 DatasetID 而非 Dataset
// New creates a new BigQuery target
func New[T any](client *bigquery.Client, dataset *bigquery.Dataset, req *etl.ExtractRequest[T], datas etl.TargetDatas, commitFunc CommitFunc[T]) (*Target[T], error) {
	if commitFunc == nil {
		return nil, errors.New("commitFunc is required")
	}

	if client == nil {
		return nil, errors.New("client is required")
	}

	if dataset == nil {
		return nil, errors.New("dataset is required")
	}

	// Validate data structure first
	if err := datas.Validate(); err != nil {
		return nil, err
	}

	return &Target[T]{
		client:          client,
		dataset:         dataset,
		datas:           datas,
		commitFunc:      commitFunc,
		req:             req,
		stagingTables:   make(map[string]string),
		stagingTableTTL: 24 * time.Hour, // Default TTL: 24 hours
	}, nil
}

// Datas returns the target datas associated with this target.
func (t *Target[T]) Datas() etl.TargetDatas {
	return t.datas
}

// Client returns the BigQuery client associated with this target.
func (t *Target[T]) Client() *bigquery.Client {
	return t.client
}

// Dataset returns the BigQuery dataset associated with this target.
func (t *Target[T]) Dataset() *bigquery.Dataset {
	return t.dataset
}

// WithCreateStagingTableHook adds a hook to the target for creating staging tables
func (t *Target[T]) WithCreateStagingTableHook(hooks ...hook.Hook[CreateStagingTableFunc[T]]) *Target[T] {
	t.createStagingTableHook = hook.Prepend(t.createStagingTableHook, hooks...)
	return t
}

// WithStagingTableTTL sets the TTL (time to live) for staging tables
func (t *Target[T]) WithStagingTableTTL(ttl time.Duration) *Target[T] {
	t.stagingTableTTL = ttl
	return t
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

	client := input.Target.Client()
	dataset := input.Target.Dataset()
	datasetID := dataset.DatasetID
	projectID := client.Project()

	// Delete staging table if it exists (ignore not found errors)
	// BigQuery doesn't support CREATE TABLE LIKE IF NOT EXISTS, and if the table exists we need to truncate it.
	// Since we need to ensure a clean state, we delete the table first (ignoring not found errors) and then create it.
	if err := dataset.Table(input.StagingTable).Delete(ctx); err != nil {
		if !IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to delete existing staging table %s", input.StagingTable)
		}
		// Table doesn't exist, which is fine - continue to create
	}

	// Create staging table using CREATE TABLE LIKE
	// BigQuery supports CREATE TABLE LIKE which copies schema, partitioning, clustering, and options
	// We don't use TEMP TABLE because for BigQuery, session management is not convenient
	// Add expiration_timestamp option to automatically clean up staging tables
	createQuery := fmt.Sprintf(`CREATE TABLE %s.%s.%s LIKE %s.%s.%s`,
		projectID, datasetID, input.StagingTable,
		projectID, datasetID, input.TargetTable)

	createQuery += fmt.Sprintf(` OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL %d SECOND))`,
		int(input.Target.stagingTableTTL.Seconds()))

	q := client.Query(createQuery)
	job, err := q.Run(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create staging table %s: job run failed", input.StagingTable)
	}

	// Wait for job to complete
	st, err := job.Wait(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create staging table %s: job wait failed", input.StagingTable)
	}

	if st.Err() != nil {
		return nil, errors.Wrapf(st.Err(), "failed to create staging table %s: job status failed", input.StagingTable)
	}

	return &CreateStagingTableOutput{StagingTable: input.StagingTable}, nil
}

// Load processes and writes the data to BigQuery target system
func (t *Target[T]) Load(ctx context.Context) error {
	if len(t.datas) == 0 {
		return nil // Nothing to write
	}

	// Prepare: create or reuse staging tables in data order
	stagingSuffix := strings.ToLower(fmt.Sprintf("_staging_%s", t.req.String()))

	t.stagingTables = make(map[string]string)
	for _, data := range t.datas {
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

		t.stagingTables[data.Table] = output.StagingTable
	}

	// Write: insert data into staging tables
	for _, data := range t.datas {
		stagingTableName := t.stagingTables[data.Table]

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
		inserter := t.dataset.Table(stagingTableName).Inserter()

		// Put data
		if err := inserter.Put(ctx, recordSlice); err != nil {
			return errors.Wrapf(err, "failed to insert into staging table %s", stagingTableName)
		}
	}

	// Execute commit function (required)
	// Note: commitFunc may modify staging table data (e.g., deduplication, incremental updates)
	if _, err := t.commitFunc(ctx, &CommitInput[T]{
		Target:        t,
		StagingTables: t.stagingTables,
	}); err != nil {
		return errors.Wrap(err, "commit function failed")
	}

	return nil
}

// Cleanup cleans up staging tables (only called on successful completion)
func (t *Target[T]) Cleanup(ctx context.Context) error {
	// Drop staging tables in reverse order (reverse dependency order)
	for i := len(t.datas) - 1; i >= 0; i-- {
		stagingTableName := t.stagingTables[t.datas[i].Table]

		// Delete staging table
		// BigQuery doesn't support DROP TABLE IF EXISTS, so we try to delete and ignore not found errors
		table := t.dataset.Table(stagingTableName)
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
	t.stagingTables = make(map[string]string)
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

// bqTableNameRegex validates that table name contains only letters, numbers, and underscores
var bqTableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

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
	if !bqTableNameRegex.MatchString(name) {
		return errors.Errorf("table name contains invalid characters: %s (only letters, numbers, and underscores are allowed)", name)
	}

	return nil
}
