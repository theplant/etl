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
	"github.com/theplant/appkit/logtracing"
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
	*Config[T]
	stagingTables          map[string]string // Track staging tables for cleanup
	createStagingTableHook hook.Hook[CreateStagingTableFunc[T]]
}

var _ etl.Target = (*Target[any])(nil)

// New creates a new BigQuery target with the given configuration
func New[T any](conf *Config[T]) (*Target[T], error) {
	if conf == nil {
		return nil, errors.New("config is required")
	}

	if conf.Client == nil {
		return nil, errors.New("client is required")
	}

	if err := validateDatasetID(conf.DatasetID); err != nil {
		return nil, errors.Wrapf(err, "invalid datasetID: %s", conf.DatasetID)
	}

	if conf.Req == nil {
		return nil, errors.New("req is required")
	}

	if err := conf.Datas.Validate(); err != nil {
		return nil, err
	}

	if conf.CommitFunc == nil {
		return nil, errors.New("commitFunc is required")
	}

	// Set default TTL if not specified
	if conf.StagingTableTTL == 0 {
		conf.StagingTableTTL = 24 * time.Hour // Default TTL: 24 hours
	}

	return &Target[T]{
		Config:        conf,
		stagingTables: make(map[string]string),
	}, nil
}

// WithCreateStagingTableHook adds a hook to the target for creating staging tables
func (t *Target[T]) WithCreateStagingTableHook(hooks ...hook.Hook[CreateStagingTableFunc[T]]) *Target[T] {
	t.createStagingTableHook = hook.Prepend(t.createStagingTableHook, hooks...)
	return t
}

// IsAlreadyExistsError checks if the error is a BigQuery "Already Exists" error (409 Conflict)
func IsAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Code == http.StatusConflict
}

// TruncateTableIfExists truncates a table if the error indicates it already exists
// Returns nil if table was truncated successfully, or the original error if it's not an "Already Exists" error
func TruncateTableIfExists(ctx context.Context, err error, client *bigquery.Client, datasetID, tableName string) error {
	if !IsAlreadyExistsError(err) {
		return err
	}
	return truncateTable(ctx, client, datasetID, tableName)
}

func truncateTable(ctx context.Context, client *bigquery.Client, datasetID, tableName string) (xerr error) {
	ctx, _ = logtracing.StartSpan(ctx, "bqtarget.truncateTable")
	spanKVs := make(map[string]any)
	defer func() {
		logtracing.AppendSpanKVs(ctx, spanKVs)
		logtracing.EndSpan(ctx, xerr)
	}()

	projectID := client.Project()
	spanKVs["project_id"] = projectID
	spanKVs["dataset_id"] = datasetID
	spanKVs["table_name"] = tableName

	truncateQuery := fmt.Sprintf("TRUNCATE TABLE `%s.%s.%s`", projectID, datasetID, tableName)
	q := client.Query(truncateQuery)
	job, err := q.Run(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to run truncate query for table %s", tableName)
	}

	st, err := job.Wait(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to wait for truncate job for table %s", tableName)
	}

	if st.Err() != nil {
		return errors.Wrapf(st.Err(), "truncate job failed for table %s", tableName)
	}

	return nil
}

func createStagingTable[T any](ctx context.Context, input *CreateStagingTableInput[T]) (output *CreateStagingTableOutput, xerr error) {
	ctx, _ = logtracing.StartSpan(ctx, "bqtarget.createStagingTable")
	spanKVs := make(map[string]any)
	defer func() {
		logtracing.AppendSpanKVs(ctx, spanKVs)
		logtracing.EndSpan(ctx, xerr)
	}()

	// Validate staging table name
	if err := validateTableName(input.StagingTable); err != nil {
		return nil, errors.Wrapf(err, "invalid staging table name: %s", input.StagingTable)
	}

	// Validate target table name
	if err := validateTableName(input.TargetTable); err != nil {
		return nil, errors.Wrapf(err, "invalid target table name: %s", input.TargetTable)
	}

	client := input.Target.Client
	projectID := client.Project()
	datasetID := input.Target.DatasetID

	spanKVs["project_id"] = projectID
	spanKVs["dataset_id"] = datasetID
	spanKVs["target_table"] = input.TargetTable
	spanKVs["staging_table"] = input.StagingTable

	defer func() {
		if xerr != nil && IsAlreadyExistsError(xerr) {
			if err := truncateTable(ctx, client, datasetID, input.StagingTable); err != nil {
				xerr = errors.Wrapf(err, "failed to truncate existing staging table %s", input.StagingTable)
			} else {
				output = &CreateStagingTableOutput{StagingTable: input.StagingTable}
				xerr = nil
			}
		}
	}()

	// Create staging table using CREATE TABLE LIKE
	// BigQuery supports CREATE TABLE LIKE which copies schema, partitioning, clustering, and options
	// We don't use TEMP TABLE because for BigQuery, session management is not convenient
	// Add expiration_timestamp option to automatically clean up staging tables
	createQuery := fmt.Sprintf("CREATE TABLE `%s.%s.%s` LIKE `%s.%s.%s`",
		projectID, datasetID, input.StagingTable, projectID, datasetID, input.TargetTable)

	createQuery += fmt.Sprintf(" OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL %d SECOND))", int(input.Target.StagingTableTTL.Seconds()))

	q := client.Query(createQuery)
	job, err := q.Run(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to run create staging table query for %s", input.StagingTable)
	}

	st, err := job.Wait(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to wait for create staging table job for %s", input.StagingTable)
	}

	if st.Err() != nil {
		return nil, errors.Wrapf(st.Err(), "failed to create staging table %s", input.StagingTable)
	}

	return &CreateStagingTableOutput{StagingTable: input.StagingTable}, nil
}

// Load processes and writes the data to BigQuery target system
func (t *Target[T]) Load(ctx context.Context) (xerr error) {
	ctx, _ = logtracing.StartSpan(ctx, "bqtarget.Load")
	spanKVs := make(map[string]any)
	defer func() {
		logtracing.AppendSpanKVs(ctx, spanKVs)
		logtracing.EndSpan(ctx, xerr)
	}()

	if len(t.Datas) == 0 {
		return nil // Nothing to write
	}

	// Prepare: create or reuse staging tables in data order
	stagingSuffix := strings.ToLower(fmt.Sprintf("_stg_%s", t.Req.String()))

	t.stagingTables = make(map[string]string)
	for _, data := range t.Datas {
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
	for _, data := range t.Datas {
		stagingTableName := t.stagingTables[data.Table]

		if data.Records == nil {
			continue
		}

		records := reflect.ValueOf(data.Records)
		recordCount := records.Len()
		if recordCount == 0 {
			continue
		}

		spanKVs[fmt.Sprintf("%s_%s_record_count", data.Table, stagingTableName)] = recordCount

		// Convert records to []any for BigQuery Inserter
		recordSlice := make([]any, recordCount)
		for i := 0; i < recordCount; i++ {
			recordSlice[i] = records.Index(i).Interface()
		}

		// Use Inserter to insert data
		inserter := t.Client.Dataset(t.DatasetID).Table(stagingTableName).Inserter()

		// Put data
		if err := inserter.Put(ctx, recordSlice); err != nil {
			return errors.Wrapf(err, "failed to insert into staging table %s", stagingTableName)
		}
	}

	// Execute commit function (required)
	// Note: commitFunc may modify staging table data (e.g., deduplication, incremental updates)
	if _, err := t.CommitFunc(ctx, &CommitInput[T]{
		Target:        t,
		StagingTables: t.stagingTables,
	}); err != nil {
		return errors.Wrap(err, "commit function failed")
	}

	return nil
}

// Cleanup cleans up staging tables (only called on successful completion)
func (t *Target[T]) Cleanup(ctx context.Context) (xerr error) {
	ctx, _ = logtracing.StartSpan(ctx, "bqtarget.Cleanup")
	spanKVs := make(map[string]any)
	defer func() {
		logtracing.AppendSpanKVs(ctx, spanKVs)
		logtracing.EndSpan(ctx, xerr)
	}()

	// Drop staging tables in reverse order (reverse dependency order)
	for i := len(t.Datas) - 1; i >= 0; i-- {
		stagingTableName := t.stagingTables[t.Datas[i].Table]

		// Delete staging table
		// BigQuery doesn't support DROP TABLE IF EXISTS, so we try to delete and ignore not found errors
		table := t.Client.Dataset(t.DatasetID).Table(stagingTableName)
		if err := table.Delete(ctx); err != nil {
			// Check if error is "not found" - if so, table doesn't exist, which is fine
			if IsNotFound(err) {
				// Table doesn't exist, which is fine - continue cleanup
				continue
			}
			spanKVs[fmt.Sprintf("delete_failed_staging_table_%s", stagingTableName)] = stagingTableName
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

var identifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func validateIdentifier(name, identifierType string) error {
	if len(name) == 0 {
		return errors.Errorf("%s cannot be empty", identifierType)
	}

	if len(name) > 1024 {
		return errors.Errorf("%s exceeds maximum length of 1024 UTF-8 bytes: %d", identifierType, len(name))
	}

	if !identifierRegex.MatchString(name) {
		return errors.Errorf("%s contains invalid characters: %s (only letters, numbers, and underscores are allowed)", identifierType, name)
	}

	return nil
}

func validateTableName(name string) error {
	return validateIdentifier(name, "table name")
}

func validateDatasetID(datasetID string) error {
	return validateIdentifier(datasetID, "dataset ID")
}
