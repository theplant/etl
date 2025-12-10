package pgtarget

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/x/v3/hook"
	"github.com/qor5/x/v3/jsonx"
	"github.com/theplant/appkit/logtracing"
	"github.com/theplant/etl"
	"gorm.io/gorm"
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
	Tx           *gorm.DB
	TargetTable  string
	StagingTable string
}

// CreateStagingTableOutput represents the output of creating staging tables
type CreateStagingTableOutput struct {
	StagingTable string
}

// CreateStagingTableFunc defines the function signature for creating staging tables
type CreateStagingTableFunc[T any] func(ctx context.Context, input *CreateStagingTableInput[T]) (*CreateStagingTableOutput, error)

// Config represents the configuration for creating a PostgreSQL target
type Config[T any] struct {
	DB               *gorm.DB
	Req              *etl.ExtractRequest[T]
	Datas            etl.TargetDatas
	CommitFunc       CommitFunc[T]
	UseUnloggedTable bool // If true, use UNLOGGED TABLE instead of TEMP TABLE. Default: false (TEMP TABLE is preferred for easier database permission management, UNLOGGED TABLE is better for traceability)
}

// Target implements the Target interface for PostgreSQL
type Target[T any] struct {
	*Config[T]
	stagingTables          map[string]string // Track staging tables for cleanup
	createStagingTableHook hook.Hook[CreateStagingTableFunc[T]]
}

var _ etl.Target = (*Target[any])(nil)

// New creates a new PostgreSQL target with the given configuration
func New[T any](conf *Config[T]) (*Target[T], error) {
	if conf == nil {
		return nil, errors.New("config is required")
	}

	if conf.DB == nil {
		return nil, errors.New("db is required")
	}

	if conf.DB.PrepareStmt {
		return nil, errors.New("PrepareStmt is not supported: it conflicts with multi-statement SQL execution which is commonly used in ETL operations")
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

func createStagingTable[T any](ctx context.Context, input *CreateStagingTableInput[T]) (output *CreateStagingTableOutput, xerr error) {
	ctx, span := logtracing.StartSpan(ctx, "pgtarget.createStagingTable")
	spanKVs := make(map[string]any)
	defer func() {
		for k, v := range spanKVs {
			span.AppendKVs(k, v)
		}
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

	// Use TEMP TABLE by default, or UNLOGGED TABLE if configured
	// TEMP TABLE is preferred for easier database permission management
	// UNLOGGED TABLE is better for traceability but requires checking database restart time to detect silent failures
	var tableType string
	if input.UseUnloggedTable {
		tableType = "UNLOGGED"
	} else {
		tableType = "TEMP"
	}

	spanKVs["target_table"] = input.TargetTable
	spanKVs["staging_table"] = input.StagingTable

	createSQL := fmt.Sprintf(`
			CREATE %s TABLE IF NOT EXISTS "%s" 
			(LIKE "%s" INCLUDING ALL);
			
			TRUNCATE TABLE "%s";
			`,
		tableType, input.StagingTable, input.TargetTable, input.StagingTable)

	if err := input.Tx.WithContext(ctx).Exec(createSQL).Error; err != nil {
		return nil, errors.Wrapf(err, "failed to create staging table %s", input.StagingTable)
	}

	return &CreateStagingTableOutput{StagingTable: input.StagingTable}, nil
}

// Load processes and writes the data to PostgreSQL target system
func (t *Target[T]) Load(ctx context.Context) (xerr error) {
	ctx, span := logtracing.StartSpan(ctx, "pgtarget.Load")
	spanKVs := make(map[string]any)
	defer func() {
		for k, v := range spanKVs {
			span.AppendKVs(k, v)
		}
		logtracing.EndSpan(ctx, xerr)
	}()

	if len(t.Datas) == 0 {
		return nil // Nothing to write
	}

	db := t.DB.WithContext(ctx)

	// Record database start time at the beginning of Load (only needed for UNLOGGED tables)
	// This will be verified at the end to detect database restarts during the Load process
	// Database restart is the only silent failure scenario for UNLOGGED tables
	// TEMP TABLE doesn't need this check because it's automatically cleaned up on session end
	var initialStartedAt time.Time
	if t.UseUnloggedTable {
		var err error
		initialStartedAt, err = t.getDBStartedAt(ctx)
		if err != nil {
			return err
		}
	}

	// Prepare: create or reuse staging tables in data order
	stagingSuffix := strings.ToLower(fmt.Sprintf("_stg_%s", t.Req.String()))

	t.stagingTables = make(map[string]string)
	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, data := range t.Datas {
			createStagingTableFunc := createStagingTable[T]
			if t.createStagingTableHook != nil {
				createStagingTableFunc = t.createStagingTableHook(createStagingTableFunc)
			}

			output, err := createStagingTableFunc(ctx, &CreateStagingTableInput[T]{
				Target:       t,
				Tx:           tx,
				TargetTable:  data.Table,
				StagingTable: data.Table + stagingSuffix,
			})
			if err != nil {
				return errors.Wrapf(err, "failed to create staging table for %s", data.Table)
			}

			t.stagingTables[data.Table] = output.StagingTable
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "failed to create staging tables")
	}
	var tableRecordCounts []etl.TableRecordCount
	// Write: insert data into staging tables
	for _, data := range t.Datas {
		stagingTable := t.stagingTables[data.Table]

		if data.Records == nil {
			continue
		}

		recordCount := reflect.ValueOf(data.Records).Len()
		if recordCount == 0 {
			continue
		}

		tableRecordCounts = append(tableRecordCounts, etl.TableRecordCount{
			Table:        data.Table,
			StagingTable: stagingTable,
			RecordCount:  recordCount,
		})

		// Insert records into staging table
		if err := db.Table(stagingTable).Create(data.Records).Error; err != nil {
			return errors.Wrapf(err, "failed to insert into staging table %s", stagingTable)
		}
	}

	spanKVs["table_record_counts"] = jsonx.MustMarshalX[string](tableRecordCounts)

	// Execute commit function (required)
	// Note: commitFunc may modify staging table data (e.g., deduplication, incremental updates)
	if _, err := t.CommitFunc(ctx, &CommitInput[T]{
		Target:        t,
		StagingTables: t.stagingTables,
	}); err != nil {
		return errors.Wrap(err, "commit function failed")
	}

	// Verify database has not restarted during the Load process (only for UNLOGGED tables)
	// This is the only silent failure scenario for UNLOGGED tables
	// All other failures (write errors, I/O errors, etc.) return explicit errors
	// TEMP TABLE doesn't need this check because it's automatically cleaned up on session end
	if t.UseUnloggedTable {
		currentStartedAt, err := t.getDBStartedAt(ctx)
		if err != nil {
			return err
		}
		if !currentStartedAt.Equal(initialStartedAt) {
			spanKVs["database_restart_detected"] = true
			spanKVs["initial_db_started_at"] = initialStartedAt.Format(time.RFC3339)
			spanKVs["current_db_started_at"] = currentStartedAt.Format(time.RFC3339)
			return errors.Errorf("database restarted during Load process (started at changed from %v to %v)",
				initialStartedAt, currentStartedAt)
		}
	}

	return nil
}

// Cleanup cleans up staging tables (only called on successful completion)
func (t *Target[T]) Cleanup(ctx context.Context) (xerr error) {
	ctx, span := logtracing.StartSpan(ctx, "pgtarget.Cleanup")
	spanKVs := make(map[string]any)
	defer func() {
		for k, v := range spanKVs {
			span.AppendKVs(k, v)
		}
		logtracing.EndSpan(ctx, xerr)
	}()

	// Drop staging tables in reverse order (reverse dependency order)
	for i := len(t.Datas) - 1; i >= 0; i-- {
		stagingTable := t.stagingTables[t.Datas[i].Table]
		dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, stagingTable)
		if err := t.DB.WithContext(ctx).Exec(dropSQL).Error; err != nil {
			spanKVs["cleanup_failed_staging_table"] = stagingTable
			return errors.Wrapf(err, "failed to cleanup staging table %s", stagingTable)
		}
	}

	// Clear the staging tables map after cleanup
	t.stagingTables = make(map[string]string)
	return nil
}

// getDBStartedAt returns the PostgreSQL database start time
func (t *Target[T]) getDBStartedAt(ctx context.Context) (startedAt time.Time, xerr error) {
	ctx, _ = logtracing.StartSpan(ctx, "pgtarget.getDBStartedAt")
	defer func() {
		logtracing.EndSpan(ctx, xerr)
	}()

	if err := t.DB.WithContext(ctx).Raw("SELECT pg_postmaster_start_time()").Scan(&startedAt).Error; err != nil {
		return time.Time{}, errors.Wrap(err, "failed to query database start time")
	}
	return startedAt, nil
}

// tableNameRegex validates that table name starts with letter or underscore,
// and contains only letters, numbers, and underscores
var tableNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validateTableName validates that a table name follows PostgreSQL identifier rules
// - Must be between 1 and 63 bytes (PostgreSQL NAMEDATALEN - 1)
// - Must start with a letter or underscore
// - Can contain letters, numbers, and underscores
func validateTableName(name string) error {
	if len(name) == 0 {
		return errors.New("table name cannot be empty")
	}

	// Check byte length (PostgreSQL identifier limit is 63 bytes)
	// In Go, len(string) returns the byte length, not the character count
	if len(name) > 63 {
		return errors.Errorf("table name exceeds maximum length of 63 bytes: %d", len(name))
	}

	// Check that table name follows PostgreSQL identifier rules
	// Must start with letter or underscore, and can contain letters, numbers, and underscores
	if !tableNameRegex.MatchString(name) {
		return errors.Errorf("table name contains invalid characters: %s (must start with letter or underscore, and can only contain letters, numbers, and underscores)", name)
	}

	return nil
}
