package pgtarget

import (
	"context"
	"database/sql"
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
	// DB is bound to the single database connection that owns the staging
	// tables. With the default session-scoped TEMP staging tables the staging
	// tables exist only on this one connection, so CommitFunc MUST run every
	// staging-table statement (the MERGE, etc.) through this DB. Reaching for
	// Target.DB or any other pooled handle runs on a different connection that
	// cannot see the staging tables and fails with SQLSTATE 42P01.
	//
	// This field intentionally shadows the DB promoted from the embedded
	// *Target, so existing CommitFuncs that already use input.DB transparently
	// target the correct connection.
	DB            *gorm.DB
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
	DB *gorm.DB
	// Req names the staging tables (suffix derived from Req.String()).
	// Caution: with UseUnloggedTable enabled, staging tables are real shared
	// tables — if multiple pipelines write to the same target table,
	// requests at the same cursor position map to the same staging name and
	// concurrent jobs would truncate each other's staged rows; namespace the
	// names per pipeline via WithCreateStagingTableHook in that case.
	// (Irrelevant for the default TEMP tables, which are session-scoped.)
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

	// Staging tables default to session-scoped TEMP tables: they live only on
	// the connection that created them. GORM draws a connection from the pool
	// per statement, so create/insert/commit would otherwise run on different
	// sessions and the staging table would not be found (SQLSTATE 42P01) —
	// intermittently, whenever the pool hands out a different connection.
	// Pin one dedicated connection and run the entire load on it. (UNLOGGED
	// staging tables are real shared tables that any connection can see, so
	// they would not strictly need this, but pinning is harmless and keeps a
	// single code path.)
	sqlDB, err := t.DB.DB()
	if err != nil {
		return errors.Wrap(err, "failed to get sql.DB")
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to acquire dedicated connection")
	}
	defer func() {
		// Conn.Close returns the connection to the pool (it does not physically
		// close it). See the DISCARD TEMP defer below for session cleanup.
		if cerr := conn.Close(); cerr != nil && xerr == nil {
			xerr = errors.Wrap(cerr, "failed to release dedicated connection")
		}
	}()

	db := bindConn(ctx, t.DB, conn)

	// For TEMP staging tables, reset the session before the connection returns
	// to the pool so no temp table rides along and pollutes a connection a
	// later job (or unrelated caller) may reuse. Runs on success and failure —
	// a TEMP table on a pooled connection can never be inspected for debugging
	// anyway. A detached context ensures a cancelled job still cleans up.
	if !t.UseUnloggedTable {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if _, derr := conn.ExecContext(cleanupCtx, "DISCARD TEMP"); derr != nil {
				spanKVs["discard_temp_error"] = derr.Error()
			}
		}()
	}

	// Record database start time at the beginning of Load (only needed for UNLOGGED tables)
	// This will be verified at the end to detect database restarts during the Load process
	// Database restart is the only silent failure scenario for UNLOGGED tables
	// TEMP TABLE doesn't need this check because it's automatically cleaned up on session end
	var initialStartedAt time.Time
	if t.UseUnloggedTable {
		var err error
		initialStartedAt, err = t.getDBStartedAt(ctx, db)
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
		DB:            db,
		StagingTables: t.stagingTables,
	}); err != nil {
		return errors.Wrap(err, "commit function failed")
	}

	// Verify database has not restarted during the Load process (only for UNLOGGED tables)
	// This is the only silent failure scenario for UNLOGGED tables
	// All other failures (write errors, I/O errors, etc.) return explicit errors
	// TEMP TABLE doesn't need this check because it's automatically cleaned up on session end
	if t.UseUnloggedTable {
		currentStartedAt, err := t.getDBStartedAt(ctx, db)
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

	// TEMP staging tables are session-scoped: they were discarded when Load
	// released its dedicated connection (see the DISCARD TEMP defer in Load),
	// so there is nothing left to drop here. Only real UNLOGGED staging tables
	// survive Load and need an explicit drop.
	if !t.UseUnloggedTable {
		t.stagingTables = make(map[string]string)
		return nil
	}

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

// getDBStartedAt returns the PostgreSQL database start time. It runs on the
// provided db so it can share Load's pinned connection: querying the pool
// instead would deadlock when the pool is capped at a single connection (Load
// already holds it).
func (t *Target[T]) getDBStartedAt(ctx context.Context, db *gorm.DB) (startedAt time.Time, xerr error) {
	ctx, _ = logtracing.StartSpan(ctx, "pgtarget.getDBStartedAt")
	defer func() {
		logtracing.EndSpan(ctx, xerr)
	}()

	if err := db.WithContext(ctx).Raw("SELECT pg_postmaster_start_time()").Scan(&startedAt).Error; err != nil {
		return time.Time{}, errors.Wrap(err, "failed to query database start time")
	}
	return startedAt, nil
}

// bindConn returns a *gorm.DB that runs every statement on the single
// dedicated connection conn while inheriting db's configuration (naming
// strategy, plugins, logger, etc.). Session copies the config and clones the
// statement, so swapping ConnPool here does not affect the shared db. This
// mirrors how gorm's own Begin binds a *sql.Tx by setting Statement.ConnPool.
//
// Do NOT "simplify" this to gorm.Open(postgres.New(postgres.Config{Conn: conn})).
// That builds a bare *gorm.DB and drops everything the caller's db was opened
// with — for gormx that means the tracing / omit-associations / soft-delete
// plugins and the QueryFields/TranslateError/CreateBatchSize config. Losing
// OmitAssociations alone would change what Create writes into the staging
// table. Setting Statement.ConnPool is the only way to keep that config while
// pinning the connection; gorm v2 exposes no public equivalent.
func bindConn(ctx context.Context, db *gorm.DB, conn *sql.Conn) *gorm.DB {
	session := db.Session(&gorm.Session{Context: ctx})
	session.ConnPool = conn
	session.Statement.ConnPool = conn
	return session
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
