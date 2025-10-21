package pgtarget

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/x/v3/hook"
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

// Target implements the Target interface for PostgreSQL
type Target[T any] struct {
	db                     *gorm.DB
	datas                  etl.TargetDatas
	commitFunc             CommitFunc[T]
	req                    *etl.ExtractRequest[T]
	stagingTables          map[string]string // Track staging tables for cleanup
	createStagingTableHook hook.Hook[CreateStagingTableFunc[T]]
}

var _ etl.Target = (*Target[any])(nil)

// New creates a new PostgreSQL target
func New[T any](db *gorm.DB, req *etl.ExtractRequest[T], datas etl.TargetDatas, commitFunc CommitFunc[T]) (*Target[T], error) {
	if commitFunc == nil {
		return nil, errors.New("commitFunc is required")
	}

	// Validate data structure first
	if err := datas.Validate(); err != nil {
		return nil, err
	}

	// Filter out empty tables to avoid creating unnecessary staging tables
	filteredDatas := datas.FilterNonEmpty()

	if db.PrepareStmt {
		return nil, errors.New("PrepareStmt is not supported: it conflicts with multi-statement SQL execution which is commonly used in ETL operations")
	}

	return &Target[T]{
		db:            db,
		datas:         filteredDatas,
		commitFunc:    commitFunc,
		req:           req,
		stagingTables: make(map[string]string),
	}, nil
}

// Datas returns the target datas associated with this target.
func (t *Target[T]) Datas() etl.TargetDatas {
	return t.datas
}

// DB returns the GORM DB instance associated with this target.
func (t *Target[T]) DB() *gorm.DB {
	return t.db
}

// WithCreateStagingTableHook adds a hook to the target for creating staging tables
func (t *Target[T]) WithCreateStagingTableHook(hooks ...hook.Hook[CreateStagingTableFunc[T]]) *Target[T] {
	t.createStagingTableHook = hook.Prepend(t.createStagingTableHook, hooks...)
	return t
}

func createStagingTable[T any](ctx context.Context, input *CreateStagingTableInput[T]) (*CreateStagingTableOutput, error) {
	createSQL := fmt.Sprintf(`
			CREATE UNLOGGED TABLE IF NOT EXISTS %s 
			(LIKE %s INCLUDING ALL);
			
			TRUNCATE TABLE %s;
			`,
		input.StagingTable, input.TargetTable, input.StagingTable)

	if err := input.Tx.WithContext(ctx).Exec(createSQL).Error; err != nil {
		return nil, errors.Wrapf(err, "failed to create staging table %s", input.StagingTable)
	}

	return &CreateStagingTableOutput{StagingTable: input.StagingTable}, nil
}

// Load processes and writes the data to PostgreSQL target system
func (t *Target[T]) Load(ctx context.Context) error {
	if len(t.datas) == 0 {
		return nil // Nothing to write
	}

	db := t.db.WithContext(ctx)

	// Record database start time at the beginning of Load
	// This will be verified at the end to detect database restarts during the Load process
	// Database restart is the only silent failure scenario for UNLOGGED tables
	initialStartedAt, err := t.getDBStartedAt(ctx)
	if err != nil {
		return err
	}

	// Prepare: create or reuse staging tables in data order
	stagingSuffix := strings.ToLower(fmt.Sprintf("_staging_%s", t.req.String()))

	t.stagingTables = make(map[string]string)
	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, data := range t.datas {
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

	// Write: insert data into staging tables
	for _, data := range t.datas {
		stagingTable := t.stagingTables[data.Table]

		if data.Records == nil {
			continue
		}

		recordCount := reflect.ValueOf(data.Records).Len()
		if recordCount == 0 {
			continue
		}

		// Insert records into staging table
		if err := db.Table(stagingTable).Create(data.Records).Error; err != nil {
			return errors.Wrapf(err, "failed to insert into staging table %s", stagingTable)
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

	// Verify database has not restarted during the Load process
	// This is the only silent failure scenario for UNLOGGED tables
	// All other failures (write errors, I/O errors, etc.) return explicit errors
	currentStartedAt, err := t.getDBStartedAt(ctx)
	if err != nil {
		return err
	}
	if !currentStartedAt.Equal(initialStartedAt) {
		return errors.Errorf("database restarted during Load process (started at changed from %v to %v)",
			initialStartedAt, currentStartedAt)
	}

	return nil
}

// Cleanup cleans up staging tables (only called on successful completion)
func (t *Target[T]) Cleanup(ctx context.Context) error {
	// Drop staging tables in reverse order (reverse dependency order)
	for i := len(t.datas) - 1; i >= 0; i-- {
		stagingTable := t.stagingTables[t.datas[i].Table]
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", stagingTable)
		if err := t.db.WithContext(ctx).Exec(dropSQL).Error; err != nil {
			return errors.Wrapf(err, "failed to cleanup staging table %s", stagingTable)
		}

	}

	// Clear the staging tables map after cleanup
	t.stagingTables = make(map[string]string)
	return nil
}

// getDBStartedAt returns the PostgreSQL database start time
func (t *Target[T]) getDBStartedAt(ctx context.Context) (time.Time, error) {
	var startedAt time.Time
	if err := t.db.WithContext(ctx).Raw("SELECT pg_postmaster_start_time()").Scan(&startedAt).Error; err != nil {
		return time.Time{}, errors.Wrap(err, "failed to query database start time")
	}
	return startedAt, nil
}
