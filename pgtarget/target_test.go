package pgtarget_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/qor5/x/v3/gormx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/theplant/etl"
	"github.com/theplant/etl/pgtarget"
	"gorm.io/gorm"
)

// widget is a minimal target table for exercising pgtarget staging.
type widget struct {
	ID        int64 `gorm:"primaryKey"`
	Name      string
	UpdatedAt time.Time
}

// mergeWidgets upserts the staging rows into the widgets target table. It runs
// through input.DB — the handle bound to the connection that owns the staging
// table — exactly as a real CommitFunc must.
func mergeWidgets(ctx context.Context, input *pgtarget.CommitInput[*etl.Cursor]) (*pgtarget.CommitOutput[*etl.Cursor], error) {
	query := `
		MERGE INTO widgets AS t
		USING ` + input.StagingTables["widgets"] + ` AS s
		ON t.id = s.id
		WHEN MATCHED THEN
			UPDATE SET name = s.name, updated_at = s.updated_at
		WHEN NOT MATCHED THEN
			INSERT (id, name, updated_at) VALUES (s.id, s.name, s.updated_at);
	`
	if err := input.DB.WithContext(ctx).Exec(query).Error; err != nil {
		return nil, err
	}
	return &pgtarget.CommitOutput[*etl.Cursor]{}, nil
}

// startTargetDB starts a throwaway PostgreSQL, migrates widgets, and — this is
// the point of the test — configures the pool with zero idle connections. With
// MaxIdleConns(0) a connection is physically closed the moment it is returned
// to the pool, so a session-scoped TEMP table created in one statement is gone
// by the next statement unless the whole load pins a single connection. This
// turns the connection-affinity bug into a deterministic SQLSTATE 42P01.
func startTargetDB(t *testing.T, ctx context.Context) *gorm.DB {
	t.Helper()

	suite := gormx.MustStartTestSuite(ctx)
	t.Cleanup(func() { _ = suite.Stop(context.Background()) })

	db := suite.DB()
	require.NoError(t, db.AutoMigrate(&widget{}))

	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetMaxOpenConns(5)

	return db
}

func newWidgetReq() *etl.ExtractRequest[*etl.Cursor] {
	return &etl.ExtractRequest[*etl.Cursor]{
		After: &etl.Cursor{At: time.Unix(0, 0).UTC(), ID: "seed"},
		First: 100,
	}
}

// TestLoad_TempTable_ZeroIdleConns proves the load pins one connection: with
// MaxIdleConns(0) the pre-fix flow (create in a committed transaction, then
// insert/commit on fresh pool connections) fails with 42P01. It must succeed
// now.
func TestLoad_TempTable_ZeroIdleConns(t *testing.T) {
	ctx := context.Background()
	db := startTargetDB(t, ctx)
	req := newWidgetReq()

	now := time.Now().UTC().Truncate(time.Second)
	target, err := pgtarget.New(&pgtarget.Config[*etl.Cursor]{
		DB:  db,
		Req: req,
		Datas: etl.TargetDatas{
			{Table: "widgets", Records: []widget{
				{ID: 1, Name: "alpha", UpdatedAt: now},
				{ID: 2, Name: "beta", UpdatedAt: now},
			}},
		},
		CommitFunc:       mergeWidgets,
		UseUnloggedTable: false,
	})
	require.NoError(t, err)

	require.NoError(t, target.Load(ctx), "Load must pin one connection so the TEMP staging table survives")
	require.NoError(t, target.Cleanup(ctx))

	var got []widget
	require.NoError(t, db.WithContext(ctx).Order("id").Find(&got).Error)
	require.Len(t, got, 2)
	assert.Equal(t, "alpha", got[0].Name)
	assert.Equal(t, "beta", got[1].Name)
}

// TestLoad_UnloggedTable is a regression for the shared-table path: staging
// survives Load (real UNLOGGED table) and is dropped by Cleanup.
func TestLoad_UnloggedTable(t *testing.T) {
	ctx := context.Background()
	db := startTargetDB(t, ctx)
	req := newWidgetReq()
	stg := "widgets" + strings.ToLower("_stg_"+req.String())

	now := time.Now().UTC().Truncate(time.Second)
	target, err := pgtarget.New(&pgtarget.Config[*etl.Cursor]{
		DB:  db,
		Req: req,
		Datas: etl.TargetDatas{
			{Table: "widgets", Records: []widget{{ID: 1, Name: "alpha", UpdatedAt: now}}},
		},
		CommitFunc:       mergeWidgets,
		UseUnloggedTable: true,
	})
	require.NoError(t, err)

	require.NoError(t, target.Load(ctx))

	// Unlogged staging table is real and shared: it is still visible from a
	// pooled connection after Load.
	tableExists := func() bool {
		var exists bool
		require.NoError(t, db.WithContext(ctx).Raw("SELECT to_regclass(?) IS NOT NULL", stg).Scan(&exists).Error)
		return exists
	}
	require.True(t, tableExists(), "unlogged staging table should survive Load")

	require.NoError(t, target.Cleanup(ctx))
	assert.False(t, tableExists(), "Cleanup should drop the unlogged staging table")

	var got []widget
	require.NoError(t, db.WithContext(ctx).Find(&got).Error)
	require.Len(t, got, 1)
}
