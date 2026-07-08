package etl_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/theplant/etl"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus"
	"github.com/qor5/go-que"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// recordingSource wraps a Source and records every ExtractRequest it
// receives, so tests can assert on pagination and filter propagation.
type recordingSource struct {
	inner etl.Source[*etl.Cursor]

	mu   sync.Mutex
	reqs []*etl.ExtractRequest[*etl.Cursor]
}

var _ etl.Source[*etl.Cursor] = (*recordingSource)(nil)

func (r *recordingSource) Extract(ctx context.Context, req *etl.ExtractRequest[*etl.Cursor]) (*etl.ExtractResponse[*etl.Cursor], error) {
	r.mu.Lock()
	r.reqs = append(r.reqs, req)
	r.mu.Unlock()
	return r.inner.Extract(ctx, req)
}

func (r *recordingSource) requests() []*etl.ExtractRequest[*etl.Cursor] {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*etl.ExtractRequest[*etl.Cursor]{}, r.reqs...)
}

// failingSource always fails, to exercise the one-shot retry→expire path.
type failingSource struct {
	mu    sync.Mutex
	calls int
}

var _ etl.Source[*etl.Cursor] = (*failingSource)(nil)

func (f *failingSource) Extract(_ context.Context, _ *etl.ExtractRequest[*etl.Cursor]) (*etl.ExtractResponse[*etl.Cursor], error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return nil, errors.New("simulated extract failure")
}

func (f *failingSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// prepareOneShotSourceTestData seeds 12 active users (user01..user12), each
// with one password credential. user01..user09 will be targeted by the
// one-shot sync; user10..user12 are bystanders that must stay untouched.
func prepareOneShotSourceTestData(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := time.Now()

	for i := 1; i <= 12; i++ {
		id := fmt.Sprintf("user%02d", i)
		user := &OptimizedUser{
			ID:                  id,
			Username:            fmt.Sprintf("name_%02d", i),
			Email:               fmt.Sprintf("%s@example.com", id),
			DisplayName:         fmt.Sprintf("User %02d", i),
			Status:              "active",
			CredentialUpdatedAt: now.AddDate(0, 0, -1),
			CreatedAt:           now.AddDate(0, 0, -2),
			UpdatedAt:           now.AddDate(0, 0, -1),
		}
		require.NoError(t, db.Create(user).Error, "Failed to create %s", id)

		cred := &UserCred{
			ID:         id + "_cred",
			UserID:     id,
			CredType:   "password",
			Identifier: user.Email,
			CredValue:  "hashed_password_" + id,
			IsActive:   true,
			CreatedAt:  now.AddDate(0, 0, -2),
			UpdatedAt:  now.AddDate(0, 0, -1),
		}
		require.NoError(t, db.Create(cred).Error, "Failed to create cred for %s", id)
	}
}

// TestOneShotPipeline covers the targeted one-shot mode end to end:
// a dedicated OneShot pipeline (own queue) syncs exactly the records named by
// the filter, pages through them with the same keyset cursor as the
// incremental mode, destroys its jobs on success without enqueueing any
// time-window successor, and coexists with a normally running incremental
// pipeline without disturbing it.
func TestOneShotPipeline(t *testing.T) {
	ctx := context.Background()

	sourceDB, targetDB, pipelineSQLDB := setupTestDatabases(t, ctx)
	require.NoError(t, sourceDB.AutoMigrate(&OptimizedUser{}), "Failed to migrate optimized_users table")
	prepareOneShotSourceTestData(t, sourceDB)

	const (
		oneShotQueue     = "oneshot_identity_etl"
		incrementalQueue = "incremental_identity_etl"
	)

	countRows := func(queue string) int {
		var n int
		require.NoError(t, pipelineSQLDB.QueryRowContext(ctx,
			`SELECT count(*) FROM goque_jobs WHERE queue = $1`, queue).Scan(&n))
		return n
	}
	countPending := func(queue string) int {
		var n int
		require.NoError(t, pipelineSQLDB.QueryRowContext(ctx,
			`SELECT count(*) FROM goque_jobs WHERE queue = $1 AND expired_at IS NULL AND done_at IS NULL`, queue).Scan(&n))
		return n
	}
	countExpired := func(queue string) int {
		var n int
		require.NoError(t, pipelineSQLDB.QueryRowContext(ctx,
			`SELECT count(*) FROM goque_jobs WHERE queue = $1 AND expired_at IS NOT NULL`, queue).Scan(&n))
		return n
	}

	// All tests submit tasks the way production does: render the INSERT with
	// BuildOneShotJobSQL and execute it verbatim.
	buildOneShotSQL := func(t *testing.T, queue string, filter OptimizedUserFilter, policy *que.RetryPolicy) string {
		t.Helper()
		sqlText, err := etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, OptimizedUserFilter]{
			QueueName:   queue,
			PageSize:    4,
			SeedCursor:  &etl.Cursor{},
			Filter:      filter,
			RetryPolicy: policy,
		})
		require.NoError(t, err)
		return sqlText
	}
	execSQL := func(sqlText string) error {
		_, err := pipelineSQLDB.ExecContext(ctx, sqlText)
		return err
	}

	syncer := &optimizedIdentitySyncer{sourceDB: sourceDB, targetDB: targetDB}
	source := &recordingSource{inner: syncer}

	// A one-shot pipeline needs no Interval/ConsistencyDelay/CircuitBreaker*
	// configuration — creating it without them also verifies the relaxed
	// validation.
	oneShot, err := etl.NewPipeline(&etl.PipelineConfig[*etl.Cursor]{
		Source:      source,
		QueueDB:     pipelineSQLDB,
		QueueName:   oneShotQueue,
		PageSize:    4, // 9 targeted ids -> pages of 4+4+1, exercising in-task pagination
		OneShot:     true,
		RetryPolicy: bus.DefaultRetryPolicyFactory(),
	})
	require.NoError(t, err, "Failed to create one-shot pipeline")

	controller, err := oneShot.Start(ctx, &etl.Cursor{})
	require.NoError(t, err, "Failed to start one-shot pipeline")
	defer func() { _ = controller.Stop(context.Background()) }()

	// Unlike the incremental mode, Start must NOT have seeded any job.
	assert.Equal(t, 0, countRows(oneShotQueue), "one-shot Start must not enqueue a seed job")

	targetIDs := make([]string, 0, 9)
	for i := 1; i <= 9; i++ {
		targetIDs = append(targetIDs, fmt.Sprintf("user%02d", i))
	}
	filter, err := etl.MarshalOneShotFilter(&OptimizedUserFilter{IDs: targetIDs})
	require.NoError(t, err)

	t.Run("targeted sync of 9 ids", func(t *testing.T) {
		taskSQL := buildOneShotSQL(t, oneShotQueue, OptimizedUserFilter{IDs: targetIDs}, bus.DefaultRetryPolicyFactory())
		require.NoError(t, execSQL(taskSQL))

		// A finished one-shot task leaves its queue completely empty: every
		// page job destroyed, no successor of any kind enqueued.
		require.Eventually(t, func() bool { return countRows(oneShotQueue) == 0 },
			60*time.Second, 200*time.Millisecond, "one-shot task should drain its queue")

		// Exactly the 9 targeted users are synced, bystanders are untouched.
		var identities []Identity
		require.NoError(t, targetDB.Find(&identities).Error)
		require.Len(t, identities, 9, "only the targeted users should be synced")
		synced := make(map[string]bool, len(identities))
		for _, identity := range identities {
			synced[identity.ID] = true
		}
		for _, id := range targetIDs {
			assert.True(t, synced[id], "%s should be synced", id)
		}
		for _, bystander := range []string{"user10", "user11", "user12"} {
			assert.False(t, synced[bystander], "%s should NOT be synced", bystander)
		}

		var credentials []Credential
		require.NoError(t, targetDB.Find(&credentials).Error)
		assert.Len(t, credentials, 9, "each targeted user's credential should be synced")

		// Pagination: 3 extract pages; the Filter is carried unchanged across
		// pages while the cursor advances; the time window stays unused.
		reqs := source.requests()
		require.Len(t, reqs, 3, "9 ids with PageSize 4 should extract in 3 pages")
		for i, req := range reqs {
			assert.JSONEq(t, string(filter), string(req.OneShotFilter), "page %d should carry the original filter", i+1)
			assert.True(t, req.FromAt.IsZero(), "one-shot requests must not carry a time window")
			assert.True(t, req.BeforeAt.IsZero(), "one-shot requests must not carry a time window")
		}
		assert.Empty(t, reqs[0].After.ID, "page 1 starts from the seed cursor")
		assert.Equal(t, "user04", reqs[1].After.ID, "page 2 continues after the last row of page 1")
		assert.Equal(t, "user08", reqs[2].After.ID, "page 3 continues after the last row of page 2")

		// Completion released the filter-derived unique id (que.Lockable):
		// re-executing the very same statement is accepted and runs again.
		require.NoError(t, execSQL(taskSQL))
		require.Eventually(t, func() bool { return countRows(oneShotQueue) == 0 },
			60*time.Second, 200*time.Millisecond, "re-fired task should drain again")
	})

	t.Run("generated SQL carries dedup id and survives quoting", func(t *testing.T) {
		const sqlQueue = "oneshot_sql_etl"

		// No worker consumes this queue, so the in-flight window is
		// deterministic. The id with a single quote exercises literal escaping.
		quotedFilter := OptimizedUserFilter{IDs: []string{"o'brien", "user02"}}
		sqlText, err := etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, OptimizedUserFilter]{
			QueueName:   sqlQueue,
			PageSize:    4,
			SeedCursor:  &etl.Cursor{},
			Filter:      quotedFilter,
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		require.NoError(t, err)

		_, err = pipelineSQLDB.ExecContext(ctx, sqlText)
		require.NoError(t, err)

		// The stored row carries the document the one-shot worker expects.
		var argsJSON, uniqueID string
		var lifecycle int
		require.NoError(t, pipelineSQLDB.QueryRowContext(ctx,
			`SELECT args, unique_id, unique_lifecycle FROM goque_jobs WHERE queue = $1`, sqlQueue).
			Scan(&argsJSON, &uniqueID, &lifecycle))

		expectedFilter, err := etl.MarshalOneShotFilter(quotedFilter)
		require.NoError(t, err)
		assert.Equal(t, etl.OneShotUniqueID(expectedFilter), uniqueID)
		assert.Equal(t, int(que.Lockable), lifecycle)

		var req etl.ExtractRequest[*etl.Cursor]
		_, err = que.ParseArgs([]byte(argsJSON), &req)
		require.NoError(t, err)
		assert.Equal(t, 4, req.First)
		assert.JSONEq(t, string(expectedFilter), string(req.OneShotFilter),
			"filter must survive SQL literal quoting byte-exactly")

		// Executing the same generated SQL again while the task is in flight
		// violates the unique constraint (duplicate-insert protection).
		_, err = pipelineSQLDB.ExecContext(ctx, sqlText)
		require.Error(t, err, "duplicate execution of the generated SQL must be rejected")
		assert.Contains(t, err.Error(), "goque_jobs_unique_uidx")

		// A different filter coexists in the same queue.
		require.NoError(t, execSQL(buildOneShotSQL(t, sqlQueue, OptimizedUserFilter{IDs: []string{"user03"}}, bus.DefaultRetryPolicyFactory())))
		assert.Equal(t, 2, countRows(sqlQueue))

		_, err = pipelineSQLDB.ExecContext(ctx, `DELETE FROM goque_jobs WHERE queue = $1`, sqlQueue)
		require.NoError(t, err)
	})

	t.Run("incremental chain runs unaffected alongside", func(t *testing.T) {
		chain, err := etl.NewPipeline(&etl.PipelineConfig[*etl.Cursor]{
			Source:                  syncer,
			QueueDB:                 pipelineSQLDB,
			QueueName:               incrementalQueue,
			PageSize:                10,
			Interval:                3 * time.Second,
			ConsistencyDelay:        1 * time.Second,
			RetryPolicy:             bus.DefaultRetryPolicyFactory(),
			CircuitBreakerThreshold: 3,
			CircuitBreakerCooldown:  60 * time.Second,
		})
		require.NoError(t, err, "Failed to create incremental pipeline")

		chainController, err := chain.Start(ctx, &etl.Cursor{})
		require.NoError(t, err, "Failed to start incremental pipeline")
		defer func() { _ = chainController.Stop(context.Background()) }()

		// The chain sweeps the full history and catches up all 12 users.
		require.Eventually(t, func() bool {
			var n int64
			if err := targetDB.Model(&Identity{}).Count(&n).Error; err != nil {
				return false
			}
			return n == 12
		}, 60*time.Second, 500*time.Millisecond, "incremental chain should sync all users")

		// Fire another one-shot while the chain is live.
		require.NoError(t, execSQL(buildOneShotSQL(t, oneShotQueue, OptimizedUserFilter{IDs: []string{"user01"}}, bus.DefaultRetryPolicyFactory())))
		require.Eventually(t, func() bool { return countRows(oneShotQueue) == 0 },
			60*time.Second, 200*time.Millisecond, "second one-shot task should drain its queue")

		// The chain keeps exactly one pending window job — the one-shot task
		// neither consumed nor duplicated it.
		assert.Equal(t, 1, countPending(incrementalQueue), "the chain's single pending job must be untouched")

		// A one-shot (filtered) job manually inserted into the incremental
		// queue must be expired instead of misinterpreted.
		_, err = pipelineSQLDB.ExecContext(ctx,
			`INSERT INTO goque_jobs(queue, run_at, args, retry_policy, unique_id, unique_lifecycle)
			 VALUES ($1, now(), $2, '{}', NULL, 0)`,
			incrementalQueue,
			`[{"After":{"at":"0001-01-01T00:00:00Z","id":""},"First":10,"OneShotFilter":{"ids":["user01"]}}]`)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return countExpired(incrementalQueue) == 1 },
			60*time.Second, 200*time.Millisecond, "filtered job in incremental queue should be expired")
		assert.Equal(t, 1, countPending(incrementalQueue), "the chain must keep running after expiring the mismatched job")
	})

	t.Run("windowed job in one-shot queue is expired", func(t *testing.T) {
		_, err := pipelineSQLDB.ExecContext(ctx,
			`INSERT INTO goque_jobs(queue, run_at, args, retry_policy, unique_id, unique_lifecycle)
			 VALUES ($1, now(), $2, '{}', NULL, 0)`,
			oneShotQueue,
			`[{"After":{"at":"0001-01-01T00:00:00Z","id":""},"First":4}]`)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return countExpired(oneShotQueue) == 1 },
			60*time.Second, 200*time.Millisecond, "filter-less job in one-shot queue should be expired")
	})

	t.Run("failure retries then expires without successor", func(t *testing.T) {
		const failingQueue = "oneshot_failing_etl"

		failing := &failingSource{}
		fastRetry := &que.RetryPolicy{
			InitialInterval:        200 * time.Millisecond,
			MaxInterval:            time.Second,
			NextIntervalMultiplier: 1,
			MaxRetryCount:          1,
		}
		failingPipeline, err := etl.NewPipeline(&etl.PipelineConfig[*etl.Cursor]{
			Source:      failing,
			QueueDB:     pipelineSQLDB,
			QueueName:   failingQueue,
			PageSize:    4,
			OneShot:     true,
			RetryPolicy: fastRetry,
		})
		require.NoError(t, err)

		failingController, err := failingPipeline.Start(ctx, &etl.Cursor{})
		require.NoError(t, err)
		defer func() { _ = failingController.Stop(context.Background()) }()

		failingSQL := buildOneShotSQL(t, failingQueue, OptimizedUserFilter{IDs: []string{"user01"}}, fastRetry)
		require.NoError(t, execSQL(failingSQL))

		// While the task is in flight (pending or retrying), executing the
		// same statement again is rejected by the filter-derived unique id.
		err = execSQL(failingSQL)
		require.Error(t, err, "identical in-flight task must be rejected")
		assert.Contains(t, err.Error(), "goque_jobs_unique_uidx")

		require.Eventually(t, func() bool { return countExpired(failingQueue) == 1 },
			60*time.Second, 200*time.Millisecond, "exhausted one-shot job should be expired")
		assert.Equal(t, 1, countRows(failingQueue), "no successor job may be enqueued on failure")
		assert.Equal(t, 2, failing.callCount(), "MaxRetryCount 1 means the initial attempt plus one retry")

		// Expiry released the unique id: the same statement is accepted again.
		require.NoError(t, execSQL(failingSQL))
		require.Eventually(t, func() bool { return countExpired(failingQueue) == 2 },
			60*time.Second, 200*time.Millisecond, "re-fired task should run and expire again")
	})

	t.Run("builder and filter validation", func(t *testing.T) {
		// BuildOneShotJobSQL validates its inputs.
		_, err := etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, OptimizedUserFilter]{
			QueueName:   "validation_etl",
			Filter:      OptimizedUserFilter{IDs: []string{"user01"}},
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "missing PageSize must be rejected")

		_, err = etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, OptimizedUserFilter]{
			QueueName: "validation_etl",
			PageSize:  4,
			Filter:    OptimizedUserFilter{IDs: []string{"user01"}},
		})
		assert.Error(t, err, "missing RetryPolicy must be rejected")

		// A typed nil pointer filter would marshal to JSON null; reject it.
		_, err = etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, *OptimizedUserFilter]{
			QueueName:   "validation_etl",
			PageSize:    4,
			Filter:      nil,
			RetryPolicy: bus.DefaultRetryPolicyFactory(),
		})
		assert.Error(t, err, "nil filter must be rejected")

		// UnmarshalOneShotFilter rejects unknown fields so a typo fails loudly.
		_, err = etl.UnmarshalOneShotFilter[OptimizedUserFilter](json.RawMessage(`{"idz":["user01"]}`))
		assert.Error(t, err, "unknown filter fields must be rejected")

		// ...and rejects trailing data after the filter document.
		_, err = etl.UnmarshalOneShotFilter[OptimizedUserFilter](json.RawMessage(`{"ids":["user01"]}{"junk":1}`))
		assert.Error(t, err, "trailing data after the filter must be rejected")
	})
}
