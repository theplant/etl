package etl_test

import (
	"context"
	"database/sql"
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

// ====== Test doubles ======

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

// ====== Shared test environment ======

// oneShotPageSize is used for every one-shot pipeline and generated SQL in
// these tests: 9 targeted ids -> pages of 4+4+1, exercising in-task pagination.
const oneShotPageSize = 4

func userID(i int) string { return fmt.Sprintf("user%02d", i) }

func userIDs(from, to int) []string {
	ids := make([]string, 0, to-from+1)
	for i := from; i <= to; i++ {
		ids = append(ids, userID(i))
	}
	return ids
}

// oneShotEnv bundles what every subtest needs: the three databases (shared
// because the testcontainers are expensive to start), the seeded source rows,
// and helpers that submit tasks exactly the way production does — render the
// INSERT with BuildOneShotJobSQL and execute it verbatim.
type oneShotEnv struct {
	ctx      context.Context
	sourceDB *gorm.DB
	targetDB *gorm.DB
	queueDB  *sql.DB
}

// newOneShotEnv starts the databases and seeds 12 active users
// (user01..user12), each with one password credential.
func newOneShotEnv(t *testing.T) *oneShotEnv {
	ctx := context.Background()
	sourceDB, targetDB, queueDB := setupTestDatabases(t, ctx)
	require.NoError(t, sourceDB.AutoMigrate(&OptimizedUser{}), "Failed to migrate optimized_users table")

	e := &oneShotEnv{ctx: ctx, sourceDB: sourceDB, targetDB: targetDB, queueDB: queueDB}
	e.seedUsers(t, 12)
	return e
}

func (e *oneShotEnv) seedUsers(t *testing.T, n int) {
	t.Helper()
	now := time.Now()
	for i := 1; i <= n; i++ {
		id := userID(i)
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
		require.NoError(t, e.sourceDB.Create(user).Error, "Failed to create %s", id)

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
		require.NoError(t, e.sourceDB.Create(cred).Error, "Failed to create cred for %s", id)
	}
}

func (e *oneShotEnv) syncer() *optimizedIdentitySyncer {
	return &optimizedIdentitySyncer{sourceDB: e.sourceDB, targetDB: e.targetDB}
}

// --- goque_jobs observation ---

func (e *oneShotEnv) countJobs(t *testing.T, queue string) int {
	t.Helper()
	var n int
	require.NoError(t, e.queueDB.QueryRowContext(e.ctx,
		`SELECT count(*) FROM goque_jobs WHERE queue = $1`, queue).Scan(&n))
	return n
}

func (e *oneShotEnv) countPending(t *testing.T, queue string) int {
	t.Helper()
	var n int
	require.NoError(t, e.queueDB.QueryRowContext(e.ctx,
		`SELECT count(*) FROM goque_jobs WHERE queue = $1 AND expired_at IS NULL AND done_at IS NULL`, queue).Scan(&n))
	return n
}

func (e *oneShotEnv) countExpired(t *testing.T, queue string) int {
	t.Helper()
	var n int
	require.NoError(t, e.queueDB.QueryRowContext(e.ctx,
		`SELECT count(*) FROM goque_jobs WHERE queue = $1 AND expired_at IS NOT NULL`, queue).Scan(&n))
	return n
}

// waitDrained waits until the queue has no rows at all — for a one-shot task
// that means every page job was destroyed and no successor was enqueued.
func (e *oneShotEnv) waitDrained(t *testing.T, queue string) {
	t.Helper()
	require.Eventually(t, func() bool { return e.countJobs(t, queue) == 0 },
		60*time.Second, 200*time.Millisecond, "queue %s should drain", queue)
}

func (e *oneShotEnv) waitExpired(t *testing.T, queue string, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return e.countExpired(t, queue) == n },
		60*time.Second, 200*time.Millisecond, "queue %s should have %d expired job(s)", queue, n)
}

// --- production-style submission ---

// buildSQL renders the production submission statement. A nil policy means
// the default retry policy.
func (e *oneShotEnv) buildSQL(t *testing.T, queue string, filter OptimizedUserFilter, policy *que.RetryPolicy) string {
	t.Helper()
	if policy == nil {
		policy = bus.DefaultRetryPolicyFactory()
	}
	sqlText, err := etl.BuildOneShotJobSQL(&etl.OneShotJobSQLInput[*etl.Cursor, OptimizedUserFilter]{
		QueueName:   queue,
		PageSize:    oneShotPageSize,
		SeedCursor:  &etl.Cursor{},
		Filter:      filter,
		RetryPolicy: policy,
	})
	require.NoError(t, err)
	return sqlText
}

// exec executes a statement verbatim, as an operator would.
func (e *oneShotEnv) exec(sqlText string) error {
	_, err := e.queueDB.ExecContext(e.ctx, sqlText)
	return err
}

// submit is the happy-path shorthand: render and execute in one step.
func (e *oneShotEnv) submit(t *testing.T, queue string, filter OptimizedUserFilter) {
	t.Helper()
	require.NoError(t, e.exec(e.buildSQL(t, queue, filter, nil)))
}

// insertRawJob inserts a hand-written job row (no unique id), for testing how
// workers treat jobs that did not come from BuildOneShotJobSQL.
func (e *oneShotEnv) insertRawJob(t *testing.T, queue, args string) {
	t.Helper()
	_, err := e.queueDB.ExecContext(e.ctx,
		`INSERT INTO goque_jobs(queue, run_at, args, retry_policy, unique_id, unique_lifecycle)
		 VALUES ($1, now(), $2, '{}', NULL, 0)`, queue, args)
	require.NoError(t, err)
}

// --- pipelines ---

// startOneShot boots a one-shot worker on its own queue and stops it when the
// (sub)test ends. It also verifies the one-shot Start contract: no seed job.
// The config deliberately omits Interval/ConsistencyDelay/CircuitBreaker* —
// creating it without them also verifies the relaxed validation. A nil policy
// means the default retry policy.
func (e *oneShotEnv) startOneShot(t *testing.T, queue string, source etl.Source[*etl.Cursor], policy *que.RetryPolicy) {
	t.Helper()
	if policy == nil {
		policy = bus.DefaultRetryPolicyFactory()
	}
	pipeline, err := etl.NewPipeline(&etl.PipelineConfig[*etl.Cursor]{
		Source:      source,
		QueueDB:     e.queueDB,
		QueueName:   queue,
		PageSize:    oneShotPageSize,
		OneShot:     true,
		RetryPolicy: policy,
	})
	require.NoError(t, err, "Failed to create one-shot pipeline")

	controller, err := pipeline.Start(e.ctx, &etl.Cursor{})
	require.NoError(t, err, "Failed to start one-shot pipeline")
	t.Cleanup(func() { _ = controller.Stop(context.Background()) })

	assert.Equal(t, 0, e.countJobs(t, queue), "one-shot Start must not enqueue a seed job")
}

// startChain boots an incremental pipeline on its own queue and stops it when
// the (sub)test ends.
func (e *oneShotEnv) startChain(t *testing.T, queue string) {
	t.Helper()
	pipeline, err := etl.NewPipeline(&etl.PipelineConfig[*etl.Cursor]{
		Source:                  e.syncer(),
		QueueDB:                 e.queueDB,
		QueueName:               queue,
		PageSize:                10,
		Interval:                3 * time.Second,
		ConsistencyDelay:        1 * time.Second,
		RetryPolicy:             bus.DefaultRetryPolicyFactory(),
		CircuitBreakerThreshold: 3,
		CircuitBreakerCooldown:  60 * time.Second,
	})
	require.NoError(t, err, "Failed to create incremental pipeline")

	controller, err := pipeline.Start(e.ctx, &etl.Cursor{})
	require.NoError(t, err, "Failed to start incremental pipeline")
	t.Cleanup(func() { _ = controller.Stop(context.Background()) })
}

// --- target observation ---

func (e *oneShotEnv) syncedIdentityIDs(t *testing.T) map[string]bool {
	t.Helper()
	var identities []Identity
	require.NoError(t, e.targetDB.Find(&identities).Error)
	synced := make(map[string]bool, len(identities))
	for _, identity := range identities {
		synced[identity.ID] = true
	}
	return synced
}

// ====== Tests ======

// TestOneShotPipeline covers the targeted one-shot mode end to end: a
// one-shot pipeline syncs exactly the records named by the filter, pages
// through them with the same keyset cursor as the incremental mode, destroys
// its jobs on success without enqueueing any time-window successor, and
// coexists with a normally running incremental pipeline.
//
// The subtests share the seeded databases (containers are expensive) but each
// owns its queues and workers. One ordering dependency is deliberate:
// "targeted sync of 9 ids" asserts that bystanders are untouched, so it must
// run before "incremental chain...", which syncs everything.
func TestOneShotPipeline(t *testing.T) {
	env := newOneShotEnv(t)

	t.Run("targeted sync of 9 ids", func(t *testing.T) {
		const queue = "oneshot_sync_etl"
		source := &recordingSource{inner: env.syncer()}
		env.startOneShot(t, queue, source, nil)

		targeted := OptimizedUserFilter{IDs: userIDs(1, 9)}
		taskSQL := env.buildSQL(t, queue, targeted, nil)
		require.NoError(t, env.exec(taskSQL))
		env.waitDrained(t, queue)

		// Exactly the 9 targeted users (and their credentials) are synced;
		// bystanders are untouched.
		synced := env.syncedIdentityIDs(t)
		require.Len(t, synced, 9, "only the targeted users should be synced")
		for _, id := range targeted.IDs {
			assert.True(t, synced[id], "%s should be synced", id)
		}
		for _, bystander := range userIDs(10, 12) {
			assert.False(t, synced[bystander], "%s should NOT be synced", bystander)
		}
		var credentials []Credential
		require.NoError(t, env.targetDB.Find(&credentials).Error)
		assert.Len(t, credentials, 9, "each targeted user's credential should be synced")

		// Pagination: 3 extract pages; the filter is carried unchanged across
		// pages while the cursor advances; the time window stays unused.
		expectedFilter, err := json.Marshal(targeted)
		require.NoError(t, err)
		reqs := source.requests()
		require.Len(t, reqs, 3, "9 ids with PageSize 4 should extract in 3 pages")
		for i, req := range reqs {
			assert.JSONEq(t, string(expectedFilter), string(req.OneShotFilter), "page %d should carry the original filter", i+1)
			assert.True(t, req.FromAt.IsZero(), "one-shot requests must not carry a time window")
			assert.True(t, req.BeforeAt.IsZero(), "one-shot requests must not carry a time window")
		}
		assert.Empty(t, reqs[0].After.ID, "page 1 starts from the seed cursor")
		assert.Equal(t, "user04", reqs[1].After.ID, "page 2 continues after the last row of page 1")
		assert.Equal(t, "user08", reqs[2].After.ID, "page 3 continues after the last row of page 2")

		// Completion released the fixed one-shot unique id (que.Lockable):
		// re-executing the very same statement is accepted and runs again.
		require.NoError(t, env.exec(taskSQL))
		env.waitDrained(t, queue)
	})

	// The empty-predicate check is the load-bearing guard of the consumer-side
	// decode (strict decoding was deliberately dropped): every unusable filter
	// collapses to either a decode error or an empty predicate, and must fail
	// loudly instead of silently syncing nothing. Both guards fire before any
	// database access, so these are direct Extract calls.
	t.Run("unusable filters fail loudly", func(t *testing.T) {
		for name, bad := range map[string]json.RawMessage{
			"empty object":         json.RawMessage(`{}`),
			"empty ids":            json.RawMessage(`{"ids":[]}`),
			"typo'd field (idz)":   json.RawMessage(`{"idz":["user01"]}`),
			"bare null":            json.RawMessage(`null`),
			"invalid JSON":         json.RawMessage(`{`),
			"wrong shape (string)": json.RawMessage(`"user01"`),
		} {
			t.Run(name, func(t *testing.T) {
				_, err := env.syncer().Extract(env.ctx, &etl.ExtractRequest[*etl.Cursor]{
					First:         oneShotPageSize,
					OneShotFilter: bad,
				})
				assert.Error(t, err)
			})
		}
	})

	t.Run("generated SQL carries dedup id and survives quoting", func(t *testing.T) {
		const queue = "oneshot_sqldoc_etl"
		// No worker consumes this queue, so the in-flight window is
		// deterministic. The ids exercise literal escaping: a single quote
		// (doubled) and a backslash (E'...' escape-string form).
		quotedFilter := OptimizedUserFilter{IDs: []string{"o'brien", `back\slash`, "user02"}}
		sqlText := env.buildSQL(t, queue, quotedFilter, nil)
		require.NoError(t, env.exec(sqlText))

		// The stored row carries the document the one-shot worker expects.
		var argsJSON, uniqueID string
		var lifecycle int
		require.NoError(t, env.queueDB.QueryRowContext(env.ctx,
			`SELECT args, unique_id, unique_lifecycle FROM goque_jobs WHERE queue = $1`, queue).
			Scan(&argsJSON, &uniqueID, &lifecycle))

		expectedFilter, err := json.Marshal(quotedFilter)
		require.NoError(t, err)
		assert.Equal(t, etl.OneShotUniqueID, uniqueID)
		assert.Equal(t, int(que.Lockable), lifecycle)

		var req etl.ExtractRequest[*etl.Cursor]
		_, err = que.ParseArgs([]byte(argsJSON), &req)
		require.NoError(t, err)
		assert.Equal(t, oneShotPageSize, req.First)
		assert.JSONEq(t, string(expectedFilter), string(req.OneShotFilter),
			"filter must survive SQL literal quoting byte-exactly")

		// Executing the same generated SQL again while the task is in flight
		// violates the unique constraint (duplicate-insert protection)...
		err = env.exec(sqlText)
		require.Error(t, err, "duplicate execution of the generated SQL must be rejected")
		assert.Contains(t, err.Error(), "goque_jobs_unique_uidx")

		// ...and so is a task with a different filter: the fixed unique id
		// serializes one-shot tasks queue-wide, so nothing else can be
		// submitted until the in-flight task completes or expires.
		otherSQL := env.buildSQL(t, queue, OptimizedUserFilter{IDs: []string{"user03"}}, nil)
		err = env.exec(otherSQL)
		require.Error(t, err, "a second task must be rejected while one is in flight")
		assert.Contains(t, err.Error(), "goque_jobs_unique_uidx")
		assert.Equal(t, 1, env.countJobs(t, queue), "the in-flight task must remain the only job")

		// This queue has no worker to drain it; clean up by hand.
		_, err = env.queueDB.ExecContext(env.ctx, `DELETE FROM goque_jobs WHERE queue = $1`, queue)
		require.NoError(t, err)
	})

	t.Run("windowed job in one-shot queue is expired", func(t *testing.T) {
		const queue = "oneshot_mismatch_etl"
		env.startOneShot(t, queue, env.syncer(), nil)

		// A filter-less (incremental-shaped) job cannot be fixed by retrying;
		// the one-shot worker must expire it instead of misinterpreting it.
		env.insertRawJob(t, queue, `[{"After":{"at":"0001-01-01T00:00:00Z","id":""},"First":4}]`)
		env.waitExpired(t, queue, 1)
	})

	t.Run("failure retries then expires without successor", func(t *testing.T) {
		const queue = "oneshot_failing_etl"
		failing := &failingSource{}
		fastRetry := &que.RetryPolicy{
			InitialInterval:        200 * time.Millisecond,
			MaxInterval:            time.Second,
			NextIntervalMultiplier: 1,
			MaxRetryCount:          1,
		}
		env.startOneShot(t, queue, failing, fastRetry)

		taskSQL := env.buildSQL(t, queue, OptimizedUserFilter{IDs: []string{"user01"}}, fastRetry)
		require.NoError(t, env.exec(taskSQL))

		// While the task is in flight (pending or retrying), executing the
		// same statement again is rejected by the fixed one-shot unique id.
		err := env.exec(taskSQL)
		require.Error(t, err, "identical in-flight task must be rejected")
		assert.Contains(t, err.Error(), "goque_jobs_unique_uidx")

		env.waitExpired(t, queue, 1)
		assert.Equal(t, 1, env.countJobs(t, queue), "no successor job may be enqueued on failure")
		assert.Equal(t, 2, failing.callCount(), "MaxRetryCount 1 means the initial attempt plus one retry")

		// Expiry released the unique id: the same statement is accepted again.
		require.NoError(t, env.exec(taskSQL))
		env.waitExpired(t, queue, 2)
	})

	// Must run after "targeted sync of 9 ids": the chain syncs every user,
	// which would defeat that subtest's bystander assertions.
	t.Run("incremental chain runs unaffected alongside", func(t *testing.T) {
		const (
			chainQueue   = "incremental_identity_etl"
			oneShotQueue = "oneshot_alongside_etl"
		)
		env.startChain(t, chainQueue)
		env.startOneShot(t, oneShotQueue, env.syncer(), nil)

		// The chain sweeps the full history and catches up all 12 users.
		require.Eventually(t, func() bool {
			var n int64
			if err := env.targetDB.Model(&Identity{}).Count(&n).Error; err != nil {
				return false
			}
			return n == 12
		}, 60*time.Second, 500*time.Millisecond, "incremental chain should sync all users")

		// Fire a one-shot while the chain is live; the chain keeps exactly
		// one pending window job — neither consumed nor duplicated.
		env.submit(t, oneShotQueue, OptimizedUserFilter{IDs: []string{"user01"}})
		env.waitDrained(t, oneShotQueue)
		assert.Equal(t, 1, env.countPending(t, chainQueue), "the chain's single pending job must be untouched")

		// A one-shot (filtered) job manually inserted into the incremental
		// queue must be expired instead of misinterpreted — and the chain
		// keeps running.
		env.insertRawJob(t, chainQueue,
			`[{"After":{"at":"0001-01-01T00:00:00Z","id":""},"First":10,"OneShotFilter":{"ids":["user01"]}}]`)
		env.waitExpired(t, chainQueue, 1)
		assert.Equal(t, 1, env.countPending(t, chainQueue), "the chain must keep running after expiring the mismatched job")
	})
}

// Pure input validation of the one-shot helpers lives in
// pipeline_oneshot_helper_test.go — no databases involved there.
