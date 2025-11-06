package etl

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus/quex"
	"github.com/qor5/go-que"
	"github.com/qor5/go-que/pg"
	"github.com/qor5/x/v3/goquex"
	"github.com/qor5/x/v3/sqlx"
	"github.com/samber/lo"
	"github.com/theplant/appkit/errornotifier"
	"github.com/theplant/appkit/logtracing"
)

// PipelineConfig contains configuration for the pipeline
type PipelineConfig[T any] struct {
	// Core dependencies
	Source Source[T]

	// Database and queue configuration
	QueueDB   *sql.DB
	QueueName string

	// Processing parameters
	PageSize         int
	Interval         time.Duration
	ConsistencyDelay time.Duration

	// Retry and circuit breaker configuration
	RetryPolicy             *que.RetryPolicy
	CircuitBreakerThreshold int           // Number of consecutive skipped jobs before stopping pipeline
	CircuitBreakerCooldown  time.Duration // Time to wait before attempting to close circuit breaker

	// Optional configurations
	Notifier errornotifier.Notifier
}

// Validate validates the configuration
func (c *PipelineConfig[T]) Validate() error {
	if c == nil {
		return errors.New("config is nil")
	}

	if c.Source == nil {
		return errors.New("Source is required")
	}

	if c.QueueDB == nil {
		return errors.New("DB is required")
	}

	if c.QueueName == "" {
		return errors.New("QueueName is required")
	}

	if c.PageSize <= 0 {
		return errors.New("PageSize must be greater than 0")
	}

	if c.Interval <= 0 {
		return errors.New("Interval must be greater than 0")
	}

	if c.ConsistencyDelay < 0 {
		return errors.New("ConsistencyDelay must be greater than or equal to 0")
	}

	if c.RetryPolicy == nil {
		return errors.New("RetryPolicy is required")
	}

	if c.CircuitBreakerThreshold <= 0 {
		return errors.New("CircuitBreakerThreshold must be greater than 0")
	}

	if c.CircuitBreakerCooldown <= 0 {
		return errors.New("CircuitBreakerCooldown must be greater than 0")
	}

	return nil
}

type Pipeline[T any] struct {
	*PipelineConfig[T]
	queue que.Queue

	// Circuit breaker state - based on skipped job count
	skippedCount  atomic.Int64 // Counter for consecutive skipped jobs
	lastSkippedAt atomic.Value // time.Time of last skipped job
}

// NewPipeline creates a new Pipeline instance
func NewPipeline[T any](conf *PipelineConfig[T]) (*Pipeline[T], error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	queue, err := pg.NewWithOptions(pg.Options{DB: conf.QueueDB, DBMigrate: false})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create queue")
	}

	return &Pipeline[T]{
		PipelineConfig: conf,
		queue:          queue,
	}, nil
}

// Start starts the ETL processing
func (s *Pipeline[T]) Start(ctx context.Context, seedCursor T) (quex.WorkerController, error) {
	if err := s.enqueueSeedJob(ctx, seedCursor); err != nil {
		return nil, err
	}

	worker, err := quex.StartWorker(ctx, que.WorkerOptions{
		Mutex:   s.queue.Mutex(),
		Queue:   s.QueueName,
		Perform: goquex.PerformWithTracing(s.Notifier)(s.Process),
	})
	if err != nil {
		return nil, err
	}

	return worker, nil
}

// enqueueSeedJob enqueues the initial job
func (s *Pipeline[T]) enqueueSeedJob(ctx context.Context, seedCursor T) error {
	now := time.Now()

	req := &ExtractRequest[T]{
		After:  seedCursor,
		First:  s.PageSize,
		FromAt: time.Time{}, // Start from zero time to process all historical data
	}

	err := sqlx.Transaction(ctx, s.QueueDB, func(ctx context.Context, tx *sql.Tx) error {
		return s.enqueueJob(ctx, tx, req, now)
	})
	if err != nil && !errors.Is(err, que.ErrViolateUniqueConstraint) {
		return errors.Wrap(err, "failed to enqueue seed job")
	}
	return nil
}

var UniqueID = "etl_pipeline"

// enqueueJob enqueues a job with the given request template and run time
func (s *Pipeline[T]) enqueueJob(ctx context.Context, tx *sql.Tx, req *ExtractRequest[T], runAt time.Time) error {
	now := time.Now()

	if req.BeforeAt.IsZero() {
		intervalStart := req.FromAt
		if intervalStart.IsZero() {
			// For seed job, align to interval boundaries
			intervalStart = now.Truncate(s.Interval).Add(-s.Interval)
		}
		req.BeforeAt = intervalStart.Add(s.Interval)
	}

	if runAt.IsZero() {
		runAt = req.BeforeAt.Add(s.ConsistencyDelay)
	}
	if runAt.Before(now) {
		runAt = now
	}

	// Ensure we don't process future data beyond execution time - consistency delay
	maxBeforeAt := runAt.Add(-s.ConsistencyDelay)
	if req.BeforeAt.After(maxBeforeAt) {
		req.BeforeAt = maxBeforeAt
	}

	plan := que.Plan{
		Queue:           s.QueueName,
		Args:            que.Args(req),
		RunAt:           runAt,
		RetryPolicy:     *s.RetryPolicy,
		UniqueID:        &UniqueID,
		UniqueLifecycle: que.Lockable,
	}

	jobIDs, err := s.queue.Enqueue(ctx, tx, plan)
	if err != nil {
		return errors.Wrap(err, "failed to enqueue job")
	}

	if len(jobIDs) != 1 {
		return errors.New("unexpected number of job IDs returned")
	}

	return nil
}

// isCircuitBreakerOpen checks if the circuit breaker is currently open
func (s *Pipeline[T]) isCircuitBreakerOpen() bool {
	skipped := s.skippedCount.Load()
	if skipped < int64(s.CircuitBreakerThreshold) {
		return false
	}

	// Circuit breaker should be open, but check if cooldown period has passed
	if lastSkipped, ok := s.lastSkippedAt.Load().(time.Time); ok {
		cooldownExpiry := lastSkipped.Add(s.CircuitBreakerCooldown)
		return time.Now().Before(cooldownExpiry)
	}

	return true // If no lastSkippedAt, assume it's open
}

// recordSuccess resets the skipped jobs counter
func (s *Pipeline[T]) recordSuccess() {
	s.skippedCount.Store(0)
}

// recordSkipped increments skipped jobs counter and may trigger circuit breaker
// Returns true if circuit breaker threshold was reached
func (s *Pipeline[T]) recordSkipped() bool {
	s.lastSkippedAt.Store(time.Now())
	return s.skippedCount.Add(1) >= int64(s.CircuitBreakerThreshold)
}

// ProcessResult represents the result of an ETL processing operation
type ProcessResult[T any] struct {
	NewCursor   T
	HasNextPage bool
	Error       error
}

// Process performs the actual ETL processing logic
func (s *Pipeline[T]) Process(ctx context.Context, job que.Job) error {
	if s.isCircuitBreakerOpen() {
		logtracing.AppendSpanKVs(ctx, "circuit_breaker_open", true)
		return s.handleCircuitBreakerOpen(ctx, job)
	}

	var req ExtractRequest[T]
	if _, err := que.ParseArgs(job.Plan().Args, &req); err != nil {
		return errors.Wrap(err, "failed to parse ExtractRequest from job args")
	}

	result := s.doProcess(ctx, &req)
	if result.Error != nil {
		return s.handleFailure(ctx, job, &req, result)
	}

	return s.handleSuccess(ctx, job, &req, result)
}

// doProcess performs the core ETL processing logic for a single page
func (s *Pipeline[T]) doProcess(ctx context.Context, req *ExtractRequest[T]) *ProcessResult[T] {
	resp, err := s.Source.Extract(ctx, req)
	if err != nil {
		return &ProcessResult[T]{
			NewCursor:   req.After,
			HasNextPage: false,
			Error:       errors.Wrap(err, "failed to extract"),
		}
	}

	if resp.Target == nil {
		return &ProcessResult[T]{
			NewCursor:   req.After,
			HasNextPage: false,
		}
	}

	if lo.IsNil(resp.EndCursor) {
		return &ProcessResult[T]{
			NewCursor:   req.After,
			HasNextPage: false,
			Error:       errors.New("end cursor is nil"),
		}
	}

	// Create result with extract information
	result := &ProcessResult[T]{
		NewCursor:   resp.EndCursor,
		HasNextPage: resp.HasNextPage,
	}

	if err := resp.Target.Load(ctx); err != nil {
		// Do not cleanup on error to preserve debugging data
		result.Error = errors.Wrap(err, "failed to load")
		return result
	}

	// Only cleanup on successful write to allow error debugging
	if err := resp.Target.Cleanup(ctx); err != nil {
		logtracing.AppendSpanKVs(ctx, "etl.cleanup_error", err.Error())
		if s.Notifier != nil {
			// TODO: Pass current span key-values to notifier
			s.Notifier.Notify(errors.Wrap(err, "failed to cleanup"), nil, map[string]any{})
		}
	}

	return result
}

// calculateCooldownRunAt calculates the run time for the cooldown job
func (s *Pipeline[T]) calculateCooldownRunAt() time.Time {
	lastSkipped, ok := s.lastSkippedAt.Load().(time.Time)
	if !ok {
		panic("circuit breaker is open but lastSkippedAt is not set - this indicates a programming error")
	}
	return lastSkipped.Add(s.CircuitBreakerCooldown)
}

// handleCircuitBreakerOpen handles the case when circuit breaker is open
func (s *Pipeline[T]) handleCircuitBreakerOpen(ctx context.Context, job que.Job) error {
	var req ExtractRequest[T]
	if _, err := que.ParseArgs(job.Plan().Args, &req); err != nil {
		return errors.Wrap(err, "failed to parse request for circuit breaker handling")
	}

	return sqlx.Transaction(ctx, s.QueueDB, func(ctx context.Context, tx *sql.Tx) error {
		job.In(tx)
		defer job.In(nil)

		// Make sure the job is expired
		if err := job.Expire(ctx, errors.New("circuit breaker cooldown")); err != nil {
			return errors.Wrap(err, "failed to expire job during circuit breaker")
		}

		// Enqueue the same job to run after cooldown
		nextRunAt := s.calculateCooldownRunAt()
		if err := s.enqueueJob(ctx, tx, &req, nextRunAt); err != nil {
			return errors.Wrap(err, "failed to enqueue cooldown job")
		}

		logtracing.AppendSpanKVs(ctx,
			"cooldown_duration", s.CircuitBreakerCooldown.String(),
			"next_run_at", nextRunAt.Format(time.RFC3339),
		)

		return nil
	})
}

// handleFailure handles job failure with appropriate retry/skip logic
func (s *Pipeline[T]) handleFailure(ctx context.Context, job que.Job, req *ExtractRequest[T], result *ProcessResult[T]) error {
	_, hasMoreRetries := job.Plan().RetryPolicy.NextInterval(job.RetryCount())

	if hasMoreRetries {
		// Let goque handle the retry naturally
		return result.Error
	}

	// No more retries - need to skip and enqueue next job
	return sqlx.Transaction(ctx, s.QueueDB, func(ctx context.Context, tx *sql.Tx) error {
		job.In(tx)
		defer job.In(nil)

		// Mark current job as expired
		if err := job.Expire(ctx, result.Error); err != nil {
			return errors.Wrap(err, "failed to expire job")
		}

		// Record skipped job and check circuit breaker
		circuitBreakerOpened := s.recordSkipped()

		nextReq := s.createNextExtractRequest(req, result)

		var nextRunAt time.Time
		if circuitBreakerOpened {
			nextRunAt = s.calculateCooldownRunAt()
		} else if result.HasNextPage {
			nextRunAt = time.Now() // Same interval, next page
		}
		// For !result.HasNextPage && !circuitBreakerOpened, nextRunAt stays zero
		// and enqueueJob will handle it with req.BeforeAt.Add(s.ConsistencyDelay)

		if err := s.enqueueJob(ctx, tx, nextReq, nextRunAt); err != nil {
			return errors.Wrap(err, "failed to enqueue next job after failure")
		}

		// Log failure information for observability
		logtracing.AppendSpanKVs(ctx,
			"job_skipped", true,
			"process_error", fmt.Sprintf("%+v", result.Error),
			"has_next_page", result.HasNextPage,
			"circuit_breaker_opened", circuitBreakerOpened,
			"skipped_count", s.skippedCount.Load(),
		)

		// Notify about circuit breaker if it just opened
		if circuitBreakerOpened && s.Notifier != nil {
			// TODO: Pass current span key-values to notifier
			s.Notifier.Notify(errors.New("pipeline circuit breaker opened"), nil, map[string]any{
				"circuit_breaker_opened": true,
				"skipped_count":          s.skippedCount.Load(),
			})
		}

		return nil
	})
}

// handleSuccess handles completion of a successful job
func (s *Pipeline[T]) handleSuccess(ctx context.Context, job que.Job, req *ExtractRequest[T], result *ProcessResult[T]) error {
	s.recordSuccess()
	return sqlx.Transaction(ctx, s.QueueDB, func(ctx context.Context, tx *sql.Tx) error {
		job.In(tx)
		defer job.In(nil)

		// Mark current job as completed
		if err := job.Destroy(ctx); err != nil {
			return errors.Wrap(err, "failed to mark job as done")
		}

		// Determine and enqueue next req
		nextReq := s.createNextExtractRequest(req, result)
		var nextRunAt time.Time
		if result.HasNextPage {
			nextRunAt = time.Now()
		}
		if err := s.enqueueJob(ctx, tx, nextReq, nextRunAt); err != nil {
			return errors.Wrap(err, "failed to enqueue next job")
		}

		// Log success information for observability
		logtracing.AppendSpanKVs(ctx,
			"job_completed", true,
			"has_next_page", result.HasNextPage,
		)

		return nil
	})
}

// createNextExtractRequest creates the next job request based on current result
func (s *Pipeline[T]) createNextExtractRequest(req *ExtractRequest[T], result *ProcessResult[T]) *ExtractRequest[T] {
	if result.HasNextPage {
		return &ExtractRequest[T]{
			After:    result.NewCursor, // next page
			First:    s.PageSize,
			FromAt:   req.FromAt,
			BeforeAt: req.BeforeAt,
		}
	}

	return &ExtractRequest[T]{
		After:    result.NewCursor,
		First:    s.PageSize,
		FromAt:   req.BeforeAt, // next interval
		BeforeAt: time.Time{},
	}
}
