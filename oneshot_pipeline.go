package etl

import (
	"context"
	"database/sql"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus/quex"
	"github.com/qor5/go-que"
	"github.com/qor5/x/v3/sqlx"
	"github.com/theplant/appkit/errornotifier"
	"github.com/theplant/appkit/logtracing"
)

// OneShotPipelineConfig contains configuration for a OneShotPipeline.
//
// Compared to PipelineConfig there is no Interval/ConsistencyDelay (one-shot
// jobs select records by filter, not by time window) and no circuit breaker:
// a one-shot task is finite — it retries per RetryPolicy and expires when
// retries are exhausted — so the runaway skip-and-continue condition the
// breaker protects the incremental chain from cannot occur.
type OneShotPipelineConfig[T any] struct {
	// Core dependencies. Typically the same Source instance that serves the
	// incremental Pipeline; its Extract must branch on
	// ExtractRequest.OneShotFilter.
	Source Source[T]

	// Database and queue configuration. Use a dedicated QueueName, separate
	// from the incremental pipeline's queue (e.g. "<name>_ONESHOT").
	QueueDB   *sql.DB
	QueueName string

	// Processing parameters
	PageSize    int
	RetryPolicy *que.RetryPolicy

	// Optional configurations
	Notifier errornotifier.Notifier
}

// Validate validates the configuration
func (c *OneShotPipelineConfig[T]) Validate() error {
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

	if c.RetryPolicy == nil {
		return errors.New("RetryPolicy is required")
	}

	return nil
}

// OneShotPipeline runs targeted one-shot sync tasks: each task syncs only the
// records matched by a caller-provided filter of type F, pages through them
// with the same keyset cursor mechanism as the incremental Pipeline, then
// ends. It never enqueues a time-window successor.
//
// F is the Source-defined filter schema. Enqueue accepts it typed; on the
// extract side the Source decodes ExtractRequest.OneShotFilter via
// UnmarshalOneShotFilter[F].
type OneShotPipeline[T any, F any] struct {
	*OneShotPipelineConfig[T]
	queue que.Queue
}

// NewOneShotPipeline creates a new OneShotPipeline instance.
func NewOneShotPipeline[T any, F any](conf *OneShotPipelineConfig[T]) (*OneShotPipeline[T, F], error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	queue, err := newTracedQueue(conf.QueueDB)
	if err != nil {
		return nil, err
	}

	return &OneShotPipeline[T, F]{
		OneShotPipelineConfig: conf,
		queue:                 queue,
	}, nil
}

// Start boots the worker. Unlike Pipeline.Start it enqueues nothing — tasks
// are submitted via Enqueue.
func (s *OneShotPipeline[T, F]) Start(ctx context.Context) (quex.WorkerController, error) {
	return startWorker(ctx, s.queue, s.QueueName, s.Notifier, s.process)
}

// Enqueue submits a one-shot task that syncs only the records matched by
// filter, starting keyset pagination from seedCursor (usually the cursor's
// zero value, e.g. &etl.Cursor{}). Jobs carry no UniqueID, so multiple tasks
// can coexist in the same queue.
func (s *OneShotPipeline[T, F]) Enqueue(ctx context.Context, seedCursor T, filter F) error {
	raw, err := MarshalOneShotFilter(filter)
	if err != nil {
		return err
	}

	req := &ExtractRequest[T]{
		After:         seedCursor,
		First:         s.PageSize,
		OneShotFilter: raw,
	}

	return sqlx.Transaction(ctx, s.QueueDB, func(ctx context.Context, tx *sql.Tx) error {
		return s.enqueueJob(ctx, tx, req, time.Now())
	})
}

// enqueueJob enqueues a one-shot job. It applies no time-window math and sets
// no UniqueID.
func (s *OneShotPipeline[T, F]) enqueueJob(ctx context.Context, tx *sql.Tx, req *ExtractRequest[T], runAt time.Time) error {
	if len(req.OneShotFilter) == 0 {
		return errors.New("filter is required for one-shot job")
	}

	plan := que.Plan{
		Queue:       s.QueueName,
		Args:        que.Args(req),
		RunAt:       runAt,
		RetryPolicy: *s.RetryPolicy,
	}

	jobIDs, err := s.queue.Enqueue(ctx, tx, plan)
	if err != nil {
		return errors.Wrap(err, "failed to enqueue one-shot job")
	}

	if len(jobIDs) != 1 {
		return errors.New("unexpected number of job IDs returned")
	}

	return nil
}

// process handles one one-shot job. On success the job is destroyed; if the
// filtered set spans multiple pages, the next page is enqueued carrying the
// same OneShotFilter with an advanced cursor — the task ends when the last
// page completes. On failure the error is simply returned: go-que retries per
// the job's RetryPolicy (restarting from this page's cursor — CommitFunc
// MERGEs are expected to be idempotent, so replays are safe) and expires the
// job when retries are exhausted.
func (s *OneShotPipeline[T, F]) process(ctx context.Context, job que.Job) (xerr error) {
	ctx, span := logtracing.StartSpan(ctx, "etl.OneShotPipeline.process")
	spanKVs := make(map[string]any)
	defer func() {
		for k, v := range spanKVs {
			span.AppendKVs(k, v)
		}
		logtracing.EndSpan(ctx, xerr)
	}()

	spanKVs["process_job_id"] = job.ID()

	var req ExtractRequest[T]
	if _, err := que.ParseArgs(job.Plan().Args, &req); err != nil {
		return errors.Wrap(err, "failed to parse ExtractRequest from job args")
	}

	// A job without filter (e.g. an incremental job manually inserted into
	// this queue) cannot be fixed by retrying — expire it right away instead
	// of misinterpreting it.
	if len(req.OneShotFilter) == 0 {
		spanKVs["mode_mismatch"] = true
		return expireJobWithReason(ctx, job, s.Notifier, s.QueueName,
			errors.New("one-shot pipeline received a job without filter"))
	}

	result := doProcess(ctx, s.Source, s.Notifier, &req)
	if result.Error != nil {
		return result.Error
	}

	return sqlx.Transaction(ctx, s.QueueDB, func(ctx context.Context, tx *sql.Tx) error {
		job.In(tx)
		defer job.In(nil)

		// Mark current job as completed
		if err := job.Destroy(ctx); err != nil {
			return errors.Wrap(err, "failed to mark job as done")
		}

		if result.HasNextPage {
			nextReq := &ExtractRequest[T]{
				After:         result.NewCursor,
				First:         s.PageSize,
				OneShotFilter: req.OneShotFilter,
			}
			if err := s.enqueueJob(ctx, tx, nextReq, time.Now()); err != nil {
				return errors.Wrap(err, "failed to enqueue next page one-shot job")
			}
		}

		spanKVs["job_completed"] = true
		spanKVs["has_next_page"] = result.HasNextPage
		return nil
	})
}
