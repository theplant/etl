package etl

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus/quex"
	"github.com/qor5/go-que"
	"github.com/qor5/go-que/pg"
	"github.com/qor5/x/v3/goquex"
	"github.com/samber/lo"
	"github.com/theplant/appkit/errornotifier"
	"github.com/theplant/appkit/logtracing"
)

// This file holds the machinery shared by Pipeline (incremental chain mode)
// and OneShotPipeline (targeted one-shot mode): queue construction, worker
// boot, the extract-load-cleanup step, and permanent job failure. The two
// pipeline kinds differ only in scheduling policy — what happens around and
// after this shared step.

// newTracedQueue creates the goque queue used by both pipeline kinds.
func newTracedQueue(db *sql.DB) (que.Queue, error) {
	queue, err := pg.NewWithOptions(pg.Options{DB: db, DBMigrate: false})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create queue")
	}
	return goquex.WithTracing(queue), nil
}

// startWorker boots a goque worker consuming queueName with perform.
func startWorker(ctx context.Context, queue que.Queue, queueName string, notifier errornotifier.Notifier, perform func(context.Context, que.Job) error) (quex.WorkerController, error) {
	return quex.StartWorker(ctx, que.WorkerOptions{
		Mutex:   queue.Mutex(),
		Queue:   queueName,
		Perform: goquex.PerformWithTracing(notifier)(perform),
	})
}

// expireJobWithReason permanently fails a job that cannot succeed by retrying
// (e.g. its args do not match the queue's pipeline kind), notifying if a
// notifier is configured.
func expireJobWithReason(ctx context.Context, job que.Job, notifier errornotifier.Notifier, queueName string, reason error) error {
	if notifier != nil {
		notifier.Notify(reason, nil, map[string]any{"queue": queueName, "job_id": job.ID()})
	}
	if err := job.Expire(ctx, reason); err != nil {
		return errors.Wrap(err, "failed to expire job")
	}
	return nil
}

// ProcessResult represents the result of an ETL processing operation
type ProcessResult[T any] struct {
	NewCursor   T
	HasNextPage bool
	Error       error
}

// doProcess performs the core ETL processing logic for a single page:
// extract, load, cleanup. It is shared by both pipeline kinds.
func doProcess[T any](ctx context.Context, source Source[T], notifier errornotifier.Notifier, req *ExtractRequest[T]) (result *ProcessResult[T]) {
	ctx, span := logtracing.StartSpan(ctx, "etl.doProcess")
	spanKVs := make(map[string]any)
	defer func() {
		if result != nil {
			spanKVs["result.new_cursor"] = fmt.Sprintf("%v", result.NewCursor)
			spanKVs["result.has_next_page"] = result.HasNextPage
			spanKVs["result.error"] = fmt.Sprintf("%+v", result.Error)
		}
		for k, v := range spanKVs {
			span.AppendKVs(k, v)
		}
		logtracing.EndSpan(ctx, nil)
	}()

	spanKVs["req.after_cursor"] = fmt.Sprintf("%v", req.After)
	spanKVs["req.first"] = req.First
	spanKVs["req.from_at"] = req.FromAt.Format(time.RFC3339)
	spanKVs["req.before_at"] = req.BeforeAt.Format(time.RFC3339)
	if req.OneShotFilter != nil {
		spanKVs["req.one_shot_filter"] = string(req.OneShotFilter)
	}

	resp, err := source.Extract(ctx, req)
	if err != nil {
		return &ProcessResult[T]{
			NewCursor:   req.After,
			HasNextPage: false,
			Error:       errors.Wrap(err, "failed to extract"),
		}
	}

	spanKVs["resp.target_is_nil"] = resp.Target == nil
	spanKVs["resp.end_cursor"] = fmt.Sprintf("%v", resp.EndCursor)
	spanKVs["resp.has_next_page"] = resp.HasNextPage

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
	result = &ProcessResult[T]{
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
		if notifier != nil {
			notifier.Notify(errors.Wrap(err, "failed to cleanup"), nil, spanKVs)
		}
	}

	return result
}
