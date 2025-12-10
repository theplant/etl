package bqtarget

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
)

var MetadataColumnName = "__etl_metadata__"

// AddMetadataColumnHook returns a hook that adds __etl_metadata__ JSON column to staging tables
// This is a common utility for tracking ETL information in staging tables
func AddMetadataColumnHook[T any](next CreateStagingTableFunc[T]) CreateStagingTableFunc[T] {
	return func(ctx context.Context, input *CreateStagingTableInput[T]) (*CreateStagingTableOutput, error) {
		output, err := next(ctx, input)
		if err != nil {
			return nil, err
		}

		// Get client and dataset info for DDL statement
		client := input.Client
		projectID := client.Project()
		datasetID := input.DatasetID

		// Use DDL ALTER TABLE to add metadata column
		// This avoids ETag conflicts and is more robust than Update API
		// BigQuery ALTER TABLE ADD COLUMN IF NOT EXISTS is idempotent, so no need to check first
		alterQuery := fmt.Sprintf(`
			ALTER TABLE `+"`%s.%s.%s`"+` 
			ADD COLUMN IF NOT EXISTS %s JSON
		`, projectID, datasetID, output.StagingTable, MetadataColumnName)

		q := client.Query(alterQuery)
		job, err := q.Run(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to add metadata column to %s: job run failed", output.StagingTable)
		}

		// Wait for job to complete
		status, err := job.Wait(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to add metadata column to %s: job wait failed", output.StagingTable)
		}

		if status.Err() != nil {
			return nil, errors.Wrapf(status.Err(), "failed to add metadata column to %s: job status failed", output.StagingTable)
		}

		return output, nil
	}
}
