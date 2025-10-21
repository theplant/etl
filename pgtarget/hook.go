package pgtarget

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
)

var MetadataColumnName = "__etl_metadata__"

// AddMetadataColumnHook returns a hook that adds __etl_metadata__ JSONB column to staging tables
// This is a common utility for tracking ETL information in staging tables
func AddMetadataColumnHook[T any](next CreateStagingTableFunc[T]) CreateStagingTableFunc[T] {
	return func(ctx context.Context, input *CreateStagingTableInput[T]) (*CreateStagingTableOutput, error) {
		output, err := next(ctx, input)
		if err != nil {
			return nil, err
		}

		// Add MetadataColumnName column for tracking ETL information
		alterSQL := fmt.Sprintf(`
				ALTER TABLE %s 
				ADD COLUMN IF NOT EXISTS %s JSONB`,
			output.StagingTable, MetadataColumnName)

		if err := input.Tx.WithContext(ctx).Exec(alterSQL).Error; err != nil {
			return nil, errors.Wrapf(err, "failed to add %s column to staging table %s", MetadataColumnName, output.StagingTable)
		}

		return output, nil
	}
}
