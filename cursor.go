package etl

import (
	"fmt"
	"time"
)

// Cursor represents the ETL processing cursor with timestamp and ID
type Cursor struct {
	At time.Time `json:"at"`
	ID string    `json:"id"`
}

// String implements fmt.Stringer interface for Cursor
func (c *Cursor) String() string {
	if c == nil {
		return "null"
	}
	return fmt.Sprintf("%s_%s", c.At.Format(TimeLayout), c.ID)
}
