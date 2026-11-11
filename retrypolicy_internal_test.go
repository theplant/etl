package etl

import (
	"time"

	"github.com/qor5/go-que"
)

// defaultRetryPolicy is the retry policy the tests use (go-bus's default).
func defaultRetryPolicy() *que.RetryPolicy {
	return &que.RetryPolicy{
		InitialInterval:        30 * time.Second,
		MaxInterval:            600 * time.Second,
		NextIntervalMultiplier: 2,
		IntervalRandomPercent:  33,
		MaxRetryCount:          3,
	}
}
