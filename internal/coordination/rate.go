package coordination

import (
	"math"
	"time"
)

// RateRetryAt estimates when an exhausted bucket with a positive per-minute
// capacity can admit again. Very large valid token usage may create debt beyond
// time.Duration's range; saturating keeps Retry-After positive in that case.
func RateRetryAt(balance float64, capacity int64, now time.Time) time.Time {
	delay := (1 - balance) / float64(capacity) * float64(time.Minute)
	if delay >= float64(math.MaxInt64) {
		return now.Add(time.Duration(math.MaxInt64))
	}
	return now.Add(time.Duration(delay))
}
