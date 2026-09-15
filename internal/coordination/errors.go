package coordination

import (
	"context"
	"errors"
)

// IsCallerCancellation distinguishes cancellation of an individual caller from
// a coordinator's own timeout. Neither caller cancellation nor its deadline is
// evidence that the shared coordination backend is unavailable. Other errors,
// including permanent faults, must still be reported even if the caller exits.
func IsCallerCancellation(ctx context.Context, err error) bool {
	var permanent PermanentError
	if ctx.Err() == nil || errors.As(err, &permanent) {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
