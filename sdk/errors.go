package sandbox

import (
	"errors"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/temporal"
)

// AsUnsupported returns the UnsupportedError carried anywhere in err's chain.
func AsUnsupported(err error) (*compute.UnsupportedError, bool) {
	var unsupported *compute.UnsupportedError
	if errors.As(err, &unsupported) {
		return unsupported, true
	}
	for ; err != nil; err = errors.Unwrap(err) {
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) {
			return nil, false
		}
		if appErr.Type() == compute.ErrorTypeUnsupported {
			details := &compute.UnsupportedError{Reason: appErr.Message()}
			if appErr.HasDetails() {
				if decodeErr := appErr.Details(details); decodeErr != nil {
					details = &compute.UnsupportedError{Reason: appErr.Message()}
				}
			}
			return details, true
		}
		err = appErr
	}
	return nil, false
}
