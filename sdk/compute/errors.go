package compute

import (
	"errors"
	"fmt"
)

// ErrorTypeUnsupported is the ApplicationError type for an UnsupportedError.
const ErrorTypeUnsupported = "Unsupported"

// UnsupportedError reports an operation or option the provider or policy refuses.
type UnsupportedError struct {
	// Operation is the refused operation.
	Operation Operation `json:"operation,omitempty"`
	// Option is the refused option's field name, such as "working_directory".
	Option string `json:"option,omitempty"`
	// Reason explains the refusal.
	Reason string `json:"reason,omitempty"`
}

func (e *UnsupportedError) Error() string {
	var msg string
	switch {
	case e.Option != "":
		msg = fmt.Sprintf("%s option %q is not supported", e.Operation, e.Option)
	case e.Operation != "":
		msg = fmt.Sprintf("%s is not supported", e.Operation)
	default:
		msg = "not supported"
	}
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	return msg
}

// Is matches errors.ErrUnsupported.
func (e *UnsupportedError) Is(target error) bool {
	return target == errors.ErrUnsupported
}

// UnsupportedOption returns an UnsupportedError for an option of operation.
func UnsupportedOption(operation Operation, option, reason string) error {
	return &UnsupportedError{Operation: operation, Option: option, Reason: reason}
}
