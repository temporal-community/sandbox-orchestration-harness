package compute_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func TestUnsupportedError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "option", err: compute.UnsupportedOption(compute.OperationCommand, "working_directory", "agentcore has no working directory"), want: `command option "working_directory" is not supported: agentcore has no working directory`},
		{name: "operation", err: &compute.UnsupportedError{Operation: compute.OperationSnapshot}, want: "snapshot is not supported"},
		{name: "reason only", err: &compute.UnsupportedError{Reason: "fresh lifecycle"}, want: "not supported: fresh lifecycle"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.err.Error(); got != test.want {
				t.Fatalf("Error() = %q, want %q", got, test.want)
			}
			wrapped := fmt.Errorf("provider: %w", test.err)
			if !errors.Is(wrapped, errors.ErrUnsupported) {
				t.Fatal("UnsupportedError does not match errors.ErrUnsupported")
			}
			var unsupported *compute.UnsupportedError
			if !errors.As(wrapped, &unsupported) || unsupported != test.err {
				t.Fatal("errors.As did not find the UnsupportedError")
			}
		})
	}
}
