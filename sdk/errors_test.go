package sandbox

import (
	"errors"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/temporal"
)

func TestAsUnsupported(t *testing.T) {
	want := compute.UnsupportedError{Operation: compute.OperationCommand, Option: "working_directory", Reason: "no working directory"}
	refusal := temporal.NewNonRetryableApplicationError(want.Error(), compute.ErrorTypeUnsupported, nil, want)
	for _, test := range []struct {
		name string
		err  error
		want *compute.UnsupportedError
	}{
		{name: "direct", err: &want, want: &want},
		{name: "application error", err: refusal, want: &want},
		{name: "rewrapped by update", err: wrapUpdateError(refusal), want: &want},
		{name: "no details", err: temporal.NewNonRetryableApplicationError("refused", compute.ErrorTypeUnsupported, nil), want: &compute.UnsupportedError{Reason: "refused"}},
		{name: "other type", err: temporal.NewNonRetryableApplicationError("gone", "SandboxNotFound", nil)},
		{name: "plain error", err: errors.New("boom")},
		{name: "nil"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := AsUnsupported(test.err)
			if test.want == nil {
				if ok {
					t.Fatalf("AsUnsupported = %+v, want no match", got)
				}
				return
			}
			if !ok || *got != *test.want {
				t.Fatalf("AsUnsupported = %+v, %v; want %+v", got, ok, *test.want)
			}
		})
	}
}
