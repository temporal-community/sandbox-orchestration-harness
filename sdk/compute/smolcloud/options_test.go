package smolcloud

import (
	"context"
	"errors"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/command"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func TestExecuteCommandRefusesWorkingDirectory(t *testing.T) {
	_, err := (&provider{}).ExecuteCommand(context.Background(), &compute.ProviderStatus{}, "ls", &command.Options{WorkingDirectory: "/work"})
	var unsupported *compute.UnsupportedError
	if !errors.As(err, &unsupported) || unsupported.Option != "working_directory" {
		t.Fatalf("err = %v, want unsupported working_directory", err)
	}
}
