package filemanagement

import (
	"fmt"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "file-ops-queue"

// StepType identifies the file operation a Step performs.
type StepType string

const (
	StepCreateFile StepType = "create_file"
	StepReadFile   StepType = "read_file"
	StepListFiles  StepType = "list_files"
)

// Step describes a single file operation to perform.
type Step struct {
	Type StepType
	Path string
}

// WorkflowInput is the top-level argument passed when starting the workflow.
type WorkflowInput struct {
	Steps []Step
}

// WorkflowResult holds one result string per step.
// Each step produces a formatted string: "X result - Code: N - Stdout: ... - Stderr: ...".
type WorkflowResult struct {
	Results []string
}

// FileOpsWorkflow executes each Step sequentially via activities.
// The sandbox is not stopped explicitly; Temporal's parent close policy sends a
// cancellation to the sandbox child workflow when this workflow completes, which
// triggers sandbox cleanup automatically.
func FileOpsWorkflow(ctx workflow.Context, input WorkflowInput) (WorkflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	// The sandbox runs on a dedicated sandbox worker (sdk/cmd/sandbox-worker), so
	// this example's own worker needs no compute-provider credentials.
	sbx, err := sandbox.NewSandbox(ctx, sandbox.Provider{
		Type: compute.ProviderTypeModal,
		Config: map[string]string{
			"image": "ubuntu:26.04",
		},
	}, sandbox.WithTaskQueue(sandbox.DefaultSandboxTaskQueue))
	if err != nil {
		return WorkflowResult{}, err
	}

	results := make([]string, len(input.Steps))

	for i, step := range input.Steps {
		switch step.Type {
		case StepCreateFile:
			result, err := sbx.ExecuteCommand(ctx, fmt.Sprintf("touch %s", step.Path))
			if err != nil {
				return WorkflowResult{}, fmt.Errorf("step %d (create_file %q): %w", i, step.Path, err)
			}
			results[i] = fmt.Sprintf("Create File result - Code: %d - Stdout: %s - Stderr: %s", result.ExitCode, result.Stdout, result.Stderr)
		case StepReadFile:
			result, err := sbx.ExecuteCommand(ctx, fmt.Sprintf("cat %s", step.Path))
			if err != nil {
				return WorkflowResult{}, fmt.Errorf("step %d (read_file %q): %w", i, step.Path, err)
			}
			results[i] = fmt.Sprintf("Read File result - Code: %d - Stdout: %s - Stderr: %s", result.ExitCode, result.Stdout, result.Stderr)
		case StepListFiles:
			result, err := sbx.ExecuteCommand(ctx, fmt.Sprintf("ls -la %s", step.Path))
			if err != nil {
				return WorkflowResult{}, fmt.Errorf("step %d (list_files %q): %w", i, step.Path, err)
			}
			results[i] = fmt.Sprintf("List File result - Code: %d - Stdout: %s - Stderr: %s", result.ExitCode, result.Stdout, result.Stderr)
		default:
			return WorkflowResult{}, fmt.Errorf("step %d: unknown type %q", i, step.Type)
		}
	}

	return WorkflowResult{Results: results}, nil
}
