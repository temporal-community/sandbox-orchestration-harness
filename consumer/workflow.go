package consumer

import (
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/ephemeral-workers-poc/sdk/compute"
	"github.com/temporalio/ephemeral-workers-poc/sdk"
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
	Type      StepType
	Path      string
	Content   string // only used by StepCreateFile
	Sandboxed bool
}

// WorkflowInput is the top-level argument passed when starting the workflow.
type WorkflowInput struct {
	Steps []Step
}

// WorkflowResult holds one result string per step.
// create_file steps produce an empty string; read_file steps produce file content.
type WorkflowResult struct {
	Results []string
}

// FileOpsWorkflow executes each Step sequentially via activities.
func FileOpsWorkflow(ctx workflow.Context, input WorkflowInput) (WorkflowResult, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	sbx, err := sandbox.NewSandbox(ctx, compute.ComputeProvider{
		// Type:   compute.ComputeProviderTypeLambda,
		// Config: map[string]string{"function-arn": "arn:aws:lambda:us-west-2:093235337669:function:ephemeral-worker"},
		// --------------------------------------------
		// Type: compute.ComputeProviderTypeECS,
		// Config: map[string]string{
		// 	"cluster":            "arn:aws:ecs:us-west-2:093235337669:cluster/ephemeral-workers",
		// 	"task-definition":    "arn:aws:ecs:us-west-2:093235337669:task-definition/ephemeral-worker-poc:3",
		// 	"subnet-ids":         "subnet-03efb0eda57bd8d3e,subnet-0d9608e8bf78f3184",
		// 	"security-group-ids": "sg-00a9bcd71fb711df0",
		//  "assign-public-ip":   "true",
		// },
		// --------------------------------------------
		Type: compute.ComputeProviderTypeAgentCore,
		Config: map[string]string{
			"agent-runtime-arn": "arn:aws:bedrock-agentcore:us-west-2:093235337669:runtime/ephemeral_worker_v1-pge2nV635W",
		},
	})
	if err != nil {
		return WorkflowResult{}, err
	}

	results := make([]string, len(input.Steps))

	for i, step := range input.Steps {
		switch step.Type {
		case StepCreateFile:
			if step.Sandboxed {
				if err := sbx.ExecuteActivity(ctx, CreateFile, step.Path, step.Content).Get(ctx, nil); err != nil {
					return WorkflowResult{}, fmt.Errorf("step %d (create_file %q): %w", i, step.Path, err)
				}
			} else {
				if err := workflow.ExecuteActivity(ctx, CreateFile, step.Path, step.Content).Get(ctx, nil); err != nil {
					return WorkflowResult{}, fmt.Errorf("step %d (create_file %q): %w", i, step.Path, err)
				}
			}
		case StepReadFile:
			var content string

			if step.Sandboxed {
				if err := sbx.ExecuteActivity(ctx, ReadFile, step.Path).Get(ctx, &content); err != nil {
					return WorkflowResult{}, fmt.Errorf("step %d (read_file %q): %w", i, step.Path, err)
				}
			} else {
				if err := workflow.ExecuteActivity(ctx, ReadFile, step.Path).Get(ctx, &content); err != nil {
					return WorkflowResult{}, fmt.Errorf("step %d (read_file %q): %w", i, step.Path, err)
				}
			}
			results[i] = content
		case StepListFiles:
			var names []string

			if step.Sandboxed {
				if err := sbx.ExecuteActivity(ctx, ListFiles, step.Path).Get(ctx, &names); err != nil {
					return WorkflowResult{}, fmt.Errorf("step %d (list_files %q): %w", i, step.Path, err)
				}
			} else {
				if err := workflow.ExecuteActivity(ctx, ListFiles, step.Path).Get(ctx, &names); err != nil {
					return WorkflowResult{}, fmt.Errorf("step %d (list_files %q): %w", i, step.Path, err)
				}
			}
			results[i] = strings.Join(names, "\n")
		default:
			return WorkflowResult{}, fmt.Errorf("step %d: unknown type %q", i, step.Type)
		}
	}

	return WorkflowResult{Results: results}, nil
}
