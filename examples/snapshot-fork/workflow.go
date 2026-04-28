package snapshotfork

import (
	"fmt"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "snapshot-fork-queue"

type WorkflowResult struct {
	// Each field contains the sorted /tmp/*.txt listing from that sandbox.
	// Expected:
	//   OriginFiles: shared.txt only
	//   ForkAFiles:  fork-a.txt + shared.txt
	//   ForkBFiles:  fork-b.txt + shared.txt
	OriginFiles string
	ForkAFiles  string
	ForkBFiles  string
}

// SnapshotForkWorkflow demonstrates snapshot isolation:
//  1. Create an origin sandbox and write a shared file.
//  2. Snapshot the origin (which continues running).
//  3. Branch into two independent forks, each inheriting the shared file.
//  4. Write a fork-specific file to each fork.
//  5. Verify that each sandbox only sees its own files.
func SnapshotForkWorkflow(ctx workflow.Context) (WorkflowResult, error) {
	logger := workflow.GetLogger(ctx)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	provider := sandbox.Provider{
		Type: compute.ProviderTypeModal,
		Config: map[string]string{
			"image": "ubuntu:26.04",
		},
	}

	// Step 1: Create the origin sandbox and write the shared file.
	origin, err := sandbox.NewSandbox(ctx, provider)
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("create origin sandbox: %w", err)
	}
	if _, err := origin.ExecuteCommand(ctx, "echo 'shared content' > /tmp/shared.txt"); err != nil {
		return WorkflowResult{}, fmt.Errorf("write shared file: %w", err)
	}

	// Step 2: Snapshot the origin. The origin sandbox keeps running.
	snap, err := origin.Snapshot(ctx)
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("snapshot origin: %w", err)
	}
	defer func() {
		cleanupCtx, cancelFunc := workflow.NewDisconnectedContext(ctx)
		if err := origin.DeleteSnapshot(cleanupCtx, snap); err != nil {
			workflow.GetLogger(cleanupCtx).Error("Failed to delete snapshot", "error", err)
		}
		cancelFunc()
	}()

	// Step 3: Create two independent forks from the snapshot.
	forkA, err := sandbox.NewSandbox(ctx, provider, sandbox.WithSnapshot(snap))
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("create fork-a: %w", err)
	}
	forkB, err := sandbox.NewSandbox(ctx, provider, sandbox.WithSnapshot(snap))
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("create fork-b: %w", err)
	}

	// Step 4: Write a distinct file to each fork.
	if _, err := forkA.ExecuteCommand(ctx, "echo 'fork-a content' > /tmp/fork-a.txt"); err != nil {
		return WorkflowResult{}, fmt.Errorf("write fork-a file: %w", err)
	}
	if _, err := forkB.ExecuteCommand(ctx, "echo 'fork-b content' > /tmp/fork-b.txt"); err != nil {
		return WorkflowResult{}, fmt.Errorf("write fork-b file: %w", err)
	}

	// Step 5: List /tmp/*.txt in each sandbox to verify isolation.
	originOut, err := origin.ExecuteCommand(ctx, "ls /tmp/*.txt 2>/dev/null | sort")
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("list origin files: %w", err)
	}
	forkAOut, err := forkA.ExecuteCommand(ctx, "ls /tmp/*.txt 2>/dev/null | sort")
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("list fork-a files: %w", err)
	}
	forkBOut, err := forkB.ExecuteCommand(ctx, "ls /tmp/*.txt 2>/dev/null | sort")
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("list fork-b files: %w", err)
	}

	if err := forkA.Stop(ctx); err != nil {
		logger.Error("failed to stop fork-a", "error", err)
	}
	if err := forkB.Stop(ctx); err != nil {
		logger.Error("failed to stop fork-b", "error", err)
	}

	return WorkflowResult{
		OriginFiles: originOut.Stdout,
		ForkAFiles:  forkAOut.Stdout,
		ForkBFiles:  forkBOut.Stdout,
	}, nil
}
