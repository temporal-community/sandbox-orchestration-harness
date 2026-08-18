// Package snapshotfork shows why forking a Crafting sandbox is different from
// forking a container.
//
// The workflow builds one environment, applies a schema change to its database,
// then branches into two independent sandboxes from the same checkpoint. Each
// fork gets its own copy of the workspace and its own copy of the database, so
// two candidate migrations can run at the same time without either one seeing
// the other's rows.
package databasefork

import (
	"fmt"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "database-fork-queue"

// Config names the Crafting objects the workflow runs against, so the example
// can point at any template without being rebuilt.
type Config struct {
	Template   string
	Workspace  string
	Dependency string
	Folder     string
}

type WorkflowResult struct {
	// Each field holds that sandbox's view of the shared table.
	// The origin never sees either fork's row, and neither fork sees the other's.
	OriginRows string
	ForkARows  string
	ForkBRows  string
	// Home directory listings, showing the filesystem is forked too.
	OriginFiles string
	ForkAFiles  string
	ForkBFiles  string
}

// DatabaseForkWorkflow provisions an environment, checkpoints it, and explores
// two divergent changes from that checkpoint concurrently.
func DatabaseForkWorkflow(ctx workflow.Context, cfg Config) (WorkflowResult, error) {
	logger := workflow.GetLogger(ctx)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
	})

	provider := sandbox.Provider{
		Type: compute.ProviderTypeCrafting,
		Config: map[string]string{
			"template":  cfg.Template,
			"workspace": cfg.Workspace,
			// Naming the dependency here is what makes the database part of the
			// snapshot. Without it, a fork would inherit the files but share
			// nothing of the data the agent had built up.
			"dependencies": cfg.Dependency,
			"folder":       cfg.Folder,
		},
	}

	origin, err := sandbox.NewSandbox(ctx, provider)
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("create origin sandbox: %w", err)
	}

	// Build up state the way an agent would: a file in the workspace and a
	// schema plus a row in the database.
	if _, err := origin.ExecuteCommand(ctx, `echo 'shared content' > "$HOME/shared.txt"`); err != nil {
		return WorkflowResult{}, fmt.Errorf("write shared file: %w", err)
	}
	if _, err := origin.ExecuteCommand(ctx, seedSchema); err != nil {
		return WorkflowResult{}, fmt.Errorf("seed schema: %w", err)
	}

	// Checkpoint. The origin keeps running, so it stays available for comparison.
	snap, err := origin.Snapshot(ctx)
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("snapshot origin: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		if err := origin.DeleteSnapshot(cleanupCtx, snap); err != nil {
			workflow.GetLogger(cleanupCtx).Error("failed to delete snapshot", "error", err)
		}
	}()

	forkA, err := sandbox.NewSandbox(ctx, provider, sandbox.WithSnapshot(snap))
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("create fork-a: %w", err)
	}
	forkB, err := sandbox.NewSandbox(ctx, provider, sandbox.WithSnapshot(snap))
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("create fork-b: %w", err)
	}

	// Two different changes, each against its own database.
	if _, err := forkA.ExecuteCommand(ctx, insertRow("fork-a")); err != nil {
		return WorkflowResult{}, fmt.Errorf("apply fork-a change: %w", err)
	}
	if _, err := forkA.ExecuteCommand(ctx, `echo 'fork a' > "$HOME/fork-a.txt"`); err != nil {
		return WorkflowResult{}, fmt.Errorf("write fork-a file: %w", err)
	}
	if _, err := forkB.ExecuteCommand(ctx, insertRow("fork-b")); err != nil {
		return WorkflowResult{}, fmt.Errorf("apply fork-b change: %w", err)
	}
	if _, err := forkB.ExecuteCommand(ctx, `echo 'fork b' > "$HOME/fork-b.txt"`); err != nil {
		return WorkflowResult{}, fmt.Errorf("write fork-b file: %w", err)
	}

	result := WorkflowResult{}
	for _, step := range []struct {
		sbx    sandbox.Sandbox
		cmd    string
		target *string
		what   string
	}{
		{origin, readRows, &result.OriginRows, "origin rows"},
		{forkA, readRows, &result.ForkARows, "fork-a rows"},
		{forkB, readRows, &result.ForkBRows, "fork-b rows"},
		{origin, listFiles, &result.OriginFiles, "origin files"},
		{forkA, listFiles, &result.ForkAFiles, "fork-a files"},
		{forkB, listFiles, &result.ForkBFiles, "fork-b files"},
	} {
		out, err := step.sbx.ExecuteCommand(ctx, step.cmd)
		if err != nil {
			return WorkflowResult{}, fmt.Errorf("read %s: %w", step.what, err)
		}
		*step.target = out.Stdout
	}

	if err := forkA.Stop(ctx); err != nil {
		logger.Error("failed to stop fork-a", "error", err)
	}
	if err := forkB.Stop(ctx); err != nil {
		logger.Error("failed to stop fork-b", "error", err)
	}
	// The origin is deliberately left running. Snapshot deletion is routed through
	// the workflow of the sandbox that produced it, so stopping the origin here
	// would leave the deferred DeleteSnapshot with nowhere to go. The harness tears
	// the origin down when this workflow closes.

	return result, nil
}

const seedSchema = `
set -e
psql -h "$DB_SERVICE_HOST" -U postgres -q -c "CREATE TABLE IF NOT EXISTS migrations(id serial primary key, applied_by text);"
psql -h "$DB_SERVICE_HOST" -U postgres -q -c "INSERT INTO migrations(applied_by) VALUES ('origin');"
`

const readRows = `psql -h "$DB_SERVICE_HOST" -U postgres -At -c "SELECT applied_by FROM migrations ORDER BY id;" | paste -sd, -`

const listFiles = `ls "$HOME"/*.txt 2>/dev/null | xargs -r -n1 basename | sort | paste -sd, -`

func insertRow(who string) string {
	return fmt.Sprintf(`psql -h "$DB_SERVICE_HOST" -U postgres -q -c "INSERT INTO migrations(applied_by) VALUES ('%s');"`, who)
}
