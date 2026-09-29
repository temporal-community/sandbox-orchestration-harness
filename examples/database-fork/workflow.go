// Package databasefork shows what it means for a snapshot to capture more than
// a filesystem.
//
// Agents rarely keep all of their state in files. The workflow builds up state
// in two places, a file in the home directory and rows in a Postgres database,
// snapshots the sandbox, then branches two forks from that snapshot and applies
// a different change to each. The result reports what every sandbox sees, which
// shows whether the provider's snapshots carry database state along with files:
// when they do, the forks can explore divergent migrations concurrently without
// either one seeing the other's rows.
//
// The workflow is provider-agnostic. It only assumes the sandbox has a psql
// client and the standard libpq environment (PGHOST, PGUSER, and so on) pointing
// at the database it should use.
package databasefork

import (
	"fmt"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "database-fork-queue"

type WorkflowResult struct {
	// Each field holds that sandbox's view of the shared table. With snapshots
	// that include the database, the origin never sees either fork's row, and
	// neither fork sees the other's.
	OriginRows string
	ForkARows  string
	ForkBRows  string
	// Home directory listings, showing the filesystem is forked too.
	OriginFiles string
	ForkAFiles  string
	ForkBFiles  string
}

// DatabaseForkWorkflow provisions a sandbox from the given provider, snapshots
// it, and explores two divergent changes from that snapshot concurrently.
func DatabaseForkWorkflow(ctx workflow.Context, provider sandbox.Provider) (WorkflowResult, error) {
	logger := workflow.GetLogger(ctx)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
	})

	origin, err := sandbox.NewSandbox(ctx, provider)
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("create origin sandbox: %w", err)
	}

	// Build up state the way an agent would: a file in the home directory and a
	// schema plus a row in the database.
	if _, err := origin.ExecuteCommand(ctx, `echo 'shared content' > "$HOME/shared.txt"`); err != nil {
		return WorkflowResult{}, fmt.Errorf("write shared file: %w", err)
	}
	if _, err := origin.ExecuteCommand(ctx, seedSchema); err != nil {
		return WorkflowResult{}, fmt.Errorf("seed schema: %w", err)
	}

	// The origin keeps running, so it stays available for comparison.
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

	// Two different changes, one per fork.
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
psql -q -c "CREATE TABLE IF NOT EXISTS migrations(id serial primary key, applied_by text);"
psql -q -c "INSERT INTO migrations(applied_by) VALUES ('origin');"
`

const readRows = `psql -At -c "SELECT applied_by FROM migrations ORDER BY id;" | paste -sd, -`

const listFiles = `ls "$HOME"/*.txt 2>/dev/null | xargs -r -n1 basename | sort | paste -sd, -`

func insertRow(who string) string {
	return fmt.Sprintf(`psql -q -c "INSERT INTO migrations(applied_by) VALUES ('%s');"`, who)
}
