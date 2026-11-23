package sandbox_test

import (
	"fmt"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/workflow"
)

// lifecycleFixtureProcessScript writes its PID and report, then completes after two seconds.
// Process completion is independent of sandbox idleness and cleanup ownership.
const lifecycleFixtureProcessScript = "printf '%s' $$ > process.pid; printf process-output > report.txt; exec sleep 2"

const lifecycleFixtureReadReport = "if [ -s process.pid ] && [ -f report.txt ] && ! kill -0 \"$(cat process.pid)\" 2>/dev/null; then cat report.txt; else exit 75; fi"

// lifecycleFixtureInput selects one local scenario and its provider configuration.
type lifecycleFixtureInput struct {
	Config map[string]string
	Cancel bool
}

// lifecycleFixtureWorkflow returns the process report and waits for controller cleanup on return.
// Cancellation also runs Stop through a disconnected context.
func lifecycleFixtureWorkflow(ctx workflow.Context, input lifecycleFixtureInput) (report string, err error) {
	ready := false
	if err := workflow.SetQueryHandler(ctx, "synthetic-lifecycle-ready", func() (bool, error) { return ready, nil }); err != nil {
		return "", err
	}
	s, err := sandbox.NewSandbox(ctx, sandbox.Provider{Type: compute.ProviderTypeSmolCloud, Config: input.Config}, sandbox.WithIdleTimeout(sandbox.NoIdleTimeout))
	if err != nil {
		return "", err
	}
	defer func() {
		cleanup, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		if stopErr := s.Stop(cleanup); stopErr != nil {
			err = stopErr
		}
	}()
	ready = true
	if input.Cancel {
		return "", workflow.Await(ctx, func() bool { return false })
	}
	for attempt := 0; attempt < 10; attempt++ {
		result, execErr := s.ExecuteCommand(ctx, lifecycleFixtureReadReport)
		if execErr != nil {
			return "", execErr
		}
		if result.ExitCode == 0 {
			return result.Stdout, nil
		}
		if result.ExitCode != 75 {
			return "", fmt.Errorf("report command failed: %+v", result)
		}
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("process report did not become ready")
}
