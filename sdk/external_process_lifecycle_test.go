package sandbox_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/smolcloud"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
)

// TestLocalExternalProcessLifecycle checks process completion, cancellation and policy refusal.
func TestLocalExternalProcessLifecycle(t *testing.T) {
	if os.Getenv("HARNESS_LOCAL_TEST") != "1" {
		t.Skip("set HARNESS_LOCAL_TEST=1 and start the local server on 127.0.0.1:17293")
	}
	t.Setenv("TEMPORAL_API_KEY", "local-namespace-credential")
	for _, scenario := range []string{"completion", "cancellation", "unsupported-profile"} {
		t.Run(scenario, func(t *testing.T) {
			result, err := runLifecycleFixture(context.Background(), "127.0.0.1:17293", scenario)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(result)
		})
	}
}

// runLifecycleFixture starts the local API and outside Worker, executes the selected scenario,
// and checks history, credential placement and controller cleanup before return.
// It temporarily changes provider environment variables; callers must serialize runs.
// address names a local development Temporal server. No cloud API is used.
func runLifecycleFixture(ctx context.Context, address, scenario string) (string, error) {
	if scenario != "completion" && scenario != "cancellation" && scenario != "unsupported-profile" {
		return "", fmt.Errorf("unknown scenario %q", scenario)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	a, server, cleanup, err := startLifecycleFixture(ctx)
	if err != nil {
		return "", err
	}
	defer cleanup()
	for key, value := range map[string]string{"SMOL_CLOUD_URL": server.URL, "SMOL_CLOUD_TOKEN": "synthetic-lifecycle-token"} {
		previous, exists := os.LookupEnv(key)
		defer func() {
			if exists {
				_ = os.Setenv(key, previous)
			} else {
				_ = os.Unsetenv(key)
			}
		}()
		if err := os.Setenv(key, value); err != nil {
			return "", err
		}
	}
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return "", err
	}
	defer c.Close()
	queue := fmt.Sprintf("synthetic-lifecycle-%s-%d", scenario, time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	sandbox.Register(w, c)
	w.RegisterWorkflow(lifecycleFixtureWorkflow)
	if err := w.Start(); err != nil {
		return "", err
	}
	defer w.Stop()
	config := map[string]string{"image": "synthetic-lifecycle:local", "bootstrap": "none", "lifecycle": "fresh", "branchable": "false", "network": "blocked"}
	if scenario == "unsupported-profile" {
		config["branchable"] = "true"
	}
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, lifecycleFixtureWorkflow, lifecycleFixtureInput{Config: config, Cancel: scenario == "cancellation"})
	if err != nil {
		return "", err
	}
	defer func() {
		fallback, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = c.CancelWorkflow(fallback, run.GetID(), run.GetRunID())
	}()
	if scenario == "cancellation" {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			value, queryErr := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "synthetic-lifecycle-ready")
			var ready bool
			if queryErr == nil {
				if err := value.Get(&ready); err != nil {
					return "", err
				}
			}
			if ready {
				break
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-ticker.C:
			}
		}
		select {
		case <-a.done:
			return "", fmt.Errorf("process exited before cancellation")
		default:
		}
		if err := c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()); err != nil {
			return "", err
		}
	}
	var report string
	err = run.Get(ctx, &report)
	switch scenario {
	case "unsupported-profile":
		if err == nil || !strings.Contains(err.Error(), "fresh lifecycle requires branchable=false") {
			return "", fmt.Errorf("expected unsupported-profile refusal, got %v", err)
		}
	case "cancellation":
		if !temporal.IsCanceledError(err) {
			return "", fmt.Errorf("expected cancellation, got %v", err)
		}
	default:
		if err != nil {
			return "", err
		}
		if report != "process-output" {
			return "", fmt.Errorf("unexpected report %q", report)
		}
	}
	evidence, err := a.check(scenario)
	if err != nil {
		return "", err
	}
	if scenario == "unsupported-profile" {
		return "refused: fresh lifecycle requires branchable=false; " + evidence, nil
	}
	parent := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	children, stops := 0, 0
	var childID, childRun string
	for parent.HasNext() {
		event, err := parent.Next()
		if err != nil {
			return "", err
		}
		if attr := event.GetChildWorkflowExecutionStartedEventAttributes(); attr != nil {
			children++
			childID, childRun = attr.WorkflowExecution.WorkflowId, attr.WorkflowExecution.RunId
		}
	}
	if children != 1 {
		return "", fmt.Errorf("resource controllers=%d", children)
	}
	child := c.GetWorkflowHistory(ctx, childID, childRun, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for child.HasNext() {
		event, err := child.Next()
		if err != nil {
			return "", err
		}
		if attr := event.GetActivityTaskScheduledEventAttributes(); attr != nil && attr.ActivityType.Name == "StopSandbox" {
			stops++
		}
		if event.GetTimerStartedEventAttributes() != nil {
			return "", fmt.Errorf("NoIdleTimeout scheduled an idle timer")
		}
	}
	if stops != 1 {
		return "", fmt.Errorf("controller stop Activities=%d", stops)
	}
	return fmt.Sprintf("%s: report=%q controllers=1 %s", scenario, report, evidence), nil
}
