package sandbox_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

type historyProvider struct{ compute.Provider }

func (*historyProvider) Start(_ context.Context, queue string) (*compute.ProviderStatus, error) {
	return &compute.ProviderStatus{InstanceID: queue}, nil
}
func (*historyProvider) Stop(context.Context, *compute.ProviderStatus) error { return nil }
func (*historyProvider) ExecuteCommand(_ context.Context, _ *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	return &compute.CommandResult{Stdout: cmd}, nil
}

var historyRegistration sync.Once

func registerHistoryProvider() {
	historyRegistration.Do(func() {
		compute.Register("history-fake", func(map[string]string) (compute.Provider, error) { return &historyProvider{}, nil })
	})
}

func legacyHistoryWorkflow(ctx workflow.Context) error {
	s, err := sandbox.NewSandbox(ctx, sandbox.Provider{Type: "history-fake"}, sandbox.WithIdleTimeout(sandbox.NoIdleTimeout))
	if err != nil {
		return err
	}
	result, err := s.ExecuteCommand(ctx, "bounded-process")
	if err != nil {
		return err
	}
	if result.Stdout != "bounded-process" {
		return errors.New("unexpected command result")
	}
	return s.Stop(ctx)
}

func TestRetainedLegacyReplay(t *testing.T) {
	registerHistoryProvider()
	for _, name := range []string{"legacy-parent.json", "legacy-controller.json"} {
		t.Run(name, func(t *testing.T) {
			r := worker.NewWorkflowReplayer()
			r.RegisterWorkflow(legacyHistoryWorkflow)
			r.RegisterWorkflow(controller.SandboxWorkflow)
			if err := r.ReplayWorkflowHistoryFromJSONFile(nil, filepath.Join("testdata", name)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestCaptureLegacyHistory records local server execution with fake compute.
// HARNESS_HISTORY_CAPTURE selects the output directory for both histories.
func TestCaptureLegacyHistory(t *testing.T) {
	dir := os.Getenv("HARNESS_HISTORY_CAPTURE")
	if dir == "" {
		t.Skip("history capture is an explicit local operation")
	}
	registerHistoryProvider()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293", Identity: "history-fixture-client"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	queue := "legacy-capture-" + time.Now().Format("150405.000000000")
	w := worker.New(c, queue, worker.Options{Identity: "history-fixture-worker"})
	sandbox.Register(w, c)
	w.RegisterWorkflow(legacyHistoryWorkflow)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, legacyHistoryWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Get(ctx, nil); err != nil {
		t.Fatal(err)
	}
	parent := readHistory(t, ctx, c, run.GetID(), run.GetRunID())
	var childID, childRun string
	for _, e := range parent.Events {
		if a := e.GetChildWorkflowExecutionStartedEventAttributes(); a != nil {
			childID, childRun = a.WorkflowExecution.WorkflowId, a.WorkflowExecution.RunId
		}
	}
	if childID == "" {
		t.Fatal("no controller execution")
	}
	for name, h := range map[string]*historypb.History{"legacy-parent.json": parent, "legacy-controller.json": readHistory(t, ctx, c, childID, childRun)} {
		data, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(h)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func readHistory(t *testing.T, ctx context.Context, c client.Client, id, run string) *historypb.History {
	t.Helper()
	h := &historypb.History{}
	it := c.GetWorkflowHistory(ctx, id, run, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		event, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		h.Events = append(h.Events, event)
	}
	return h
}
