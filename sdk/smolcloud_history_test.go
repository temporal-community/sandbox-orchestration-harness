package sandbox_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/smolcloud"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

// smolCloudHistoryCases are SmolCloud configurations recorded before the policy settings existed.
// Their histories must replay because Workflow code interprets only the policy settings.
var smolCloudHistoryCases = map[string]map[string]string{
	"plain":         {"image": "image"},
	"missing-image": {"network": "blocked"},
	"opaque-key":    {"image": "image", "workload": "batch"},
	"wide-cpu":      {"image": "image", "cpus": "2147483648"},
}

// smolCloudHistoryWorkflow retains the Workflow type name recorded in the SmolCloud baseline histories.
// A configuration error ends the sandbox but completes this Workflow.
func smolCloudHistoryWorkflow(ctx workflow.Context, config map[string]string) error {
	s, err := sandbox.NewSandbox(ctx, sandbox.Provider{Type: compute.ProviderTypeSmolCloud, Config: config}, sandbox.WithIdleTimeout(sandbox.NoIdleTimeout))
	if err != nil {
		return nil
	}
	if _, err := s.ExecuteCommand(ctx, "true"); err != nil {
		return err
	}
	return s.Stop(ctx)
}

func TestRetainedSmolCloudBaselineReplay(t *testing.T) {
	for name := range smolCloudHistoryCases {
		for _, role := range []string{"parent", "controller"} {
			file := "baseline-smolcloud-" + name + "-" + role + ".json"
			t.Run(file, func(t *testing.T) {
				r := worker.NewWorkflowReplayer()
				r.RegisterWorkflow(smolCloudHistoryWorkflow)
				r.RegisterWorkflow(controller.SandboxWorkflow)
				if err := r.ReplayWorkflowHistoryFromJSONFile(nil, filepath.Join("testdata", file)); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestCaptureSmolCloudBaselineHistory records SmolCloud executions against a local fake endpoint.
// HARNESS_HISTORY_CAPTURE selects the output directory.
func TestCaptureSmolCloudBaselineHistory(t *testing.T) {
	dir := os.Getenv("HARNESS_HISTORY_CAPTURE")
	if dir == "" {
		t.Skip("history capture is an explicit local operation")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/machines":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "machine", "network": map[string]any{"mode": "blocked"}})
		case r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "running", "ready": true})
		case strings.HasSuffix(r.URL.Path, "/exec"):
			_ = json.NewEncoder(w).Encode(map[string]any{"stdout": "ok", "exitCode": 0})
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	t.Setenv("SMOL_CLOUD_TOKEN", "synthetic")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293", Identity: "history-fixture-client"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	queue := "smolcloud-capture-" + time.Now().Format("150405.000000000")
	w := worker.New(c, queue, worker.Options{Identity: "history-fixture-worker"})
	sandbox.Register(w, c)
	w.RegisterWorkflow(smolCloudHistoryWorkflow)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	for name, config := range smolCloudHistoryCases {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + name, TaskQueue: queue}, smolCloudHistoryWorkflow, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := run.Get(ctx, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		parent := readHistory(t, ctx, c, run.GetID(), run.GetRunID())
		var childID, childRun string
		for _, e := range parent.Events {
			if a := e.GetChildWorkflowExecutionStartedEventAttributes(); a != nil {
				childID, childRun = a.WorkflowExecution.WorkflowId, a.WorkflowExecution.RunId
			}
		}
		if childID == "" {
			t.Fatalf("%s: no controller execution", name)
		}
		for role, h := range map[string]*historypb.History{"parent": parent, "controller": readHistory(t, ctx, c, childID, childRun)} {
			data, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(h)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "baseline-smolcloud-"+name+"-"+role+".json"), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
