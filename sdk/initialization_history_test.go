package sandbox_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/smolcloud"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestLocalConcurrentInitializationOwnership holds an actual provider create
// response while competing Updates attempt to replace the admitted request.
func TestLocalConcurrentInitializationOwnership(t *testing.T) {
	if os.Getenv("HARNESS_LOCAL_TEST") != "1" {
		t.Skip("set HARNESS_LOCAL_TEST=1 and start the local server on 127.0.0.1:17293")
	}
	for _, competing := range []string{"same", "changed", "invalid", "legacy"} {
		for _, shutdown := range []string{"stop", "cancel"} {
			t.Run(competing+"/"+shutdown, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				entered, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				defer releaseOnce.Do(func() { close(release) })
				var mu sync.Mutex
				creates, deletes := 0, 0
				var machine map[string]any
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.Header.Get("Authorization") != "Bearer synthetic-init-token" {
						t.Error("missing outside provider authorization")
					}
					switch r.Method + " " + r.URL.Path {
					case "POST /v1/machines":
						mu.Lock()
						creates++
						count := creates
						mu.Unlock()
						if count != 1 {
							t.Error("competing initialization reached allocation")
							w.WriteHeader(http.StatusConflict)
							return
						}
						var request struct {
							Name    string            `json:"name"`
							Labels  map[string]string `json:"labels"`
							Network json.RawMessage   `json:"network"`
						}
						if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						mu.Lock()
						machine = map[string]any{"id": "owned-init", "name": request.Name, "labels": request.Labels, "network": request.Network, "state": "started", "ready": true}
						mu.Unlock()
						close(entered)
						<-release
						mu.Lock()
						defer mu.Unlock()
						_ = json.NewEncoder(w).Encode(machine)
					case "GET /v1/machines/owned-init":
						mu.Lock()
						defer mu.Unlock()
						_ = json.NewEncoder(w).Encode(machine)
					case "POST /v1/machines/owned-init/start":
						w.WriteHeader(http.StatusNoContent)
					case "DELETE /v1/machines/owned-init":
						mu.Lock()
						defer mu.Unlock()
						deletes++
						w.WriteHeader(http.StatusNoContent)
					default:
						t.Errorf("unexpected provider request: %s %s", r.Method, r.URL.Path)
						w.WriteHeader(http.StatusBadRequest)
					}
				}))
				defer server.Close()
				t.Setenv("SMOL_CLOUD_URL", server.URL)
				t.Setenv("SMOL_CLOUD_TOKEN", "synthetic-init-token")
				c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293"})
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				queue := fmt.Sprintf("concurrent-init-%s-%s-%d", competing, shutdown, time.Now().UnixNano())
				w := worker.New(c, queue, worker.Options{})
				sandbox.Register(w, c)
				if err := w.Start(); err != nil {
					t.Fatal(err)
				}
				defer w.Stop()
				defer releaseOnce.Do(func() { close(release) })
				run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, controller.SandboxWorkflow, controller.SandboxLocalState{})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CancelWorkflow(context.Background(), run.GetID(), run.GetRunID())
				config := map[string]string{"image": "bounded-original", "bootstrap": "none", "lifecycle": "fresh", "branchable": "false", "network": "blocked"}
				init := func(id string, config map[string]string, stage client.WorkflowUpdateStage) (client.WorkflowUpdateHandle, error) {
					return c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateID: id, UpdateName: controller.SandboxInitUpdate, WaitForStage: stage, Args: []any{controller.SandboxInitInput{ComputeProvider: compute.ProviderDetails{Type: compute.ProviderTypeSmolCloud, Config: config}, IdleTimeout: sandbox.NoIdleTimeout}}})
				}
				first, err := init("original-init", config, client.WorkflowUpdateStageAccepted)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("first provider create did not start:", ctx.Err())
				}
				retry, err := init("original-init", maps.Clone(config), client.WorkflowUpdateStageAccepted)
				if err != nil {
					t.Fatal("same UpdateID retry was rejected:", err)
				}
				other := maps.Clone(config)
				switch competing {
				case "changed":
					other["image"] = "different-image"
				case "invalid":
					delete(other, "image")
				case "legacy":
					other = map[string]string{"image": "legacy-image"}
				}
				second, err := init("competing-init", other, client.WorkflowUpdateStageCompleted)
				if err == nil {
					err = second.Get(ctx, nil)
				}
				if err == nil || !strings.Contains(err.Error(), "sandbox initialization in progress") {
					t.Fatalf("competing init was not rejected by the ownership guard: %v", err)
				}
				state, err := c.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
				if err != nil || state.WorkflowExecutionInfo.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
					t.Fatalf("owner closed while first start was pending: %v %v", state, err)
				}
				mu.Lock()
				beforeCreates, beforeDeletes := creates, deletes
				mu.Unlock()
				if beforeCreates != 1 || beforeDeletes != 0 {
					t.Fatalf("competing effects: creates=%d deletes=%d", beforeCreates, beforeDeletes)
				}
				if shutdown == "cancel" {
					if err := c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()); err != nil {
						t.Fatal(err)
					}
					waitInitializationCancellation(t, ctx, c, run)
				}
				releaseOnce.Do(func() { close(release) })
				if err := first.Get(ctx, nil); err != nil {
					t.Fatalf("admitted initialization lost its result: %v", err)
				}
				if err := retry.Get(ctx, nil); err != nil {
					t.Fatal("same UpdateID retry lost its result:", err)
				}
				if shutdown == "stop" {
					if err := c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), controller.SandboxStopSignal, nil); err != nil {
						t.Fatal(err)
					}
				}
				err = run.Get(ctx, nil)
				if (shutdown == "stop" && err != nil) || (shutdown == "cancel" && !temporal.IsCanceledError(err)) {
					t.Fatalf("owner terminal result: %v", err)
				}
				mu.Lock()
				finalCreates, finalDeletes := creates, deletes
				mu.Unlock()
				if finalCreates != 1 || finalDeletes != 1 {
					t.Fatalf("owner cleanup: creates=%d deletes=%d", finalCreates, finalDeletes)
				}
				history := readHistory(t, ctx, c, run.GetID(), run.GetRunID())
				if got := scheduledActivityNames(history); !reflect.DeepEqual(got, []string{"StartSandbox", "StopSandbox"}) {
					t.Fatalf("initialization routing: %v", got)
				}
				for _, event := range history.Events {
					attrs := event.GetActivityTaskScheduledEventAttributes()
					if attrs == nil || attrs.ActivityType.Name != "StopSandbox" {
						continue
					}
					var stop controller.StopSandboxInput
					if err := converter.GetDefaultDataConverter().FromPayloads(attrs.Input, &stop); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(stop.Provider.Config, config) || stop.Status == nil || stop.Status.InstanceID != "owned-init" || stop.Status.ProviderType != compute.ProviderTypeSmolCloud || stop.Status.TaskQueueName != "sandbox-"+run.GetID() || stop.Status.ConfigurationDigest == "" {
						t.Fatalf("cleanup lost admitted configuration/identity: %+v", stop)
					}
				}
				replayer := worker.NewWorkflowReplayer()
				replayer.RegisterWorkflow(controller.SandboxWorkflow)
				if err := replayer.ReplayWorkflowHistoryWithOptions(nil, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}}); err != nil {
					t.Fatal("concurrent initialization replay:", err)
				}
			})
		}
	}
}

// waitInitializationCancellation waits for a completed Workflow Task after the
// cancel event, so the held start reply arrives after the owner handles cancellation.
func waitInitializationCancellation(t *testing.T, ctx context.Context, c client.Client, run client.WorkflowRun) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		requested := false
		for _, event := range readHistory(t, ctx, c, run.GetID(), run.GetRunID()).Events {
			if event.GetWorkflowExecutionCancelRequestedEventAttributes() != nil {
				requested = true
			}
			if requested && event.GetWorkflowTaskCompletedEventAttributes() != nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("owner did not process cancellation:", ctx.Err())
		case <-ticker.C:
		}
	}
}
