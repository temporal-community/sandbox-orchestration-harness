package sandbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

type nativeWorkflowInput struct {
	Provider    compute.ProviderDetails
	DisableIdle bool
	Fresh       bool
}

func nativeProcessWorkflow(ctx workflow.Context, input nativeWorkflowInput) error {
	ready := false
	if err := workflow.SetQueryHandler(ctx, "native-ready", func() (bool, error) { return ready, nil }); err != nil {
		return err
	}
	var options []sandbox.SandboxOption
	if input.DisableIdle {
		options = append(options, sandbox.WithIdleTimeout(sandbox.NoIdleTimeout))
	}
	s, err := sandbox.NewSandbox(ctx, input.Provider, options...)
	if err != nil {
		return err
	}
	result, err := s.ExecuteCommand(ctx, "read-native-output")
	if err != nil {
		return err
	}
	if result.Stdout != "native-process" {
		return errors.New("native process output was not retained")
	}
	if input.Fresh {
		ref, err := s.Ref()
		if err != nil {
			return err
		}
		attached, err := sandbox.AttachToSandbox(ref)
		if err != nil {
			return err
		}
		if _, err := attached.Snapshot(ctx); err == nil {
			return errors.New("fresh attached snapshot was accepted")
		}
		if err := attached.Suspend(ctx); err == nil {
			return errors.New("fresh attached suspend was accepted")
		}
	}
	ready = true
	workflow.GetSignalChannel(ctx, "native-finish").Receive(ctx, nil)
	return s.Stop(ctx)
}

func scheduledActivityNames(history *historypb.History) []string {
	var names []string
	for _, event := range history.Events {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			names = append(names, attrs.ActivityType.Name)
		}
	}
	return names
}

// TestLocalNativeProcessHistory uses only the local Temporal server and a fake
// provider. The only Temporal Worker polls the orchestration queue on the host.
func TestLocalNativeProcessHistory(t *testing.T) {
	if os.Getenv("HARNESS_LOCAL_TEST") != "1" {
		t.Skip("set HARNESS_LOCAL_TEST=1 for local server conformance")
	}
	for _, name := range []string{"default-idle", "no-idle-timeout", "fresh-policy"} {
		disableIdle := name != "default-idle"
		t.Run(name, func(t *testing.T) {
			b := &nativeBoundary{LoseFirstReply: true}
			typ := registerNativeBoundary(b)
			provider := compute.ProviderDetails{Type: typ, Config: map[string]string{"workload": "printf native-process", "provider-owned-extra": "retained"}}
			if name == "fresh-policy" {
				compute.RegisterPolicy(typ, func(map[string]string) (compute.ExecutionPolicy, error) {
					return compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, nil
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			queue := fmt.Sprintf("native-process-%s-%d", name, time.Now().UnixNano())
			w := worker.New(c, queue, worker.Options{})
			sandbox.Register(w, c)
			w.RegisterWorkflow(nativeProcessWorkflow)
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			defer w.Stop()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, nativeProcessWorkflow, nativeWorkflowInput{Provider: provider, DisableIdle: disableIdle, Fresh: name == "fresh-policy"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.CancelWorkflow(context.Background(), run.GetID(), run.GetRunID())
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				value, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "native-ready")
				var ready bool
				if err == nil {
					if err := value.Get(&ready); err != nil {
						t.Fatal(err)
					}
				}
				if ready {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("native workflow did not become ready:", ctx.Err())
				case <-ticker.C:
				}
			}
			parent := readHistory(t, ctx, c, run.GetID(), run.GetRunID())
			var childID, childRun string
			for _, event := range parent.Events {
				if attrs := event.GetChildWorkflowExecutionStartedEventAttributes(); attrs != nil {
					childID, childRun = attrs.WorkflowExecution.WorkflowId, attrs.WorkflowExecution.RunId
				}
			}
			if childID == "" {
				t.Fatal("controller workflow was not created")
			}
			value, err := c.QueryWorkflow(ctx, childID, childRun, controller.SandboxStateQuery)
			if err != nil {
				t.Fatal(err)
			}
			var state controller.SandboxState
			if err := value.Get(&state); err != nil {
				t.Fatal(err)
			}
			wantIdle := time.Duration(0)
			if disableIdle {
				wantIdle = sandbox.NoIdleTimeout
			}
			if state.Lifecycle != controller.SandboxLifecycleRunning || state.IdleTimeout != wantIdle || state.ComputeProvider == nil || !reflect.DeepEqual(*state.ComputeProvider, provider) {
				t.Fatalf("controller did not retain input: %+v", state)
			}
			b.mu.Lock()
			starts := append([]nativeLaunch(nil), b.Starts...)
			resource := b.Resources["sandbox-"+childID]
			resourceCount := len(b.Resources)
			b.mu.Unlock()
			if len(starts) != 2 || resourceCount != 1 {
				t.Fatalf("starts=%+v resource count=%d", starts, resourceCount)
			}
			for i, launch := range starts {
				if launch.Queue != "sandbox-"+childID || !reflect.DeepEqual(launch.Config, provider.Config) || launch.Attempt != int32(i+1) {
					t.Fatalf("actual Activity retry changed identity/config: %+v", launch)
				}
			}
			if state.Status == nil || state.Status.InstanceID != resource.ID || resource.Output != "native-process" {
				t.Fatalf("retained resource=%+v state=%+v", resource, state)
			}
			if err := c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "native-finish", nil); err != nil {
				t.Fatal(err)
			}
			if err := run.Get(ctx, nil); err != nil {
				t.Fatal(err)
			}
			b.mu.Lock()
			stops := append([]string(nil), b.Stops...)
			b.mu.Unlock()
			if !reflect.DeepEqual(stops, []string{resource.ID}) {
				t.Fatalf("Stop did not receive original resource: %v", stops)
			}
			parent = readHistory(t, ctx, c, run.GetID(), run.GetRunID())
			child := readHistory(t, ctx, c, childID, childRun)
			var idleTimers []time.Duration
			for _, event := range child.Events {
				if attrs := event.GetTimerStartedEventAttributes(); attrs != nil {
					idleTimers = append(idleTimers, attrs.StartToFireTimeout.AsDuration())
				}
			}
			if disableIdle {
				if len(idleTimers) != 0 {
					t.Fatalf("NoIdleTimeout scheduled idle timers: %v", idleTimers)
				}
			} else if !reflect.DeepEqual(idleTimers, []time.Duration{5 * time.Minute}) {
				t.Fatalf("default idle timers changed: %v", idleTimers)
			}
			wantParent := []string{"SendSandboxInit", "SendSandboxExecuteCommand"}
			if name == "fresh-policy" {
				wantParent = append(wantParent, "SendSandboxSnapshot", "SendSandboxSuspend")
			}
			if got := scheduledActivityNames(parent); !reflect.DeepEqual(got, wantParent) {
				t.Fatalf("parent Activities=%v", got)
			}
			if got := scheduledActivityNames(child); !reflect.DeepEqual(got, []string{"StartSandbox", "ExecuteCommand", "StopSandbox"}) {
				t.Fatalf("controller Activities=%v", got)
			}
			for label, history := range map[string]*historypb.History{"parent": parent, "controller": child} {
				for _, event := range history.Events {
					if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.TaskQueue.Name != queue {
						t.Fatalf("Activity %s routed outside orchestration queue: %s", attrs.ActivityType.Name, attrs.TaskQueue.Name)
					}
				}
				replayer := worker.NewWorkflowReplayer()
				replayer.RegisterWorkflow(nativeProcessWorkflow)
				replayer.RegisterWorkflow(controller.SandboxWorkflow)
				original := workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}
				if label == "controller" {
					original = workflow.Execution{ID: childID, RunID: childRun}
				}
				if err := replayer.ReplayWorkflowHistoryWithOptions(nil, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: original}); err != nil {
					t.Fatalf("fresh %s replay failed: %v", label, err)
				}
				if dir := os.Getenv("HARNESS_LOCAL_HISTORY_DIR"); dir != "" {
					if err := os.MkdirAll(dir, 0700); err != nil {
						t.Fatal(err)
					}
					data, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(history)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dir, name+"-"+label+".json"), data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				t.Logf("replayed fresh %s history: %d events, Activities %v", label, len(history.Events), scheduledActivityNames(history))
			}
		})
	}
}

func TestLocalPublicSandboxRejectsInvalidProviderBeforeAllocation(t *testing.T) {
	if os.Getenv("HARNESS_LOCAL_TEST") != "1" {
		t.Skip("set HARNESS_LOCAL_TEST=1 for local server conformance")
	}
	for _, name := range []string{"unknown", "missing-workload", "invalid-workload"} {
		t.Run(name, func(t *testing.T) {
			b := &nativeBoundary{}
			typ := registerNativeBoundary(b)
			config := map[string]string{}
			if name == "unknown" {
				typ = compute.ProviderType("unregistered-" + string(typ))
			}
			if name == "invalid-workload" {
				config["workload"] = "unsupported-process"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			queue := fmt.Sprintf("native-invalid-%s-%d", name, time.Now().UnixNano())
			w := worker.New(c, queue, worker.Options{})
			sandbox.Register(w, c)
			w.RegisterWorkflow(nativeProcessWorkflow)
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			defer w.Stop()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, nativeProcessWorkflow, nativeWorkflowInput{Provider: compute.ProviderDetails{Type: typ, Config: config}})
			if err != nil {
				t.Fatal(err)
			}
			err = run.Get(ctx, nil)
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || !app.NonRetryable() {
				t.Fatalf("expected nonretryable SDK failure, got %v", err)
			}
			if name != "unknown" && app.Type() != "ProviderConfigError" {
				t.Fatalf("constructor error type changed: %v", err)
			}
			b.mu.Lock()
			starts, resources := len(b.Starts), len(b.Resources)
			b.mu.Unlock()
			if starts != 0 || resources != 0 {
				t.Fatalf("invalid public SDK request allocated: starts=%d resources=%d", starts, resources)
			}
			parent := readHistory(t, ctx, c, run.GetID(), run.GetRunID())
			if got := scheduledActivityNames(parent); !reflect.DeepEqual(got, []string{"SendSandboxInit"}) {
				t.Fatalf("SDK invalid request Activities=%v", got)
			}
			var childID, childRun string
			for _, event := range parent.Events {
				if attrs := event.GetChildWorkflowExecutionStartedEventAttributes(); attrs != nil {
					childID, childRun = attrs.WorkflowExecution.WorkflowId, attrs.WorkflowExecution.RunId
				}
			}
			if childID == "" {
				t.Fatal("missing receiving controller")
			}
			child := readHistory(t, ctx, c, childID, childRun)
			got := scheduledActivityNames(child)
			if name == "unknown" {
				if len(got) != 0 {
					t.Fatalf("unknown provider reached Activity: %v", got)
				}
			} else if !reflect.DeepEqual(got, []string{"StartSandbox"}) {
				t.Fatalf("constructor validation did not run on existing Start Activity: %v", got)
			}
		})
	}
}
