package sandbox_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/command"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type attachmentCall struct {
	Operation string
	Config    map[string]string
	Resource  string
	Status    *compute.ProviderStatus
}

type attachmentBoundary struct {
	processBoundary
	callMu sync.Mutex
	Calls  []attachmentCall
	Issued map[string]compute.ProviderStatus
}

type attachmentProvider struct {
	*processProvider
	boundary *attachmentBoundary
	typ      compute.ProviderType
	fresh    bool
}

type pendingAttachmentProvider struct {
	*attachmentProvider
	started chan struct{}
	release chan struct{}
	fail    bool
}

func (p *pendingAttachmentProvider) Start(ctx context.Context, queue string) (*compute.ProviderStatus, error) {
	var status *compute.ProviderStatus
	var err error
	if !p.fail {
		status, err = p.attachmentProvider.Start(context.WithoutCancel(ctx), queue)
	}
	close(p.started)
	<-p.release
	if p.fail {
		return nil, temporal.NewNonRetryableApplicationError("allocation refused", "ProviderConfigError", nil)
	}
	return status, err
}

// TestLocalPendingStartCancellationCleanup waits until the controller processes
// cancellation, then delivers the provider reply from an in-flight allocation.
func TestLocalPendingStartCancellationCleanup(t *testing.T) {
	if os.Getenv("HARNESS_LOCAL_TEST") != "1" {
		t.Skip("set HARNESS_LOCAL_TEST=1 for local server conformance")
	}
	for _, scenario := range []struct{ fresh, fail, cancel bool }{{true, false, true}, {true, true, true}, {true, true, false}, {false, false, true}, {false, true, true}, {false, true, false}} {
		fail := scenario.fail
		t.Run(fmt.Sprintf("fresh=%t/failed-start=%t/canceled=%t", scenario.fresh, fail, scenario.cancel), func(t *testing.T) {
			b := &attachmentBoundary{processBoundary: processBoundary{Resources: make(map[string]processResource)}}
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			typ := compute.ProviderType("pending-start-" + uuid.NewString())
			compute.Register(typ, func(config map[string]string) (compute.Provider, error) {
				return &pendingAttachmentProvider{attachmentProvider: &attachmentProvider{processProvider: &processProvider{boundary: &b.processBoundary, config: maps.Clone(config)}, boundary: b, typ: typ, fresh: scenario.fresh}, started: started, release: release, fail: fail}, nil
			})
			compute.RegisterPolicy(typ, func(map[string]string) (compute.ExecutionPolicy, error) {
				if scenario.fresh {
					return compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, nil
				}
				return compute.ExecutionPolicy{}, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			queue := "pending-start-" + uuid.NewString()
			w := worker.New(c, queue, worker.Options{})
			sandbox.Register(w, c)
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			defer w.Stop()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, controller.SandboxWorkflow, controller.SandboxLocalState{WorkflowID: "outside-owner", RunID: "outside-run"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.CancelWorkflow(context.Background(), run.GetID(), run.GetRunID())
			provider := compute.ProviderDetails{Type: typ, Config: map[string]string{"workload": "printf process-output"}}
			update, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: controller.SandboxInitUpdate, WaitForStage: client.WorkflowUpdateStageAccepted, Args: []any{controller.SandboxInitInput{ComputeProvider: provider, IdleTimeout: sandbox.NoIdleTimeout}}})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("provider did not start:", ctx.Err())
			}
			if scenario.cancel {
				if err := c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()); err != nil {
					t.Fatal(err)
				}
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				processed := false
				for !processed {
					history := readHistory(t, ctx, c, run.GetID(), run.GetRunID())
					canceled := false
					for _, event := range history.Events {
						if event.GetWorkflowExecutionCancelRequestedEventAttributes() != nil {
							canceled = true
						}
						if canceled && event.GetWorkflowTaskCompletedEventAttributes() != nil {
							processed = true
						}
					}
					if !processed {
						select {
						case <-ctx.Done():
							t.Fatal("controller did not process cancellation:", ctx.Err())
						case <-ticker.C:
						}
					}
				}
			}
			releaseOnce.Do(func() { close(release) })
			updateErr := update.Get(ctx, nil)
			if fail && (updateErr == nil || !strings.Contains(updateErr.Error(), "allocation refused")) {
				t.Fatal("failed allocation update lost its original error:", updateErr)
			}
			if !fail && updateErr != nil {
				t.Fatal("successful late allocation update failed:", updateErr)
			}
			if err := run.Get(ctx, nil); (scenario.cancel && !temporal.IsCanceledError(err)) || (!scenario.cancel && err != nil) {
				t.Fatalf("pending start terminal result: %v", err)
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			if fail {
				if len(b.Resources) != 0 || len(b.Stops) != 0 {
					t.Fatalf("failed allocation caused cleanup: resources=%v stops=%v", b.Resources, b.Stops)
				}
			} else {
				resource := b.Resources["sandbox-"+run.GetID()]
				if len(b.Resources) != 1 || len(b.Starts) != 1 || !reflect.DeepEqual(b.Stops, []string{resource.ID}) {
					t.Fatalf("late allocation was not cleaned once: starts=%v resources=%v stops=%v", b.Starts, b.Resources, b.Stops)
				}
			}
			history := readHistory(t, ctx, c, run.GetID(), run.GetRunID())
			want := []string{"StartSandbox"}
			if !fail {
				want = append(want, "StopSandbox")
			}
			if got := scheduledActivityNames(history); !reflect.DeepEqual(got, want) {
				t.Fatalf("pending cancellation routing: %v", got)
			}
			if !fail {
				assertAttachmentActivityStatus(t, history, typ, "sandbox-"+run.GetID(), b.Resources["sandbox-"+run.GetID()].ID, scenario.fresh)
			}
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(controller.SandboxWorkflow)
			if err := replayer.ReplayWorkflowHistoryWithOptions(nil, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func (p *attachmentProvider) Start(ctx context.Context, queue string) (*compute.ProviderStatus, error) {
	status, err := p.processProvider.Start(ctx, queue)
	if err != nil {
		return nil, err
	}
	if p.fresh {
		// This marker tests context transport, independently of provider digest code.
		status.ProviderType = p.typ
		status.TaskQueueName = queue
		status.ConfigurationDigest = "synthetic-attachment-configuration"
	}
	p.boundary.callMu.Lock()
	defer p.boundary.callMu.Unlock()
	if p.boundary.Issued == nil {
		p.boundary.Issued = make(map[string]compute.ProviderStatus)
	}
	p.boundary.Issued[status.InstanceID] = *status
	return status, nil
}

func (p *attachmentProvider) validateStatus(status *compute.ProviderStatus) error {
	p.boundary.callMu.Lock()
	defer p.boundary.callMu.Unlock()
	if status != nil {
		if issued, ok := p.boundary.Issued[status.InstanceID]; ok && reflect.DeepEqual(*status, issued) {
			return nil
		}
	}
	return temporal.NewNonRetryableApplicationError("fake provider lost issued resource context", "FakeResourceBindingError", nil)
}

func (p *attachmentProvider) record(operation string, status *compute.ProviderStatus) {
	p.boundary.callMu.Lock()
	defer p.boundary.callMu.Unlock()
	resource := ""
	if status != nil {
		resource = status.InstanceID
	}
	p.boundary.Calls = append(p.boundary.Calls, attachmentCall{Operation: operation, Config: maps.Clone(p.config), Resource: resource, Status: status})
}

func (p *attachmentProvider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, command string, _ *command.Options) (*compute.CommandResult, error) {
	p.record("command", status)
	if err := p.validateStatus(status); err != nil {
		return nil, err
	}
	return p.processProvider.ExecuteCommand(ctx, status, command, nil)
}
func (p *attachmentProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	p.record("stop", status)
	if err := p.validateStatus(status); err != nil {
		return err
	}
	return p.processProvider.Stop(ctx, status)
}
func (p *attachmentProvider) Suspend(context.Context, *compute.ProviderStatus) error {
	p.record("suspend", nil)
	return errors.ErrUnsupported
}
func (p *attachmentProvider) Resume(context.Context, *compute.ProviderStatus) error {
	p.record("resume", nil)
	return errors.ErrUnsupported
}
func (p *attachmentProvider) Snapshot(context.Context, *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	p.record("snapshot", nil)
	return compute.SandboxPostSnapshotRunning, nil, errors.ErrUnsupported
}
func (p *attachmentProvider) DeleteSnapshot(context.Context, *compute.ProviderSnapshot) error {
	p.record("delete-snapshot", nil)
	return errors.ErrUnsupported
}
func (p *attachmentProvider) StartFromSnapshot(context.Context, string, *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	p.record("restore", nil)
	return nil, errors.ErrUnsupported
}

type attachmentOwnerInput struct {
	Provider     compute.ProviderDetails
	ExplicitStop bool
}

func attachmentOwnerWorkflow(ctx workflow.Context, input attachmentOwnerInput) error {
	ref := ""
	if err := workflow.SetQueryHandler(ctx, "attachment-reference", func() (string, error) { return ref, nil }); err != nil {
		return err
	}
	s, err := sandbox.NewSandbox(ctx, input.Provider, sandbox.WithIdleTimeout(sandbox.NoIdleTimeout))
	if err != nil {
		return err
	}
	ref, err = s.Ref()
	if err != nil {
		return err
	}
	if _, err := s.ExecuteCommand(ctx, "read-process-output"); err != nil {
		return err
	}
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(workflow.GetSignalChannel(ctx, "owner-finish"), func(ch workflow.ReceiveChannel, _ bool) { ch.Receive(ctx, nil) })
	selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
	selector.Select(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if input.ExplicitStop {
		if err := s.Stop(ctx); err != nil {
			return err
		}
		return s.Stop(ctx)
	}
	return nil
}

type attachmentCallerInput struct {
	Ref   string
	Fresh bool
	Wait  bool
	Stop  bool
}

func TestAttachmentReferenceContract(t *testing.T) {
	ref := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"sandbox_id":"existing-controller"}`))
	attached, err := sandbox.AttachToSandbox(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, owns := attached.(sandbox.OwnedSandbox); owns {
		t.Fatal("attachment acquired lifecycle ownership")
	}
	if roundTrip, err := attached.Ref(); err != nil || roundTrip != ref {
		t.Fatalf("existing reference changed: %q, %v", roundTrip, err)
	}
	for _, raw := range []string{`{"v":2,"sandbox_id":"existing-controller"}`, `{"v":1}`, `{"sandbox_id":"existing-controller"}`, `{`} {
		if _, err := sandbox.AttachToSandbox(base64.RawURLEncoding.EncodeToString([]byte(raw))); err == nil {
			t.Fatalf("invalid reference accepted: %s", raw)
		}
	}
	if _, err := sandbox.AttachToSandbox("%"); err == nil {
		t.Fatal("malformed base64 reference accepted")
	}
}

func attachmentCallerWorkflow(ctx workflow.Context, input attachmentCallerInput) error {
	s, err := sandbox.AttachToSandbox(input.Ref)
	if err != nil {
		return err
	}
	if _, owns := s.(sandbox.OwnedSandbox); owns {
		return errors.New("attachment acquired lifecycle ownership")
	}
	ref, err := s.Ref()
	if err != nil {
		return err
	}
	if ref != input.Ref {
		return errors.New("attachment reference changed")
	}
	if input.Stop {
		return s.RequestStop(ctx)
	}
	result, err := s.ExecuteCommand(ctx, "read-process-output", sandbox.DisableAutoResume())
	if err != nil {
		return err
	}
	if result.Stdout != "process-output" {
		return errors.New("attachment reached a different resource")
	}
	if input.Fresh {
		checks := []struct {
			name string
			call func() error
		}{
			{"snapshot", func() error { _, err := s.Snapshot(ctx); return err }},
			{"suspend", func() error { return s.Suspend(ctx) }},
			{"resume", func() error { return s.Resume(ctx) }},
			{"delete-snapshot", func() error { return s.DeleteSnapshot(ctx, &compute.ProviderSnapshot{SnapshotID: "unadmitted"}) }},
		}
		for _, check := range checks {
			err := check.call()
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || (app.Type() != "ErrUnsupported" && !(check.name == "resume" && app.Type() == "NotSuspended")) {
				return fmt.Errorf("%s did not return policy refusal: %w", check.name, err)
			}
		}
	}
	if input.Wait {
		if err := workflow.SetQueryHandler(ctx, "attachment-ready", func() (bool, error) { return true, nil }); err != nil {
			return err
		}
		return workflow.Await(ctx, func() bool { return false })
	}
	return nil
}

// TestLocalAttachmentOwnershipHistory exercises real updates between independent
// Workflows. The provider runs only a bounded local subprocess.
func TestLocalAttachmentOwnershipHistory(t *testing.T) {
	if os.Getenv("HARNESS_LOCAL_TEST") != "1" {
		t.Skip("set HARNESS_LOCAL_TEST=1 for local server conformance")
	}
	for _, fresh := range []bool{false, true} {
		for _, shutdown := range []string{"explicit-stop", "owner-completion", "owner-cancellation", "delegated-stop"} {
			t.Run(fmt.Sprintf("fresh=%t/%s", fresh, shutdown), func(t *testing.T) {
				b := &attachmentBoundary{}
				b.Resources = make(map[string]processResource)
				typ := compute.ProviderType("attachment-" + uuid.NewString())
				compute.Register(typ, func(config map[string]string) (compute.Provider, error) {
					return &attachmentProvider{processProvider: &processProvider{boundary: &b.processBoundary, config: maps.Clone(config)}, boundary: b, typ: typ, fresh: fresh}, nil
				})
				if fresh {
					compute.RegisterPolicy(typ, func(map[string]string) (compute.ExecutionPolicy, error) {
						return compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, nil
					})
				}
				provider := compute.ProviderDetails{Type: typ, Config: map[string]string{"workload": "printf process-output", "image": "bounded-native-image", "opaque": "preserved"}}
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293"})
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				queue := "attachment-" + uuid.NewString()
				w := worker.New(c, queue, worker.Options{})
				sandbox.Register(w, c)
				w.RegisterWorkflow(attachmentOwnerWorkflow)
				w.RegisterWorkflow(attachmentCallerWorkflow)
				if err := w.Start(); err != nil {
					t.Fatal(err)
				}
				defer w.Stop()
				owner, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, attachmentOwnerWorkflow, attachmentOwnerInput{Provider: provider, ExplicitStop: shutdown == "explicit-stop"})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CancelWorkflow(context.Background(), owner.GetID(), owner.GetRunID())
				ref := waitAttachmentReference(t, ctx, c, owner)
				raw, err := base64.RawURLEncoding.DecodeString(ref)
				if err != nil {
					t.Fatal(err)
				}
				var payload struct {
					Version   int    `json:"v"`
					SandboxID string `json:"sandbox_id"`
				}
				if err := json.Unmarshal(raw, &payload); err != nil {
					t.Fatal(err)
				}
				if payload.Version != 1 || payload.SandboxID == "" {
					t.Fatalf("reference contract changed: %s", raw)
				}
				controllerRun := c.GetWorkflow(ctx, payload.SandboxID, "")
				for _, canceled := range []bool{false, true} {
					caller, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + uuid.NewString(), TaskQueue: queue}, attachmentCallerWorkflow, attachmentCallerInput{Ref: ref, Fresh: fresh, Wait: canceled})
					if err != nil {
						t.Fatal(err)
					}
					if canceled {
						waitAttachmentReady(t, ctx, c, caller)
						if err := c.CancelWorkflow(ctx, caller.GetID(), caller.GetRunID()); err != nil {
							t.Fatal(err)
						}
					}
					err = caller.Get(ctx, nil)
					if canceled && !temporal.IsCanceledError(err) {
						t.Fatalf("attachment cancellation: %v", err)
					}
					if !canceled && err != nil {
						t.Fatal(err)
					}
					assertAttachmentResource(t, ctx, c, payload.SandboxID, provider, b, 0)
					history := readHistory(t, ctx, c, caller.GetID(), caller.GetRunID())
					for _, event := range history.Events {
						if event.GetStartChildWorkflowExecutionInitiatedEventAttributes() != nil || event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes() != nil {
							t.Fatal("ordinary attachment acquired lifecycle effects")
						}
					}
					replayer := worker.NewWorkflowReplayer()
					replayer.RegisterWorkflow(attachmentCallerWorkflow)
					if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
						t.Fatal(err)
					}
				}
				switch shutdown {
				case "owner-cancellation":
					if err := c.CancelWorkflow(ctx, owner.GetID(), owner.GetRunID()); err != nil {
						t.Fatal(err)
					}
				case "delegated-stop":
					caller, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-stop", TaskQueue: queue}, attachmentCallerWorkflow, attachmentCallerInput{Ref: ref, Stop: true})
					if err != nil {
						t.Fatal(err)
					}
					if err := caller.Get(ctx, nil); err != nil {
						t.Fatal(err)
					}
					if err := controllerRun.Get(ctx, nil); err != nil {
						t.Fatal(err)
					}
					fallthrough
				default:
					if err := c.SignalWorkflow(ctx, owner.GetID(), owner.GetRunID(), "owner-finish", nil); err != nil {
						t.Fatal(err)
					}
				}
				err = owner.Get(ctx, nil)
				if shutdown == "owner-cancellation" {
					if !temporal.IsCanceledError(err) {
						t.Fatalf("owner cancellation: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if err := controllerRun.Get(ctx, nil); err != nil {
					t.Fatal(err)
				}
				assertAttachmentResource(t, ctx, c, payload.SandboxID, provider, b, 1)
				childHistory := readHistory(t, ctx, c, payload.SandboxID, "")
				assertAttachmentActivityStatus(t, childHistory, typ, "sandbox-"+payload.SandboxID, "resource-sandbox-"+payload.SandboxID, fresh)
				if got := scheduledActivityNames(childHistory); !reflect.DeepEqual(got, []string{"StartSandbox", "ExecuteCommand", "ExecuteCommand", "ExecuteCommand", "StopSandbox"}) {
					t.Fatalf("controller routing changed: %v", got)
				}
				replayer := worker.NewWorkflowReplayer()
				replayer.RegisterWorkflow(controller.SandboxWorkflow)
				if err := replayer.ReplayWorkflowHistoryWithOptions(nil, childHistory, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: payload.SandboxID}}); err != nil {
					t.Fatal(err)
				}
				// Closed references retain the existing external-signal error.
				closed, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-closed-stop", TaskQueue: queue}, attachmentCallerWorkflow, attachmentCallerInput{Ref: ref, Stop: true})
				if err != nil {
					t.Fatal(err)
				}
				var app *temporal.ApplicationError
				if err := closed.Get(ctx, nil); !errors.As(err, &app) || app.Type() != "UnknownExternalWorkflowExecutionError" {
					t.Fatalf("closed reference signaling changed: %v", err)
				}
				assertAttachmentResource(t, ctx, c, payload.SandboxID, provider, b, 1)
			})
		}
	}
}

func waitAttachmentReference(t *testing.T, ctx context.Context, c client.Client, run client.WorkflowRun) string {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "attachment-reference")
		var ref string
		if err == nil {
			if err := value.Get(&ref); err != nil {
				t.Fatal(err)
			}
			if ref != "" {
				return ref
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("owner did not become ready:", ctx.Err())
			return ""
		case <-ticker.C:
		}
	}
}

func waitAttachmentReady(t *testing.T, ctx context.Context, c client.Client, run client.WorkflowRun) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "attachment-ready")
		var ready bool
		if err == nil {
			if err := value.Get(&ready); err != nil {
				t.Fatal(err)
			}
			if ready {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("attachment did not become ready:", ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

func assertAttachmentResource(t *testing.T, ctx context.Context, c client.Client, id string, provider compute.ProviderDetails, b *attachmentBoundary, stops int) {
	t.Helper()
	value, err := c.QueryWorkflow(ctx, id, "", controller.SandboxStateQuery)
	if err != nil {
		t.Fatal(err)
	}
	var state controller.SandboxState
	if err := value.Get(&state); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	resource := b.Resources["sandbox-"+id]
	if len(b.Starts) != 1 || len(b.Resources) != 1 || len(b.Stops) != stops {
		t.Fatalf("resource ownership changed: starts=%v resources=%v stops=%v", b.Starts, b.Resources, b.Stops)
	}
	wantStatus := compute.ProviderStatus{InstanceID: resource.ID}
	policy, err := compute.ResolvePolicy(provider)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Lifecycle == compute.LifecycleFresh {
		wantStatus.ProviderType = provider.Type
		wantStatus.TaskQueueName = "sandbox-" + id
		wantStatus.ConfigurationDigest = "synthetic-attachment-configuration"
	}
	if !reflect.DeepEqual(state.Status, &wantStatus) || state.ComputeProvider == nil || !reflect.DeepEqual(*state.ComputeProvider, provider) || state.IdleTimeout != sandbox.NoIdleTimeout {
		t.Fatalf("attachment changed controller binding: %+v", state)
	}
	b.callMu.Lock()
	defer b.callMu.Unlock()
	for _, call := range b.Calls {
		if call.Operation != "command" && call.Operation != "stop" {
			t.Fatalf("forbidden provider call: %+v", call)
		}
		if call.Resource != resource.ID || !reflect.DeepEqual(call.Config, provider.Config) || !reflect.DeepEqual(call.Status, &wantStatus) {
			t.Fatalf("provider binding changed: %+v", call)
		}
	}
}

func assertAttachmentActivityStatus(t *testing.T, history *historypb.History, typ compute.ProviderType, queue, instance string, fresh bool) {
	t.Helper()
	want := &compute.ProviderStatus{InstanceID: instance}
	if fresh {
		want.ProviderType = typ
		want.TaskQueueName = queue
		want.ConfigurationDigest = "synthetic-attachment-configuration"
	}
	for _, event := range history.Events {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil {
			continue
		}
		var status *compute.ProviderStatus
		switch attrs.ActivityType.Name {
		case "ExecuteCommand":
			var input controller.ExecuteCommandInput
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.Input, &input); err != nil {
				t.Fatal(err)
			}
			status = input.Status
		case "StopSandbox":
			var input controller.StopSandboxInput
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.Input, &input); err != nil {
				t.Fatal(err)
			}
			status = input.Status
		default:
			continue
		}
		if !reflect.DeepEqual(status, want) {
			t.Fatalf("%s lost full resource status: got %+v, want %+v", attrs.ActivityType.Name, status, want)
		}
	}
}
