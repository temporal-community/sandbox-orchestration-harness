package sandbox

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	wfIface "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/workflow"
)

type (
	Provider         = compute.ProviderDetails
	CommandResult    = compute.CommandResult
	ProviderSnapshot = compute.ProviderSnapshot

	// Sandbox is the base interface for interacting with a running sandbox. AttachToSandbox
	// returns this interface directly. NewSandbox returns OwnedSandbox, which extends Sandbox
	// with a blocking Stop; use OwnedSandbox when you need to wait for the sandbox workflow
	// to fully complete on shutdown.
	Sandbox interface {
		ExecuteCommand(ctx workflow.Context, cmd string, opts ...ExecuteCommandOption) (*CommandResult, error)

		Suspend(ctx workflow.Context) error
		Resume(ctx workflow.Context) error
		// Snapshot may only be called when the sandbox is in the Running lifecycle
		// state; it returns an error if called while Pending, Suspended, Failed, or Deleted.
		Snapshot(ctx workflow.Context) (*ProviderSnapshot, error)
		DeleteSnapshot(ctx workflow.Context, snapshot *ProviderSnapshot) error

		// RequestStop signals the sandbox to shut down without waiting for it to finish.
		// There is no mechanism via a non-owning Sandbox to block until the sandbox workflow
		// fully exits; use Temporal's external workflow APIs if completion confirmation is required.
		// Use OwnedSandbox.Stop when you need to block until the sandbox workflow fully completes.
		RequestStop(ctx workflow.Context) error

		// Ref returns an opaque reference to this sandbox. Pass it to AttachToSandbox
		// in child or sibling workflows to route work to this same sandbox without
		// taking ownership of its lifecycle.
		Ref() (string, error)
	}

	// OwnedSandbox is returned by NewSandbox. It extends Sandbox with a blocking Stop
	// that waits for the sandbox workflow to fully complete before returning.
	OwnedSandbox interface {
		Sandbox
		// Stop signals the sandbox to shut down and blocks until the sandbox workflow
		// fully completes. Prefer Stop over calling RequestStop then manually waiting;
		// calling both is redundant but not harmful.
		Stop(ctx workflow.Context) error
	}
)

// sandboxRefData is the JSON payload encoded inside an opaque sandbox reference.
// Version must equal sandboxRefVersion; unknown versions are rejected so future
// format changes can be detected rather than silently misread.
type sandboxRefData struct {
	Version   int    `json:"v"`
	SandboxID string `json:"sandbox_id"`
}

const sandboxRefVersion = 1

func encodeRef(sandboxID string) (string, error) {
	data, err := json.Marshal(sandboxRefData{Version: sandboxRefVersion, SandboxID: sandboxID})
	if err != nil {
		return "", fmt.Errorf("sandbox: encode ref: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeRef(ref string) (sandboxRefData, error) {
	raw, err := base64.RawURLEncoding.DecodeString(ref)
	if err != nil {
		return sandboxRefData{}, fmt.Errorf("sandbox: invalid ref: %w", err)
	}
	var data sandboxRefData
	if err := json.Unmarshal(raw, &data); err != nil {
		return sandboxRefData{}, fmt.Errorf("sandbox: invalid ref: %w", err)
	}
	if data.Version != sandboxRefVersion {
		return sandboxRefData{}, fmt.Errorf("sandbox: unsupported ref version %d (expected %d)", data.Version, sandboxRefVersion)
	}
	if data.SandboxID == "" {
		return sandboxRefData{}, fmt.Errorf("sandbox: ref missing sandboxId")
	}
	return data, nil
}

// NoIdleTimeout disables automatic idle suspension when passed to WithIdleTimeout.
// By default a sandbox is suspended after 5 minutes of inactivity; passing this
// value opts out of that behaviour entirely.
const NoIdleTimeout = compute.NoIdleTimeout

// CleanupBehavior controls what happens to the sandbox when the creator workflow closes.
type CleanupBehavior int

const (
	// CleanupWithWorkflow cancels the sandbox when the creator workflow closes (default).
	CleanupWithWorkflow CleanupBehavior = iota
	// CleanupDisabled leaves the sandbox running after the creator workflow closes.
	// The caller is responsible for stopping the sandbox explicitly.
	CleanupDisabled
)

// ExecuteCommandOption is a functional option for ExecuteCommand.
type ExecuteCommandOption func(*executeCommandConfig)

type executeCommandConfig struct {
	disableAutoResume bool // zero value = auto-resume enabled
}

// DisableAutoResume opts out of automatic sandbox resume before command execution.
// By default ExecuteCommand resumes a suspended sandbox transparently; pass this
// option to receive an error instead.
func DisableAutoResume() ExecuteCommandOption {
	return func(c *executeCommandConfig) { c.disableAutoResume = true }
}

// SandboxOption is a functional option for NewSandbox.
type SandboxOption func(*sandboxConfig)

// sandboxConfig holds resolved options built from SandboxOption functions.
type sandboxConfig struct {
	cleanup     CleanupBehavior   // zero value == CleanupWithWorkflow
	idleTimeout time.Duration     // zero → use default (idleAutoSuspendTimeout)
	snapshot    *ProviderSnapshot // nil → fresh start
}

// WithCleanup sets the cleanup behavior for the sandbox.
func WithCleanup(b CleanupBehavior) SandboxOption {
	return func(c *sandboxConfig) { c.cleanup = b }
}

// WithIdleTimeout sets the idle auto-suspend timeout for the sandbox.
// If a command is not executed within d after the previous command completes,
// the sandbox is automatically suspended.
// Zero uses the default (5 minutes) — it does NOT disable auto-suspend.
// Pass NoIdleTimeout (-1) to disable auto-suspend entirely.
// Other negative values are rejected by NewSandbox.
func WithIdleTimeout(d time.Duration) SandboxOption {
	return func(c *sandboxConfig) { c.idleTimeout = d }
}

// WithSnapshot starts the sandbox from a previously taken snapshot instead of
// from scratch. The snapshot must have been obtained from Sandbox.Snapshot.
func WithSnapshot(s *ProviderSnapshot) SandboxOption {
	return func(c *sandboxConfig) { c.snapshot = s }
}

// sandboxBase holds the sandboxID and activity method receiver shared by both
// defaultSandbox (owning) and sandboxRef (non-owning). All Sandbox interface methods
// are implemented here and promoted to both concrete types via embedding.
// defaultSandbox additionally implements OwnedSandbox.Stop.
type sandboxBase struct {
	sandboxID  string
	activities *sandboxActivities
}

func (b *sandboxBase) ExecuteCommand(ctx workflow.Context, cmd string, opts ...ExecuteCommandOption) (*compute.CommandResult, error) {
	cfg := &executeCommandConfig{}
	for _, o := range opts {
		o(cfg)
	}
	updateID, err := sideEffectUUID(ctx)
	if err != nil {
		return nil, err
	}
	var result compute.CommandResult
	err = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 20 * time.Minute}),
		b.activities.SendSandboxExecuteCommand,
		SendSandboxExecuteCommandInput{UpdateID: updateID, SandboxID: b.sandboxID, Command: cmd, DisableAutoResume: cfg.disableAutoResume},
	).Get(ctx, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (b *sandboxBase) Suspend(ctx workflow.Context) error {
	updateID, err := sideEffectUUID(ctx)
	if err != nil {
		return err
	}
	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Minute}),
		b.activities.SendSandboxSuspend, SendSandboxSuspendInput{UpdateID: updateID, SandboxID: b.sandboxID},
	).Get(ctx, nil)
}

func (b *sandboxBase) Resume(ctx workflow.Context) error {
	updateID, err := sideEffectUUID(ctx)
	if err != nil {
		return err
	}
	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Minute}),
		b.activities.SendSandboxResume, SendSandboxResumeInput{UpdateID: updateID, SandboxID: b.sandboxID},
	).Get(ctx, nil)
}

func (b *sandboxBase) Snapshot(ctx workflow.Context) (*ProviderSnapshot, error) {
	updateID, err := sideEffectUUID(ctx)
	if err != nil {
		return nil, err
	}
	var snapshot ProviderSnapshot
	err = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Minute}),
		b.activities.SendSandboxSnapshot, SendSandboxSnapshotInput{UpdateID: updateID, SandboxID: b.sandboxID},
	).Get(ctx, &snapshot)
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (b *sandboxBase) DeleteSnapshot(ctx workflow.Context, snapshot *ProviderSnapshot) error {
	updateID, err := sideEffectUUID(ctx)
	if err != nil {
		return err
	}
	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Minute}),
		b.activities.SendSandboxDeleteSnapshot, SendSandboxDeleteSnapshotInput{UpdateID: updateID, SandboxID: b.sandboxID, Snapshot: snapshot},
	).Get(ctx, nil)
}

func (b *sandboxBase) RequestStop(ctx workflow.Context) error {
	signalErr := workflow.SignalExternalWorkflow(
		ctx, b.sandboxID, "",
		wfIface.SandboxStopSignal, nil,
	).Get(ctx, nil)
	var notFound *serviceerror.NotFound
	if signalErr != nil && !errors.As(signalErr, &notFound) {
		return signalErr
	}
	return nil
}

func (b *sandboxBase) Ref() (string, error) { return encodeRef(b.sandboxID) }

type defaultSandbox struct {
	sandboxBase
	future  workflow.ChildWorkflowFuture
	stopped bool
}

// NewSandbox starts a sandbox as a child workflow, using a generated UUID as
// the workflow ID, then blocks until the sandbox has been initialised with the
// given compute provider. Use WithIdleTimeout, WithCleanup, and WithSnapshot to
// configure idle suspension, parent-close behaviour, and snapshot-based starts.
//
// The caller must call Stop (or RequestStop) when the sandbox is no longer
// needed. If WithCleanup is not set, the default policy cancels the sandbox
// workflow when the parent closes, so Stop is only strictly required when
// CleanupDisabled is in use.
func NewSandbox(ctx workflow.Context, sandboxProvider Provider, opts ...SandboxOption) (OwnedSandbox, error) {
	cfg := &sandboxConfig{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.idleTimeout < 0 && cfg.idleTimeout != NoIdleTimeout {
		return nil, fmt.Errorf("sandbox: WithIdleTimeout: duration must be non-negative or NoIdleTimeout, got %v", cfg.idleTimeout)
	}

	encodedSandboxId := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} { return uuid.New().String() })
	var sandboxID string
	if err := encodedSandboxId.Get(&sandboxID); err != nil {
		return nil, fmt.Errorf("sandbox: get side-effect sandbox ID: %w", err)
	}
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox: side-effect produced empty sandbox ID")
	}

	s := &defaultSandbox{
		sandboxBase: sandboxBase{sandboxID: sandboxID, activities: &sandboxActivities{}},
	}

	if err := s.start(ctx, sandboxProvider, cfg); err != nil {
		return nil, err
	}

	return s, nil
}

// start launches the sandbox workflow using sandboxId as its workflow ID,
// waits for the child workflow execution to be accepted by the Temporal server,
// then sends an init update so the sandbox knows what compute provider to use.
func (s *defaultSandbox) start(ctx workflow.Context, computeProvider compute.ProviderDetails, cfg *sandboxConfig) error {
	parentClosePolicy := enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL
	if cfg.cleanup == CleanupDisabled {
		parentClosePolicy = enumspb.PARENT_CLOSE_POLICY_ABANDON
	}

	cwo := workflow.ChildWorkflowOptions{
		WorkflowID:        s.sandboxID,
		ParentClosePolicy: parentClosePolicy,
	}
	info := workflow.GetInfo(ctx)
	s.future = workflow.ExecuteChildWorkflow(
		workflow.WithChildOptions(ctx, cwo),
		wfIface.SandboxWorkflowType,
		wfIface.SandboxLocalState{WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID},
	)

	// Wait for the sandbox workflow to be scheduled and running before
	// sending the init update so the update is not lost.
	if err := s.future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return err
	}

	// Send an init update to the running sandbox workflow. Using an activity
	// to call client.UpdateWorkflow ensures the parent blocks until the
	// sandbox's update handler has completed (request-response semantics).
	updateID, err := sideEffectUUID(ctx)
	if err != nil {
		return err
	}
	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Minute,
		}),
		s.activities.SendSandboxInit,
		SendSandboxInitInput{UpdateID: updateID, SandboxID: s.sandboxID, ComputeProvider: computeProvider, IdleTimeout: cfg.idleTimeout, Snapshot: cfg.snapshot},
	).Get(ctx, nil)
}

func (s *defaultSandbox) Stop(ctx workflow.Context) error {
	if s.stopped {
		return nil
	}
	s.stopped = true
	signalErr := workflow.SignalExternalWorkflow(
		ctx, s.sandboxID, "",
		wfIface.SandboxStopSignal, nil,
	).Get(ctx, nil)
	var notFound *serviceerror.NotFound
	if errors.As(signalErr, &notFound) {
		// Sandbox workflow is already gone; nothing to wait for.
		return nil
	}
	if signalErr != nil {
		return signalErr
	}
	return s.future.Get(ctx, nil)
}

// AttachToSandbox returns a Sandbox that routes work to an existing sandbox identified by
// ref (an opaque reference obtained from Sandbox.Ref). The returned value implements
// Sandbox but not OwnedSandbox: use RequestStop for fire-and-forget shutdown signalling.
// There is no mechanism to block until the sandbox workflow fully exits from a non-owning
// reference; lifecycle management belongs to the workflow that originally called NewSandbox.
func AttachToSandbox(ref string) (Sandbox, error) {
	data, err := decodeRef(ref)
	if err != nil {
		return nil, err
	}
	return &sandboxRef{sandboxBase: sandboxBase{sandboxID: data.SandboxID, activities: &sandboxActivities{}}}, nil
}

type sandboxRef struct {
	sandboxBase
}
