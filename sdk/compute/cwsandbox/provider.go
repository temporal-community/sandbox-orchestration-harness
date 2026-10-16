// Package cwsandbox implements compute.Provider for CoreWeave Sandboxes.
// See https://docs.coreweave.com/products/sandboxes.
package cwsandbox

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"buf.build/gen/go/coreweave/sandbox/connectrpc/go/coreweave/sandbox/v1/sandboxv1connect"
	sandboxv1 "buf.build/gen/go/coreweave/sandbox/protocolbuffers/go/coreweave/sandbox/v1"
	"connectrpc.com/connect"
	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"google.golang.org/protobuf/proto"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

const (
	defaultBaseURL   = "https://api.cwsandbox.com"
	taskQueueEnvKey  = "TEMPORAL_TASK_QUEUE"
	workspaceVolume  = "workspace"
	operationTimeout = 5 * time.Minute
)

func init() {
	compute.Register(compute.ProviderTypeCoreWeaveSandbox, newCoreWeaveSandboxProvider)
}

type cwSandboxProvider struct {
	client       sandboxv1connect.SandboxServiceClient
	spec         *sandboxv1.SandboxSpec
	token        string
	pollInterval time.Duration
}

var _ compute.Provider = (*cwSandboxProvider)(nil)

func newCoreWeaveSandboxProvider(config map[string]string) (compute.Provider, error) {
	spec, token, baseURL, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	return &cwSandboxProvider{
		client: sandboxv1connect.NewSandboxServiceClient(http.DefaultClient, baseURL),
		spec:   spec, token: token, pollInterval: time.Second,
	}, nil
}

func newAuthedRequest[T any](token string, msg *T) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	return req
}

// Reuse the same token across Temporal retries, but not across distinct starts
// or snapshots. A provider can also be called outside a Temporal activity.
func requestID(ctx context.Context) string {
	if activity.IsActivity(ctx) {
		info := activity.GetInfo(ctx)
		seed := fmt.Sprintf("%q/%q/%q/%q", info.WorkflowNamespace, info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.ActivityID)
		return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)).String()
	}
	return uuid.NewString()
}

func (p *cwSandboxProvider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	return p.start(ctx, taskQueueName, "")
}

func (p *cwSandboxProvider) start(ctx context.Context, taskQueueName, snapshotID string) (*compute.ProviderStatus, error) {
	if taskQueueName == "" {
		return nil, fmt.Errorf("cwsandbox: task queue required")
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	spec := proto.Clone(p.spec).(*sandboxv1.SandboxSpec)
	spec.Containers[0].EnvironmentVariables = map[string]string{taskQueueEnvKey: taskQueueName}
	if snapshotID != "" {
		spec.Volumes[0].GetScratch().RestoreFromSnapshotId = snapshotID
	}
	resp, err := p.client.CreateSandbox(ctx, newAuthedRequest(p.token, &sandboxv1.CreateSandboxRequest{
		Sandbox: &sandboxv1.Sandbox{Spec: spec}, RequestId: requestID(ctx),
	}))
	if err != nil {
		return nil, fmt.Errorf("cwsandbox: CreateSandbox: %w", err)
	}
	sb := resp.Msg
	id := sb.GetSandboxId()
	if id == "" {
		return nil, fmt.Errorf("cwsandbox: CreateSandbox returned no sandbox ID")
	}
	for {
		switch sb.GetStatus().GetState() {
		case sandboxv1.State_STATE_RUNNING:
			return &compute.ProviderStatus{InstanceID: id}, nil
		case sandboxv1.State_STATE_CREATING, sandboxv1.State_STATE_PREPARING:
			// Creation is asynchronous; exec is only available after RUNNING.
		default:
			return nil, fmt.Errorf("cwsandbox: sandbox %s entered %s: %s", id, sb.GetStatus().GetState(), sb.GetStatus().GetStateReason())
		}
		if err := p.wait(ctx); err != nil {
			return nil, fmt.Errorf("cwsandbox: waiting for sandbox %s: %w", id, err)
		}
		resp, err = p.client.GetSandbox(ctx, newAuthedRequest(p.token, &sandboxv1.GetSandboxRequest{SandboxId: id}))
		if err != nil {
			return nil, fmt.Errorf("cwsandbox: GetSandbox %s: %w", id, err)
		}
		sb = resp.Msg
	}
}

func (p *cwSandboxProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	_, err := p.client.DeleteSandbox(ctx, newAuthedRequest(p.token, &sandboxv1.DeleteSandboxRequest{SandboxId: status.InstanceID}))
	if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
		return fmt.Errorf("cwsandbox: DeleteSandbox: %w", err)
	}
	return nil
}

// v1 has no in-place pause/resume. The harness handles suspension through
// Snapshot, Stop, and StartFromSnapshot when a workspace is configured.
func (p *cwSandboxProvider) Suspend(context.Context, *compute.ProviderStatus) error {
	return fmt.Errorf("cwsandbox: Suspend: %w", errors.ErrUnsupported)
}

func (p *cwSandboxProvider) Resume(context.Context, *compute.ProviderStatus) error {
	return fmt.Errorf("cwsandbox: Resume: %w", errors.ErrUnsupported)
}

func (p *cwSandboxProvider) Snapshot(ctx context.Context, status *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	snapshot, err := p.snapshot(ctx, status)
	// Filesystem snapshots leave the source running, including on failure.
	return compute.SandboxPostSnapshotRunning, snapshot, err
}

func (p *cwSandboxProvider) snapshot(ctx context.Context, status *compute.ProviderStatus) (*compute.ProviderSnapshot, error) {
	if len(p.spec.Volumes) == 0 {
		return nil, fmt.Errorf("cwsandbox: Snapshot requires workspace-path: %w", errors.ErrUnsupported)
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	resp, err := p.client.CreateFileSystemSnapshot(ctx, newAuthedRequest(p.token, &sandboxv1.CreateFileSystemSnapshotRequest{
		SandboxId: status.InstanceID, ScratchVolumeName: workspaceVolume, RequestId: requestID(ctx),
	}))
	if err != nil {
		return nil, fmt.Errorf("cwsandbox: CreateFileSystemSnapshot: %w", err)
	}
	snapshot := resp.Msg
	id := snapshot.GetFileSystemSnapshotId()
	if id == "" {
		return nil, fmt.Errorf("cwsandbox: CreateFileSystemSnapshot returned no snapshot ID")
	}
	for {
		switch snapshot.GetState() {
		case sandboxv1.SnapshotState_SNAPSHOT_STATE_READY:
			return &compute.ProviderSnapshot{SnapshotID: id}, nil
		case sandboxv1.SnapshotState_SNAPSHOT_STATE_CREATING:
		default:
			return nil, fmt.Errorf("cwsandbox: snapshot %s entered %s: %s", id, snapshot.GetState(), snapshot.GetStateReason())
		}
		if err := p.wait(ctx); err != nil {
			return nil, fmt.Errorf("cwsandbox: waiting for snapshot %s: %w", id, err)
		}
		resp, err = p.client.GetFileSystemSnapshot(ctx, newAuthedRequest(p.token, &sandboxv1.GetFileSystemSnapshotRequest{FileSystemSnapshotId: id}))
		if err != nil {
			return nil, fmt.Errorf("cwsandbox: GetFileSystemSnapshot %s: %w", id, err)
		}
		snapshot = resp.Msg
	}
}

func (p *cwSandboxProvider) StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	if len(p.spec.Volumes) == 0 {
		return nil, fmt.Errorf("cwsandbox: StartFromSnapshot requires workspace-path: %w", errors.ErrUnsupported)
	}
	if snapshot == nil || snapshot.SnapshotID == "" {
		return nil, fmt.Errorf("cwsandbox: snapshot ID required")
	}
	return p.start(ctx, taskQueueName, snapshot.SnapshotID)
}

func (p *cwSandboxProvider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) error {
	_, err := p.client.DeleteFileSystemSnapshot(ctx, newAuthedRequest(p.token, &sandboxv1.DeleteFileSystemSnapshotRequest{
		FileSystemSnapshotId: snapshot.SnapshotID, AllowMissing: true,
	}))
	if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
		return fmt.Errorf("cwsandbox: DeleteFileSystemSnapshot: %w", err)
	}
	return nil
}

func (p *cwSandboxProvider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	resp, err := p.client.Exec(ctx, newAuthedRequest(p.token, &sandboxv1.ExecRequest{
		SandboxId: status.InstanceID, Command: []string{"sh", "-c", cmd},
	}))
	if err != nil {
		return nil, fmt.Errorf("cwsandbox: Exec: %w", err)
	}
	result := resp.Msg
	if result.GetStdoutTruncated() || result.GetStderrTruncated() {
		return nil, fmt.Errorf("cwsandbox: Exec output truncated (stdout=%t, stderr=%t)", result.GetStdoutTruncated(), result.GetStderrTruncated())
	}
	return &compute.CommandResult{Stdout: string(result.GetStdout()), Stderr: string(result.GetStderr()), ExitCode: result.GetExitCode()}, nil
}

func (p *cwSandboxProvider) wait(ctx context.Context) error {
	timer := time.NewTimer(p.pollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
