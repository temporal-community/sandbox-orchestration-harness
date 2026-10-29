// Package blaxel provides a Blaxel compute provider for the Temporal sandbox
// orchestration harness. Credentials stay on the activity worker in BL_API_KEY
// and BL_WORKSPACE and are never placed in workflow history.
package blaxel

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	blaxelsdk "github.com/blaxel-ai/sdk-go"
	"github.com/blaxel-ai/sdk-go/option"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

const providerType = compute.ProviderTypeBlaxel

func init() { compute.Register(providerType, New) }

// New constructs the provider. Every config value is optional: Blaxel picks the
// image (blaxel/base-image:latest), memory and region when they are not set.
// The region falls back to BL_REGION on the worker, as in Blaxel's SDKs.
func New(config map[string]string) (compute.Provider, error) {
	if workspace := blaxelsdk.GetDefaultWorkspace(); workspace == "" || !hasCredentials(workspace) {
		return nil, fmt.Errorf("blaxel: set BL_WORKSPACE and BL_API_KEY on the activity worker")
	}
	p := &provider{
		image:  strings.TrimSpace(config["image"]),
		region: cmp.Or(strings.TrimSpace(config["region"]), os.Getenv("BL_REGION")),
	}
	if config["memory-mb"] != "" {
		v, err := strconv.Atoi(config["memory-mb"])
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("blaxel: memory-mb must be a positive integer")
		}
		p.memoryMB = v
	}
	client, err := blaxelsdk.NewDefaultClient()
	if err != nil {
		return nil, fmt.Errorf("blaxel: create client: %w", err)
	}
	p.client = client
	return p, nil
}

type provider struct {
	client        blaxelsdk.Client
	image, region string
	memoryMB      int
}

var _ compute.Provider = (*provider)(nil)

// noRetry leaves repeating snapshot and fork requests to Temporal's activity
// retry, as the SDK already does for POST /process. A snapshot request repeated
// after a lost response leaves one more snapshot behind each time, and a
// repeated fork only waits out the conflict the provider handles itself.
var noRetry = option.WithMaxRetries(0)

// truncated starts an output stream the sandbox has cut down to its tail.
const truncated = "[... truncated, see the process log file for the full output ...]"

// String prevents the activity worker's credentials from appearing in logs.
func (p *provider) String() string { return "blaxel provider" }

func (p *provider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	name, err := sandboxName(taskQueueName, "")
	if err != nil {
		return nil, err
	}
	runtime := blaxelsdk.SandboxRuntimeParam{Envs: taskQueueEnv(taskQueueName)}
	if p.image != "" {
		runtime.Image = blaxelsdk.String(p.image)
	}
	if p.memoryMB != 0 {
		runtime.Memory = blaxelsdk.Int(int64(p.memoryMB))
	}
	spec := blaxelsdk.SandboxSpecParam{Runtime: runtime}
	if p.region != "" {
		spec.Region = blaxelsdk.String(p.region)
	}
	// createIfNotExist returns the sandbox an earlier attempt of this activity
	// created instead of failing with a conflict.
	sandbox, err := p.client.Sandboxes.New(ctx, blaxelsdk.SandboxNewParams{
		CreateIfNotExist: blaxelsdk.Bool(true),
		Sandbox: blaxelsdk.SandboxParam{
			Metadata: blaxelsdk.MetadataParam{Name: name, Labels: map[string]string{"temporal-harness": "blaxel"}},
			Spec:     spec,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("blaxel: create sandbox: %w", err)
	}
	return p.waitDeployed(ctx, sandbox)
}

func (p *provider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	name, err := instanceName(status)
	if err != nil {
		return err
	}
	if _, err := p.client.Sandboxes.Delete(ctx, name); err != nil && !isStatus(err, http.StatusNotFound) {
		return fmt.Errorf("blaxel: delete sandbox: %w", err)
	}
	return nil
}

// Suspend is a no-op: Blaxel puts a sandbox on standby by itself about 15 to 30
// seconds after its last request, keeping memory and filesystem.
func (p *provider) Suspend(context.Context, *compute.ProviderStatus) error { return nil }

// Resume is a no-op: the next request to a sandbox on standby resumes it.
func (p *provider) Resume(context.Context, *compute.ProviderStatus) error { return nil }

func (p *provider) Snapshot(ctx context.Context, status *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	name, err := instanceName(status)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}
	snapshot, err := p.client.Snapshots.New(ctx, blaxelsdk.SnapshotNewParams{
		SandboxSnapshotRequest: blaxelsdk.SandboxSnapshotRequestParam{Source: blaxelsdk.SandboxSnapshotSourceParam{Name: name}},
	}, noRetry)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, snapshotError("create snapshot", err)
	}
	switch {
	case snapshot.ID == "":
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("blaxel: snapshot of %s returned no id", name)
	case snapshot.Status == "failed":
		// A failed snapshot stays in the workspace until it is deleted.
		err := fmt.Errorf("blaxel: snapshot of %s failed", name)
		return compute.SandboxPostSnapshotRunning, nil, errors.Join(err, p.DeleteSnapshot(ctx, &compute.ProviderSnapshot{SnapshotID: snapshot.ID}))
	}
	return compute.SandboxPostSnapshotRunning, &compute.ProviderSnapshot{SnapshotID: snapshot.ID}, nil
}

func (p *provider) StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	if snapshot == nil || snapshot.SnapshotID == "" {
		return nil, fmt.Errorf("blaxel: snapshot id is required")
	}
	name, err := sandboxName(taskQueueName, snapshot.SnapshotID)
	if err != nil {
		return nil, err
	}
	_, err = p.client.Snapshots.Fork(ctx, snapshot.SnapshotID, blaxelsdk.SnapshotForkParams{
		SandboxForkRequest: blaxelsdk.SandboxForkRequestParam{TargetName: name, TargetType: "sandbox", Envs: taskQueueEnv(taskQueueName)},
	}, noRetry)
	// A conflict means an earlier attempt of this activity already forked it.
	if err != nil && !isStatus(err, http.StatusConflict) {
		return nil, snapshotError("fork snapshot", err)
	}
	sandbox, err := p.client.Sandboxes.Get(ctx, name, blaxelsdk.SandboxGetParams{})
	if err != nil {
		return nil, fmt.Errorf("blaxel: get sandbox: %w", err)
	}
	return p.waitDeployed(ctx, sandbox)
}

func (p *provider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) error {
	if snapshot == nil || snapshot.SnapshotID == "" {
		return fmt.Errorf("blaxel: snapshot id is required")
	}
	if err := p.client.Snapshots.Delete(ctx, snapshot.SnapshotID); err != nil && !isStatus(err, http.StatusNotFound) {
		return snapshotError("delete snapshot", err)
	}
	return nil
}

func (p *provider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	name, err := instanceName(status)
	if err != nil {
		return nil, err
	}
	// keepAlive holds off standby while the command runs and kills it when the
	// activity's deadline passes.
	request := blaxelsdk.ProcessRequestParam{Command: cmd, KeepAlive: blaxelsdk.Bool(true)}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := int64(time.Until(deadline).Seconds())
		if remaining < 1 {
			return nil, context.DeadlineExceeded
		}
		request.Timeout = blaxelsdk.Int(remaining)
	}
	sandbox, err := p.client.Sandboxes.GetInstance(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("blaxel: get sandbox: %w", err)
	}
	// Start the command and poll it rather than holding one request open: the
	// gateway ends requests after two minutes, and process status responses
	// truncate output, so the output is read from the logs afterwards.
	process, err := sandbox.Process.New(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("blaxel: execute command: %w", err)
	}
	if process, err = sandbox.Process.Wait(ctx, process.Name, -1, 250*time.Millisecond); err != nil {
		return nil, fmt.Errorf("blaxel: wait for command: %w", err)
	}
	logs, err := sandbox.Process.GetLogs(ctx, process.Name)
	if err != nil {
		return nil, fmt.Errorf("blaxel: read command output: %w", err)
	}
	// The logs return a bounded tail of each stream: up to 4 MB, less once a
	// sandbox has run many commands. Partial output is not the command's output,
	// and a retry would only run the command into the same limit again.
	if strings.HasPrefix(logs.Stdout, truncated) || strings.HasPrefix(logs.Stderr, truncated) {
		return nil, fmt.Errorf("blaxel: command output truncated; refusing to report incomplete output: %w", errors.ErrUnsupported)
	}
	return &compute.CommandResult{Stdout: logs.Stdout, Stderr: logs.Stderr, ExitCode: int32(process.ExitCode)}, nil
}

// waitDeployed returns once the sandbox accepts commands.
func (p *provider) waitDeployed(ctx context.Context, sandbox *blaxelsdk.Sandbox) (*compute.ProviderStatus, error) {
	for sandbox.Status != blaxelsdk.StatusDeployed {
		switch sandbox.Status {
		case blaxelsdk.StatusFailed, blaxelsdk.StatusTerminated, blaxelsdk.StatusDeleting:
			return nil, fmt.Errorf("blaxel: sandbox %s is %s", sandbox.Metadata.Name, sandbox.Status)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
		var err error
		if sandbox, err = p.client.Sandboxes.Get(ctx, sandbox.Metadata.Name, blaxelsdk.SandboxGetParams{}); err != nil {
			return nil, fmt.Errorf("blaxel: get sandbox: %w", err)
		}
	}
	return &compute.ProviderStatus{InstanceID: sandbox.Metadata.Name}, nil
}

// Snapshots are in private preview and answer 403 in workspaces without them.
// Report that as unsupported so the workflow fails instead of retrying forever.
func snapshotError(op string, err error) error {
	if isStatus(err, http.StatusForbidden) {
		return fmt.Errorf("blaxel: %s: %w: %w", op, errors.ErrUnsupported, err)
	}
	return fmt.Errorf("blaxel: %s: %w", op, err)
}

// hasCredentials looks where NewDefaultClient does: the environment, then the
// workspace's entry in the bl CLI config.
func hasCredentials(workspace string) bool {
	if os.Getenv("BL_API_KEY") != "" || os.Getenv("BL_CLIENT_CREDENTIALS") != "" {
		return true
	}
	creds, err := blaxelsdk.LoadCredentials(workspace)
	return err == nil && creds.IsValid()
}

func isStatus(err error, status int) bool {
	var e *blaxelsdk.Error
	return errors.As(err, &e) && e.StatusCode == status
}
func taskQueueEnv(queue string) []blaxelsdk.EnvParam {
	return []blaxelsdk.EnvParam{{Name: blaxelsdk.String("TEMPORAL_TASK_QUEUE"), Value: blaxelsdk.String(queue)}}
}
func instanceName(status *compute.ProviderStatus) (string, error) {
	if status == nil || status.InstanceID == "" {
		return "", fmt.Errorf("blaxel: sandbox name is required")
	}
	return status.InstanceID, nil
}
func sandboxName(queue, snapshot string) (string, error) {
	if queue == "" {
		return "", fmt.Errorf("blaxel: task queue is required")
	}
	sum := sha256.Sum256([]byte(queue + "\x00" + snapshot))
	return "temporal-" + hex.EncodeToString(sum[:8]), nil
}
