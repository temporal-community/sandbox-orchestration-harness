// Package blaxel provides a Blaxel compute provider for the Temporal sandbox
// orchestration harness. Credentials stay on the activity worker in BL_API_KEY
// and BL_WORKSPACE and are never placed in workflow history.
package blaxel

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const providerType = compute.ProviderTypeBlaxel

func init() { compute.Register(providerType, New) }

// New constructs the provider. Every config value is optional: Blaxel picks the
// image (blaxel/base-image:latest), memory and region when they are not set.
// The region falls back to BL_REGION on the worker, as in Blaxel's SDKs.
func New(config map[string]string) (compute.Provider, error) {
	if workspace := blaxelsdk.GetDefaultWorkspace(); workspace == "" || !hasCredentials(workspace) {
		return nil, fmt.Errorf("blaxel: set BL_WORKSPACE and BL_API_KEY on the activity worker, or run `bl login <workspace>` as the worker's user")
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

// maxResult keeps a command's result, as Temporal's default converter encodes
// it, below Temporal's default 2 MiB payload limit. Temporal rejects a larger
// result and retries the activity forever.
const maxResult = 3 << 19 // 1.5 MiB

// String prevents the activity worker's credentials from appearing in logs.
func (p *provider) String() string { return "blaxel provider" }

func (p *provider) Start(ctx context.Context, taskQueueName string) (_ *compute.ProviderStatus, err error) {
	defer finalAuthError(&err)
	name, err := sandboxName(taskQueueName, "")
	if err != nil {
		return nil, err
	}
	runtime := blaxelsdk.SandboxRuntimeParam{Envs: sandboxEnv(taskQueueName)}
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

func (p *provider) Stop(ctx context.Context, status *compute.ProviderStatus) (err error) {
	defer finalAuthError(&err)
	name, err := instanceName(status)
	if err != nil {
		return err
	}
	if _, err := p.client.Sandboxes.Delete(ctx, name); err != nil && !isStatus(err, http.StatusNotFound) {
		return fmt.Errorf("blaxel: delete sandbox: %w", err)
	}
	return nil
}

// Suspend is a no-op: Blaxel puts a sandbox on standby by itself about 30
// seconds after its last request, keeping memory and filesystem.
func (p *provider) Suspend(context.Context, *compute.ProviderStatus) error { return nil }

// Resume is a no-op: the next request to a sandbox on standby resumes it.
func (p *provider) Resume(context.Context, *compute.ProviderStatus) error { return nil }

func (p *provider) Snapshot(ctx context.Context, status *compute.ProviderStatus) (_ compute.SandboxPostSnapshotState, _ *compute.ProviderSnapshot, err error) {
	defer finalAuthError(&err)
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

func (p *provider) StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *compute.ProviderSnapshot) (_ *compute.ProviderStatus, err error) {
	defer finalAuthError(&err)
	if snapshot == nil || snapshot.SnapshotID == "" {
		return nil, fmt.Errorf("blaxel: snapshot id is required")
	}
	name, err := sandboxName(taskQueueName, snapshot.SnapshotID)
	if err != nil {
		return nil, err
	}
	_, err = p.client.Snapshots.Fork(ctx, snapshot.SnapshotID, blaxelsdk.SnapshotForkParams{
		SandboxForkRequest: blaxelsdk.SandboxForkRequestParam{TargetName: name, TargetType: "sandbox", Envs: sandboxEnv(taskQueueName)},
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

func (p *provider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) (err error) {
	defer finalAuthError(&err)
	if snapshot == nil || snapshot.SnapshotID == "" {
		return fmt.Errorf("blaxel: snapshot id is required")
	}
	if err := p.client.Snapshots.Delete(ctx, snapshot.SnapshotID); err != nil && !isStatus(err, http.StatusNotFound) {
		return snapshotError("delete snapshot", err)
	}
	return nil
}

func (p *provider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (_ *compute.CommandResult, err error) {
	defer finalAuthError(&err)
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
	command, err := startOnce(ctx, sandbox.Process, request)
	if err != nil {
		return nil, fmt.Errorf("blaxel: execute command: %w", err)
	}
	// Wait retries transient errors itself until the activity's deadline.
	process, err := sandbox.Process.Wait(ctx, command, -1, 250*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("blaxel: wait for command: %w", err)
	}
	if process.Status == blaxelsdk.ProcessResponseStatusKilled || process.Status == blaxelsdk.ProcessResponseStatusStopped {
		// Usually the activity's timeout. Retrying would find the same process.
		message := fmt.Sprintf("blaxel: command was %s before it finished, at the activity's timeout or by request", process.Status)
		return nil, temporal.NewNonRetryableApplicationError(message, "CommandKilled", nil)
	}
	logs, err := sandbox.Process.GetLogs(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("blaxel: read command output: %w", err)
	}
	// The logs return a bounded tail of each stream: up to 4 MB, less once a
	// sandbox has run many commands. Partial output is not the command's output,
	// and a retry would only find the same output again.
	if strings.HasPrefix(logs.Stdout, truncated) || strings.HasPrefix(logs.Stderr, truncated) {
		return nil, fmt.Errorf("blaxel: command output truncated; refusing to report incomplete output: %w", errors.ErrUnsupported)
	}
	result := &compute.CommandResult{Stdout: logs.Stdout, Stderr: logs.Stderr, ExitCode: int32(process.ExitCode)}
	// JSON escapes control characters, invalid UTF-8 and <, > and & to six bytes
	// each, so measure the result as Temporal will record it.
	if encoded, _ := json.Marshal(result); len(encoded) > maxResult {
		return nil, fmt.Errorf("blaxel: command output is %d bytes as an activity result, over the %d-byte limit: %w", len(encoded), maxResult, errors.ErrUnsupported)
	}
	return result, nil
}

// startOnce starts the command once per activity and returns its process name.
// Every attempt of an activity names its process alike, so an attempt that
// follows a lost response, a failed poll or a worker restart waits for the
// command an earlier attempt started rather than run it again. Outside an
// activity the sandbox names the process.
func startOnce(ctx context.Context, processes *blaxelsdk.SandboxInstanceProcessService, request blaxelsdk.ProcessRequestParam) (string, error) {
	if !activity.IsActivity(ctx) {
		process, err := processes.New(ctx, request)
		if err != nil {
			return "", err
		}
		return process.Name, nil
	}
	info := activity.GetInfo(ctx)
	sum := sha256.Sum256([]byte(strings.Join([]string{info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.ActivityID, info.ActivityRunID}, "\x00")))
	name := "temporal-" + hex.EncodeToString(sum[:8])
	request.Name = blaxelsdk.String(name)
	for delay := time.Second; ; delay = min(2*delay, 15*time.Second) {
		// The sandbox would start a second process under the name of one that
		// has exited, so look for it first.
		if _, err := processes.Get(ctx, name); !isStatus(err, http.StatusNotFound) {
			return name, err
		}
		_, err := processes.New(ctx, request)
		if err == nil || !transient(err) {
			return name, err
		}
		// The command may have started before its response was lost.
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(delay):
		}
	}
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
	var e *blaxelsdk.Error
	if errors.As(err, &e) && e.StatusCode == http.StatusForbidden && strings.Contains(e.RawJSON(), "feature is not enabled") {
		return fmt.Errorf("blaxel: %s: %w: %w", op, errors.ErrUnsupported, err)
	}
	return fmt.Errorf("blaxel: %s: %w", op, err)
}

// finalAuthError makes a rejected API key or workspace final: retrying it only
// hangs the workflow.
func finalAuthError(err *error) {
	if *err != nil && !errors.Is(*err, errors.ErrUnsupported) && (isStatus(*err, http.StatusUnauthorized) || isStatus(*err, http.StatusForbidden)) {
		*err = temporal.NewNonRetryableApplicationError((*err).Error(), "Unauthorized", *err)
	}
}

// transient reports whether a request may succeed if sent again.
func transient(err error) bool {
	var e *blaxelsdk.Error
	if errors.As(err, &e) {
		return e.StatusCode == http.StatusRequestTimeout || e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
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

// sandboxEnv gives a sandbox its task queue and stops it from copying each line
// of a command's output into Blaxel's logs as it reads it. That takes about 2 ms
// a line, and the command is not done until it has: 60,000 short lines take 100
// seconds with it and under one without. The output still comes back in full.
func sandboxEnv(queue string) []blaxelsdk.EnvParam {
	return []blaxelsdk.EnvParam{
		{Name: blaxelsdk.String("TEMPORAL_TASK_QUEUE"), Value: blaxelsdk.String(queue)},
		{Name: blaxelsdk.String("SANDBOX_DISABLE_PROCESS_LOGGING"), Value: blaxelsdk.String("true")},
	}
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
