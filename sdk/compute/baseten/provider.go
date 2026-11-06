// Package baseten provides a Baseten Sandboxes compute provider for the
// Temporal sandbox orchestration harness. The API key stays on the activity
// worker in BASETEN_API_KEY and is never placed in workflow history.
package baseten

import (
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

	"github.com/basetenlabs/baseten-go/sandbox"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const providerType = compute.ProviderTypeBaseten

func init() { compute.Register(providerType, New) }

// New constructs the provider. Every config value is optional: Baseten picks
// the image (baseten/base-image:latest), memory and region when they are not
// set, and the caller's only team when team-id is not set.
func New(config map[string]string) (compute.Provider, error) {
	return newProvider(config, "")
}

// newProvider takes the API's base URL so tests can point it at a fake.
func newProvider(config map[string]string, baseURL string) (compute.Provider, error) {
	apiKey := os.Getenv("BASETEN_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("baseten: set BASETEN_API_KEY on the activity worker")
	}
	p := &provider{
		image:  strings.TrimSpace(config["image"]),
		region: strings.TrimSpace(config["region"]),
	}
	if config["memory-mb"] != "" {
		v, err := strconv.Atoi(config["memory-mb"])
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("baseten: memory-mb must be a positive integer")
		}
		p.memoryMB = v
	}
	client, err := sandbox.NewClient(sandbox.ClientOptions{
		APIKey:  apiKey,
		TeamID:  strings.TrimSpace(config["team-id"]),
		BaseURL: baseURL,
	})
	if err != nil {
		return nil, fmt.Errorf("baseten: create client: %w", err)
	}
	p.client = client
	return p, nil
}

type provider struct {
	client        *sandbox.Client
	image, region string
	memoryMB      int
}

var _ compute.Provider = (*provider)(nil)

// truncated starts an output stream the sandbox has cut down to its tail.
const truncated = "[... truncated, see the process log file for the full output ...]"

// maxResult keeps a command's result, as Temporal's default converter encodes
// it, below Temporal's default 2 MiB payload limit. Temporal rejects a larger
// result and retries the activity forever.
const maxResult = 3 << 19 // 1.5 MiB

// String prevents the activity worker's credentials from appearing in logs.
func (p *provider) String() string { return "baseten provider" }

func (p *provider) Start(ctx context.Context, taskQueueName string) (_ *compute.ProviderStatus, err error) {
	defer finalAuthError(&err)
	if taskQueueName == "" {
		return nil, fmt.Errorf("baseten: task queue is required")
	}
	name := sandboxName(taskQueueName)
	// CreateIfNotExists returns the sandbox an earlier attempt of this
	// activity created instead of failing with a conflict.
	created, err := p.client.Create(ctx, sandbox.CreateOptions{
		Name:              name,
		CreateIfNotExists: true,
		Image:             p.image,
		Memory:            p.memoryMB,
		Region:            p.region,
		Envs:              sandboxEnv(taskQueueName),
		Labels:            map[string]string{"temporal-harness": "baseten"},
	})
	if err != nil {
		return nil, fmt.Errorf("baseten: create sandbox: %w", err)
	}
	return p.waitDeployed(ctx, &created.Info)
}

func (p *provider) Stop(ctx context.Context, status *compute.ProviderStatus) (err error) {
	defer finalAuthError(&err)
	name, err := instanceName(status)
	if err != nil {
		return err
	}
	if _, err := p.client.Delete(ctx, sandbox.DeleteOptions{Name: name}); err != nil && !isStatus(err, http.StatusNotFound) {
		return fmt.Errorf("baseten: delete sandbox: %w", err)
	}
	return nil
}

// Suspend is a no-op: Baseten puts a sandbox that stops receiving requests on
// standby by itself, keeping its files and processes.
func (p *provider) Suspend(context.Context, *compute.ProviderStatus) error { return nil }

// Resume is a no-op: the next request to a sandbox on standby resumes it.
func (p *provider) Resume(context.Context, *compute.ProviderStatus) error { return nil }

// Snapshot is unsupported: Baseten Sandboxes have no snapshots.
func (p *provider) Snapshot(context.Context, *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("baseten: snapshot: %w", errors.ErrUnsupported)
}

// StartFromSnapshot is unsupported: Baseten Sandboxes have no snapshots.
func (p *provider) StartFromSnapshot(context.Context, string, *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	return nil, fmt.Errorf("baseten: start from snapshot: %w", errors.ErrUnsupported)
}

// DeleteSnapshot is unsupported: Baseten Sandboxes have no snapshots.
func (p *provider) DeleteSnapshot(context.Context, *compute.ProviderSnapshot) error {
	return fmt.Errorf("baseten: delete snapshot: %w", errors.ErrUnsupported)
}

func (p *provider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (_ *compute.CommandResult, err error) {
	defer finalAuthError(&err)
	name, err := instanceName(status)
	if err != nil {
		return nil, err
	}
	// KeepAlive holds off standby while the command runs, and Timeout kills it
	// when the activity's deadline passes.
	request := sandbox.ProcessExecOptions{Command: cmd, KeepAlive: true}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline).Truncate(time.Second)
		if remaining < time.Second {
			return nil, context.DeadlineExceeded
		}
		request.Timeout = remaining
	}
	box, err := p.client.Get(ctx, sandbox.GetOptions{Name: name})
	if err != nil {
		return nil, fmt.Errorf("baseten: get sandbox: %w", err)
	}
	processes := box.Process()
	// Start the command and poll it rather than holding one request open: the
	// edge ends long requests, and process status responses truncate output,
	// so the output is read from the logs afterwards.
	command, err := startOnce(ctx, processes, request)
	if err != nil {
		return nil, fmt.Errorf("baseten: execute command: %w", err)
	}
	// Wait rides out transient errors itself until the activity's deadline.
	process, err := processes.Wait(ctx, sandbox.ProcessWaitOptions{Identifier: command, Timeout: -1, PollInterval: 250 * time.Millisecond})
	if err != nil {
		return nil, fmt.Errorf("baseten: wait for command: %w", err)
	}
	if process.Status == sandbox.ProcessStatusKilled || process.Status == sandbox.ProcessStatusStopped {
		// Usually the activity's timeout. Retrying would find the same process.
		message := fmt.Sprintf("baseten: command was %s before it finished, at the activity's timeout or by request", process.Status)
		return nil, temporal.NewNonRetryableApplicationError(message, "CommandKilled", nil)
	}
	logs, err := processes.Logs(ctx, sandbox.ProcessLogsOptions{Identifier: command})
	if err != nil {
		return nil, fmt.Errorf("baseten: read command output: %w", err)
	}
	// The logs return a bounded tail of each stream. Partial output is not the
	// command's output, and a retry would only find the same output again.
	if strings.HasPrefix(logs.Stdout, truncated) || strings.HasPrefix(logs.Stderr, truncated) {
		return nil, fmt.Errorf("baseten: command output truncated; refusing to report incomplete output: %w", errors.ErrUnsupported)
	}
	result := &compute.CommandResult{Stdout: logs.Stdout, Stderr: logs.Stderr, ExitCode: int32(process.ExitCode)}
	// JSON escapes control characters, invalid UTF-8 and <, > and & to six bytes
	// each, so measure the result as Temporal will record it.
	if encoded, _ := json.Marshal(result); len(encoded) > maxResult {
		return nil, fmt.Errorf("baseten: command output is %d bytes as an activity result, over the %d-byte limit: %w", len(encoded), maxResult, errors.ErrUnsupported)
	}
	return result, nil
}

// startOnce starts the command once per activity and returns its process name.
// Every attempt of an activity names its process alike, so an attempt that
// follows a lost response, a failed poll or a worker restart waits for the
// command an earlier attempt started rather than run it again. Outside an
// activity the sandbox names the process.
func startOnce(ctx context.Context, processes *sandbox.ProcessService, request sandbox.ProcessExecOptions) (string, error) {
	if !activity.IsActivity(ctx) {
		process, err := processes.Exec(ctx, request)
		if err != nil {
			return "", err
		}
		return process.Name, nil
	}
	info := activity.GetInfo(ctx)
	sum := sha256.Sum256([]byte(strings.Join([]string{info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.ActivityID, info.ActivityRunID}, "\x00")))
	request.Name = "temporal-" + hex.EncodeToString(sum[:8])
	for delay := time.Second; ; delay = min(2*delay, 15*time.Second) {
		// The sandbox would start a second process under the name of one that
		// has exited, so look for it first.
		if _, err := processes.Get(ctx, sandbox.ProcessGetOptions{Identifier: request.Name}); !isStatus(err, http.StatusNotFound) {
			return request.Name, err
		}
		// Exec sends the request once: the command may have side effects even
		// when its response is lost.
		_, err := processes.Exec(ctx, request)
		if err == nil || !transient(err) {
			return request.Name, err
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
func (p *provider) waitDeployed(ctx context.Context, info *sandbox.Info) (*compute.ProviderStatus, error) {
	for info.Status != sandbox.StatusDeployed {
		switch info.Status {
		case sandbox.StatusFailed, sandbox.StatusTerminated, sandbox.StatusDeleting:
			return nil, fmt.Errorf("baseten: sandbox %s is %s", info.Name, info.Status)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
		var err error
		if info, err = p.client.GetInfo(ctx, sandbox.GetInfoOptions{Name: info.Name}); err != nil {
			return nil, fmt.Errorf("baseten: get sandbox: %w", err)
		}
	}
	return &compute.ProviderStatus{InstanceID: info.Name}, nil
}

// finalAuthError makes a rejected API key final: retrying it only hangs the
// workflow.
func finalAuthError(err *error) {
	if *err != nil && (isStatus(*err, http.StatusUnauthorized) || isStatus(*err, http.StatusForbidden)) {
		*err = temporal.NewNonRetryableApplicationError((*err).Error(), "Unauthorized", *err)
	}
}

// transient reports whether a request may succeed if sent again.
func transient(err error) bool {
	var e *sandbox.APIError
	if errors.As(err, &e) {
		return e.Status == http.StatusRequestTimeout || e.Status == http.StatusTooManyRequests || e.Status >= 500
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func isStatus(err error, status int) bool {
	var e *sandbox.APIError
	return errors.As(err, &e) && e.Status == status
}

// sandboxEnv gives a sandbox its task queue and stops it from copying each line
// of a command's output into the sandbox's logs as it reads it. That takes
// about 2 ms a line, and the command is not done until it has. The output
// still comes back in full.
func sandboxEnv(queue string) map[string]sandbox.EnvValue {
	return map[string]sandbox.EnvValue{
		"TEMPORAL_TASK_QUEUE":             {Value: queue, NonSecret: true},
		"SANDBOX_DISABLE_PROCESS_LOGGING": {Value: "true", NonSecret: true},
	}
}

func instanceName(status *compute.ProviderStatus) (string, error) {
	if status == nil || status.InstanceID == "" {
		return "", fmt.Errorf("baseten: sandbox name is required")
	}
	return status.InstanceID, nil
}

func sandboxName(queue string) string {
	sum := sha256.Sum256([]byte(queue))
	return "temporal-" + hex.EncodeToString(sum[:8])
}
