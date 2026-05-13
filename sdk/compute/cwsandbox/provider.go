// Package cwsandbox implements compute.Provider for CoreWeave Sandboxes.
// See https://docs.coreweave.com/products/sandboxes.
package cwsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"buf.build/gen/go/coreweave/sandbox/connectrpc/go/coreweave/sandbox/v1beta2/sandboxv1beta2connect"
	sandboxv1beta2 "buf.build/gen/go/coreweave/sandbox/protocolbuffers/go/coreweave/sandbox/v1beta2"
	"connectrpc.com/connect"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func init() {
	compute.Register(compute.ProviderTypeCoreWeaveSandbox, newCoreWeaveSandboxProvider)
}

const (
	defaultBaseURL  = "https://api.cwsandbox.com"
	apiTokenEnvVar  = "CWSANDBOX_API_TOKEN"
	taskQueueEnvKey = "TEMPORAL_TASK_QUEUE"
)

type cwSandboxProvider struct {
	client             sandboxv1beta2connect.GatewayServiceClient
	containerImage     string
	command            []string
	args               []string
	profileNames       []string
	maxLifetimeSeconds int32
	token              string
}

func newCoreWeaveSandboxProvider(config map[string]string) (compute.Provider, error) {
	token := os.Getenv(apiTokenEnvVar)
	if token == "" {
		return nil, fmt.Errorf("cwsandbox: %s env var required", apiTokenEnvVar)
	}

	containerImage := config["container-image"]
	if containerImage == "" {
		return nil, fmt.Errorf("cwsandbox: container-image required")
	}

	rawCommand := config["command"]
	if rawCommand == "" {
		return nil, fmt.Errorf("cwsandbox: command required")
	}
	command := splitArgs(rawCommand)

	var args []string
	if rawArgs := config["args"]; rawArgs != "" {
		args = splitArgs(rawArgs)
	}

	var profileNames []string
	if raw := config["profile-names"]; raw != "" {
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				profileNames = append(profileNames, p)
			}
		}
	}

	var maxLifetime int32
	if raw := config["max-lifetime-seconds"]; raw != "" {
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cwsandbox: invalid max-lifetime-seconds: %w", err)
		}
		maxLifetime = int32(v)
	}

	baseURL := config["base-url"]
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	client := sandboxv1beta2connect.NewGatewayServiceClient(http.DefaultClient, baseURL)

	return &cwSandboxProvider{
		client:             client,
		containerImage:     containerImage,
		command:            command,
		args:               args,
		profileNames:       profileNames,
		maxLifetimeSeconds: maxLifetime,
		token:              token,
	}, nil
}

func newAuthedRequest[T any](token string, msg *T) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	return req
}

func (p *cwSandboxProvider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	req := newAuthedRequest(p.token, &sandboxv1beta2.StartSandboxRequest{
		ContainerImage: p.containerImage,
		Command:        p.command[0],
		Args:           append(append([]string{}, p.command[1:]...), p.args...),
		EnvironmentVariables: map[string]string{
			taskQueueEnvKey: taskQueueName,
		},
		ProfileNames:       p.profileNames,
		MaxLifetimeSeconds: p.maxLifetimeSeconds,
	})

	resp, err := p.client.Start(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cwsandbox: Start: %w", err)
	}
	return &compute.ProviderStatus{InstanceID: resp.Msg.GetSandboxId()}, nil
}

func (p *cwSandboxProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	req := newAuthedRequest(p.token, &sandboxv1beta2.DeleteSandboxRequest{
		SandboxId: status.InstanceID,
	})
	if _, err := p.client.Delete(ctx, req); err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("cwsandbox: Delete: %w", err)
	}
	return nil
}

func (p *cwSandboxProvider) Suspend(ctx context.Context, status *compute.ProviderStatus) error {
	req := newAuthedRequest(p.token, &sandboxv1beta2.PauseSandboxRequest{
		SandboxId: status.InstanceID,
	})
	if _, err := p.client.Pause(ctx, req); err != nil {
		return fmt.Errorf("cwsandbox: Pause: %w", err)
	}
	return nil
}

func (p *cwSandboxProvider) Resume(ctx context.Context, status *compute.ProviderStatus) error {
	req := newAuthedRequest(p.token, &sandboxv1beta2.ResumeSandboxRequest{
		SandboxId: status.InstanceID,
	})
	if _, err := p.client.Resume(ctx, req); err != nil {
		return fmt.Errorf("cwsandbox: Resume: %w", err)
	}
	return nil
}

func (p *cwSandboxProvider) Snapshot(_ context.Context, _ *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("cwsandbox: Snapshot: %w", errors.ErrUnsupported)
}

func (p *cwSandboxProvider) StartFromSnapshot(_ context.Context, _ string, _ *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	return nil, fmt.Errorf("cwsandbox: StartFromSnapshot: %w", errors.ErrUnsupported)
}

func (p *cwSandboxProvider) DeleteSnapshot(_ context.Context, _ *compute.ProviderSnapshot) error {
	return fmt.Errorf("cwsandbox: DeleteSnapshot: %w", errors.ErrUnsupported)
}

func (p *cwSandboxProvider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	req := newAuthedRequest(p.token, &sandboxv1beta2.ExecSandboxRequest{
		SandboxId: status.InstanceID,
		Command:   []string{"sh", "-c", cmd},
	})
	resp, err := p.client.Exec(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cwsandbox: Exec: %w", err)
	}
	result := resp.Msg.GetResult()
	return &compute.CommandResult{
		Stdout:   string(result.GetStdout()),
		Stderr:   string(result.GetStderr()),
		ExitCode: result.GetExitCode(),
	}, nil
}

func isNotFound(err error) bool {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connectErr.Code() == connect.CodeNotFound
	}
	return false
}

func splitArgs(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if strings.HasPrefix(raw, "[") {
		var parsed []string
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			return parsed
		}
	}
	return strings.Fields(raw)
}
