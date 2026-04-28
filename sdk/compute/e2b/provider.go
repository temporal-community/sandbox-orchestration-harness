package e2b

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process/processconnect"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func init() {
	compute.Register(compute.ProviderTypeE2B, newE2BProvider)
}

const (
	e2bAPIBase = "https://api.e2b.app"
	e2bDomain  = "e2b.app"
)

type e2bProvider struct {
	apiKey     string
	templateID string
	timeout    int32
	httpClient *http.Client
}

func newE2BProvider(config map[string]string) (compute.Provider, error) {
	apiKey := os.Getenv("E2B_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("e2b: E2B_API_KEY env var required")
	}

	templateID := config["template-id"]
	if templateID == "" {
		return nil, fmt.Errorf("e2b: template-id required")
	}

	timeout := int32(3600)
	if t := config["timeout"]; t != "" {
		v, err := strconv.ParseInt(t, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("e2b: invalid timeout: %w", err)
		}
		timeout = int32(v)
	}

	return &e2bProvider{
		apiKey:     apiKey,
		templateID: templateID,
		timeout:    timeout,
		httpClient: &http.Client{},
	}, nil
}

type sandboxCreateRequestAutoResume struct {
	Enabled bool `json:"enabled"`
}

type sandboxCreateRequest struct {
	TemplateID string                         `json:"template_id"`
	EnvVars    map[string]string              `json:"env_vars,omitempty"`
	Timeout    int32                          `json:"timeout,omitempty"`
	AutoPause  bool                           `json:"auto_pause"`
	AutoResume sandboxCreateRequestAutoResume `json:"auto_resume,omitempty"`
}

type sandboxCreateResponse struct {
	SandboxID string `json:"sandbox_id"`
	ClientID  string `json:"client_id"`
}

type sandboxSnapshotResponse struct {
	SnapshotID string   `json:"snapshot_id"`
	Names      []string `json:"names,omitempty"`
}

func (p *e2bProvider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	body, err := json.Marshal(sandboxCreateRequest{
		TemplateID: p.templateID,
		EnvVars:    map[string]string{"TEMPORAL_TASK_QUEUE": taskQueueName},
		Timeout:    p.timeout,
		AutoPause:  false,
		AutoResume: sandboxCreateRequestAutoResume{Enabled: true},
	})
	if err != nil {
		return nil, fmt.Errorf("e2b: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e2bAPIBase+"/sandboxes", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("e2b: build request: %w", err)
	}
	req.Header.Set("X-API-Key", p.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("e2b: POST /sandboxes: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("e2b: POST /sandboxes returned %d - %s", resp.StatusCode, body)
	}

	var created sandboxCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, fmt.Errorf("e2b: decode response: %w", err)
	}

	return &compute.ProviderStatus{
		InstanceID: created.SandboxID + ":" + created.ClientID,
	}, nil
}

func (p *e2bProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	sandboxID, _, err := parseInstanceID(status.InstanceID)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, e2bAPIBase+"/sandboxes/"+sandboxID, nil)
	if err != nil {
		return fmt.Errorf("e2b: build delete request: %w", err)
	}
	req.Header.Set("X-API-Key", p.apiKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("e2b: DELETE /sandboxes/%s: %w", sandboxID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("e2b: DELETE /sandboxes/%s returned %d - %s", sandboxID, resp.StatusCode, body)
	}
	return nil
}

func (p *e2bProvider) Suspend(ctx context.Context, status *compute.ProviderStatus) error {
	sandboxID, _, err := parseInstanceID(status.InstanceID)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e2bAPIBase+"/sandboxes/"+sandboxID+"/pause", nil)
	if err != nil {
		return fmt.Errorf("e2b: build pause request: %w", err)
	}
	req.Header.Set("X-API-Key", p.apiKey)
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("e2b: POST /pause: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("e2b: POST /pause returned %d - %s", resp.StatusCode, body)
	}
	return nil
}

func (p *e2bProvider) Resume(ctx context.Context, status *compute.ProviderStatus) error {
	// E2B's resume API is deprecated as it will resume the sandbox automatically when needed
	// to simplify this, we'll make this a no-op
	return nil
}

func (p *e2bProvider) Snapshot(ctx context.Context, status *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	sandboxID, _, err := parseInstanceID(status.InstanceID)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}

	body, err := json.Marshal(map[string]string{})
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("e2b: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e2bAPIBase+"/sandboxes/"+sandboxID+"/snapshots", bytes.NewReader(body))
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("e2b: build snapshot request: %w", err)
	}
	req.Header.Set("X-API-Key", p.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("e2b: POST /sandboxes/%s/snapshots: %w", sandboxID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("e2b: POST /sandboxes/%s/snapshots returned %d - %s", sandboxID, resp.StatusCode, string(body))
	}
	var snap sandboxSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("e2b: decode snapshot response: %w", err)
	}
	return compute.SandboxPostSnapshotRunning, &compute.ProviderSnapshot{SnapshotID: snap.SnapshotID}, nil
}

func (p *e2bProvider) StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	body, err := json.Marshal(sandboxCreateRequest{
		TemplateID: snapshot.SnapshotID,
		EnvVars:    map[string]string{"TEMPORAL_TASK_QUEUE": taskQueueName},
		Timeout:    p.timeout,
		AutoResume: sandboxCreateRequestAutoResume{Enabled: true},
	})
	if err != nil {
		return nil, fmt.Errorf("e2b: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e2bAPIBase+"/sandboxes", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("e2b: build request: %w", err)
	}
	req.Header.Set("X-API-Key", p.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("e2b: POST /sandboxes (from snapshot): %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("e2b: POST /sandboxes (from snapshot) returned %d - %s", resp.StatusCode, body)
	}
	var created sandboxCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, fmt.Errorf("e2b: decode response: %w", err)
	}
	return &compute.ProviderStatus{InstanceID: created.SandboxID + ":" + created.ClientID}, nil
}

func (p *e2bProvider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) error {
	path := "/templates/" + snapshot.SnapshotID
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, e2bAPIBase+path, nil)
	if err != nil {
		return fmt.Errorf("e2b: build delete snapshot request: %w", err)
	}
	req.Header.Set("X-API-Key", p.apiKey)
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("e2b: DELETE %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("e2b: DELETE %s returned %d - %s", path, resp.StatusCode, body)
}

func (p *e2bProvider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	sandboxID, clientID, err := parseInstanceID(status.InstanceID)
	if err != nil {
		return nil, err
	}

	// The envd is exposed via E2B's routing as {port}-{sandboxID}-{clientID}.e2b.app.
	baseURL := fmt.Sprintf("https://%d-%s-%s.%s", consts.DefaultEnvdServerPort, sandboxID, clientID, e2bDomain)
	client := processconnect.NewProcessClient(p.httpClient, baseURL)

	req := connect.NewRequest(&process.StartRequest{
		Process: &process.ProcessConfig{
			Cmd:  "bash",
			Args: []string{"-c", cmd},
		},
	})
	// envd authenticates via HTTP Basic auth with the sandbox user.
	req.Header().Set("Authorization", envdBasicAuth("user"))

	stream, err := client.Start(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("e2b: start process: %w", err)
	}
	defer stream.Close()

	var stdout, stderr strings.Builder
	var exitCode int32

	for stream.Receive() {
		event := stream.Msg().GetEvent()
		if event == nil {
			continue
		}
		if d := event.GetData(); d != nil {
			stdout.Write(d.GetStdout())
			stderr.Write(d.GetStderr())
		}
		if end := event.GetEnd(); end != nil {
			exitCode = end.GetExitCode()
		}
	}
	if err := stream.Err(); err != nil {
		return nil, fmt.Errorf("e2b: process stream: %w", err)
	}

	return &compute.CommandResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}, nil
}

func parseInstanceID(instanceID string) (sandboxID, clientID string, err error) {
	parts := strings.SplitN(instanceID, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("e2b: malformed instance ID %q", instanceID)
	}
	return parts[0], parts[1], nil
}

// envdBasicAuth returns the HTTP Basic auth header value for the given user with no password.
func envdBasicAuth(user string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"))
}
