// Package smolcloud provides a Smol Machines Cloud compute provider for the
// Temporal sandbox orchestration harness. Credentials stay on the activity
// worker in SMOL_CLOUD_TOKEN and are never placed in workflow history.
package smolcloud

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

const providerType = compute.ProviderTypeSmolCloud

func init() {
	compute.Register(providerType, New)
	compute.RegisterPolicy(providerType, resolvePolicy)
}

// New constructs the provider. The image is the only required config value;
// network defaults to blocked. Use "network": "open" for guest internet.
// Bootstrap, lifecycle, and branchable are independent settings. The historical
// workload=native-process preset selects none, fresh, and false respectively.
func New(config map[string]string) (compute.Provider, error) {
	c, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	base := os.Getenv("SMOL_CLOUD_URL")
	if base == "" {
		base = "https://api.smolmachines.com"
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, fmt.Errorf("smol-cloud: invalid SMOL_CLOUD_URL")
	}
	if parsed.Scheme == "http" && parsed.Hostname() != "localhost" && parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "::1" {
		return nil, fmt.Errorf("smol-cloud: SMOL_CLOUD_URL must use HTTPS")
	}
	if os.Getenv("SMOL_CLOUD_TOKEN") == "" {
		return nil, fmt.Errorf("smol-cloud: set SMOL_CLOUD_TOKEN on the activity worker")
	}
	p := &provider{
		resolvedConfig: c,
		baseURL:        strings.TrimRight(base, "/"),
		token:          os.Getenv("SMOL_CLOUD_TOKEN"),
		client:         &http.Client{},
	}
	return p, nil
}

type provider struct {
	resolvedConfig
	baseURL, token string
	client         *http.Client
}

// nativeBinding binds the existing queue identity to the admitted nonsecret request.
// Preserve this v1 encoding for configurations equivalent to the historical preset.
type nativeBinding struct {
	Version   int
	Queue     string
	Workload  string
	Source    machineSource
	Resources machineResources
	Network   networkPolicy
}

// configurationLabel hashes a canonical binding; allowlist order has no meaning.
func (p *provider) configurationLabel(queue string) string {
	network := p.networkPolicy()
	network.CIDRs, network.Hosts = slices.Clone(network.CIDRs), slices.Clone(network.Hosts)
	slices.Sort(network.CIDRs)
	slices.Sort(network.Hosts)
	var binding any = struct {
		Version    int
		Queue      string
		Bootstrap  bootstrapMode
		Lifecycle  compute.LifecyclePolicy
		Branchable bool
		Source     machineSource
		Resources  machineResources
		Network    networkPolicy
	}{2, queue, p.bootstrap, p.policy.Lifecycle, p.branchable, machineSource{"image", p.image}, machineResources{p.cpus, p.memoryMB, p.diskGB}, network}
	if p.bootstrap == bootstrapNone && p.policy.Lifecycle == compute.LifecycleFresh && !p.branchable {
		binding = nativeBinding{1, queue, "native-process", machineSource{"image", p.image}, machineResources{p.cpus, p.memoryMB, p.diskGB}, network}
	}
	encoded, _ := json.Marshal(binding)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

var _ compute.Provider = (*provider)(nil)

// String prevents the activity worker's credentials from appearing in logs.
func (p *provider) String() string { return "smol-cloud provider" }

func (p *provider) cleanup(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = p.request(ctx, http.MethodDelete, machinePath(id), nil, nil)
}

// validateOperation applies lifecycle admission before endpoint or identity validation.
func (p *provider) validateOperation(op compute.Operation) error {
	if err := p.policy.ValidateOperation(op); err != nil {
		if p.preset && op == compute.OperationRestore {
			return fmt.Errorf("smol-cloud: native-process does not support restoration: %w", err)
		}
		return err
	}
	if !p.branchable && (op == compute.OperationSnapshot || op == compute.OperationRestore) {
		return fmt.Errorf("smol-cloud: branchable=false does not support %s: %w", op, errors.ErrUnsupported)
	}
	return nil
}

func (p *provider) freshDiagnostic() string {
	if p.preset {
		return "native-process"
	}
	return "fresh lifecycle"
}

// Start creates or adopts the configured workload using the stable queue name.
// Explicit settings require unique ownership and matching configuration on adoption.
func (p *provider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	name, err := machineName(taskQueueName, "")
	if err != nil {
		return nil, err
	}
	request := createMachineRequest{
		Name:       name,
		Source:     machineSource{Type: "image", Reference: p.image},
		Resources:  machineResources{CPUs: p.cpus, MemoryMB: p.memoryMB, DiskGB: p.diskGB},
		Network:    p.networkPolicy(),
		Branchable: p.branchable,
		Env:        map[string]string{"TEMPORAL_TASK_QUEUE": taskQueueName},
		Labels:     map[string]string{"temporal-harness": "smol-cloud"},
	}
	if p.bootstrap == bootstrapNone {
		request.Env = map[string]string{}
	}
	if p.bound {
		request.Labels["temporal-harness-config"] = p.configurationLabel(taskQueueName)
	}
	var created createdMachine
	err = p.request(ctx, http.MethodPost, "/v1/machines", request, &created)
	return p.adopt(ctx, name, taskQueueName, "create", "created", created, err)
}

func (p *provider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	id, err := machineID(status)
	if err != nil {
		return err
	}
	err = p.request(ctx, http.MethodDelete, machinePath(id), nil, nil)
	if isStatus(err, http.StatusNotFound) {
		return nil
	}
	return err
}

func (p *provider) Suspend(ctx context.Context, status *compute.ProviderStatus) error {
	if err := p.validateOperation(compute.OperationSuspend); err != nil {
		return err
	}
	id, err := machineID(status)
	if err != nil {
		return err
	}
	return p.request(ctx, http.MethodPost, machinePath(id)+"/pause", nil, nil)
}

func (p *provider) Resume(ctx context.Context, status *compute.ProviderStatus) error {
	if err := p.validateOperation(compute.OperationResume); err != nil {
		return err
	}
	id, err := machineID(status)
	if err != nil {
		return err
	}
	if err := p.request(ctx, http.MethodPost, machinePath(id)+"/resume", nil, nil); err != nil {
		return err
	}
	return p.waitReady(ctx, id)
}

func (p *provider) Snapshot(ctx context.Context, status *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	if err := p.validateOperation(compute.OperationSnapshot); err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}
	id, err := machineID(status)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}
	var checkpoint struct {
		ID string `json:"id"`
	}
	err = p.request(ctx, http.MethodPost, machinePath(id)+"/checkpoints", nil, &checkpoint)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}
	if checkpoint.ID == "" {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("smol-cloud: checkpoint returned no id")
	}
	return compute.SandboxPostSnapshotRunning, &compute.ProviderSnapshot{SnapshotID: checkpoint.ID}, nil
}

// StartFromSnapshot restores when lifecycle and branchability permit it. The API
// does not accept label overrides on restore; explicit configurations refuse
// conflict adoption unless the existing machine already has a matching binding.
func (p *provider) StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	if err := p.validateOperation(compute.OperationRestore); err != nil {
		return nil, err
	}
	if snapshot == nil || snapshot.SnapshotID == "" {
		return nil, fmt.Errorf("smol-cloud: snapshot id is required")
	}
	name, err := machineName(taskQueueName, snapshot.SnapshotID)
	if err != nil {
		return nil, err
	}
	var created createdMachine
	err = p.request(ctx, http.MethodPost, "/v1/checkpoints/"+url.PathEscape(snapshot.SnapshotID)+"/restore", map[string]any{"name": name, "network": p.networkPolicy()}, &created)
	return p.adopt(ctx, name, taskQueueName, "restore", "restored", created, err)
}

func (p *provider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) error {
	if err := p.validateOperation(compute.OperationDeleteSnapshot); err != nil {
		return err
	}
	if snapshot == nil || snapshot.SnapshotID == "" {
		return fmt.Errorf("smol-cloud: snapshot id is required")
	}
	err := p.request(ctx, http.MethodDelete, "/v1/checkpoints/"+url.PathEscape(snapshot.SnapshotID), nil, nil)
	if isStatus(err, http.StatusNotFound) {
		return nil
	}
	return err
}

func (p *provider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	id, err := machineID(status)
	if err != nil {
		return nil, err
	}
	var result struct {
		Stdout          string `json:"stdout"`
		Stderr          string `json:"stderr"`
		ExitCode        int32  `json:"exitCode"`
		StdoutTruncated bool   `json:"stdoutTruncated"`
		StderrTruncated bool   `json:"stderrTruncated"`
	}
	body := map[string]any{"command": []string{"/bin/sh", "-lc", cmd}}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := int(time.Until(deadline).Seconds())
		if remaining < 1 {
			return nil, context.DeadlineExceeded
		}
		body["timeoutSeconds"] = remaining
	}
	if err := p.request(ctx, http.MethodPost, machinePath(id)+"/exec", body, &result); err != nil {
		return nil, err
	}
	if result.StdoutTruncated || result.StderrTruncated {
		return nil, fmt.Errorf("smol-cloud: command output truncated; refusing to report incomplete output")
	}
	return &compute.CommandResult{Stdout: result.Stdout, Stderr: result.Stderr, ExitCode: result.ExitCode}, nil
}

// adopt finishes a create or restore. On a 409 it attaches to the machine an
// earlier attempt of this activity created, then checks its network policy and
// starts it. A machine this attempt created is deleted if either step fails.
func (p *provider) adopt(ctx context.Context, name, queue, op, past string, created createdMachine, err error) (*compute.ProviderStatus, error) {
	recovered := isStatus(err, http.StatusConflict)
	if recovered {
		created.ID, created.Network, err = p.findOwned(ctx, name, queue)
	}
	if err != nil {
		if p.bound && !recovered && created.ID != "" {
			p.cleanup(created.ID)
		}
		return nil, fmt.Errorf("smol-cloud: %s: %w", op, err)
	}
	if created.ID == "" {
		return nil, fmt.Errorf("smol-cloud: %s returned no machine id", op)
	}
	if !p.sameNetwork(created.Network) {
		if !recovered {
			p.cleanup(created.ID)
		}
		return nil, fmt.Errorf("smol-cloud: %s machine network policy differs from requested policy", past)
	}
	start := p.start
	if recovered {
		start = p.startRecovered
	}
	if err := start(ctx, created.ID); err != nil {
		if !recovered {
			p.cleanup(created.ID)
		}
		return nil, err
	}
	return &compute.ProviderStatus{InstanceID: created.ID}, nil
}

// An activity retry can encounter the VM started by its previous attempt.
func (p *provider) startRecovered(ctx context.Context, id string) error {
	var state struct {
		State string `json:"state"`
	}
	if err := p.request(ctx, http.MethodGet, machinePath(id), nil, &state); err != nil {
		return err
	}
	if state.State == "started" || state.State == "running" {
		return p.waitReady(ctx, id)
	}
	if state.State == "paused" {
		if p.policy.Lifecycle == compute.LifecycleFresh {
			return fmt.Errorf("smol-cloud: %s cannot adopt paused machine: %w", p.freshDiagnostic(), errors.ErrUnsupported)
		}
		return p.Resume(ctx, &compute.ProviderStatus{InstanceID: id})
	}
	if p.policy.Lifecycle == compute.LifecycleFresh && state.State != "stopped" {
		return fmt.Errorf("smol-cloud: %s cannot adopt machine in state %q", p.freshDiagnostic(), state.State)
	}
	return p.start(ctx, id)
}

func (p *provider) start(ctx context.Context, id string) error {
	path := machinePath(id) + "/start"
	if p.branchable {
		path += "?forkable=true"
	}
	if err := p.request(ctx, http.MethodPost, path, nil, nil); err != nil && !isStatus(err, http.StatusConflict) {
		return fmt.Errorf("smol-cloud: start: %w", err)
	}
	return p.waitReady(ctx, id)
}

// findOwned recovers an already-created VM after Temporal retries an activity.
// Never attach to a machine merely because a human assigned the same name.
func (p *provider) findOwned(ctx context.Context, name, queue string) (string, networkPolicy, error) {
	var rows []struct {
		ID      string            `json:"id"`
		Name    string            `json:"name"`
		Labels  map[string]string `json:"labels"`
		Network networkPolicy     `json:"network"`
	}
	if err := p.request(ctx, http.MethodGet, "/v1/machines", nil, &rows); err != nil {
		return "", networkPolicy{}, err
	}
	if p.bound {
		matches := 0
		for _, machine := range rows {
			if machine.Name == name {
				matches++
			}
		}
		if matches != 1 {
			return "", networkPolicy{}, fmt.Errorf("smol-cloud: explicit configuration requires exactly one matching machine, got %d", matches)
		}
	}
	for _, machine := range rows {
		if machine.Name == name && machine.Labels["temporal-harness"] == "smol-cloud" && machine.ID != "" {
			if p.bound && machine.Labels["temporal-harness-config"] != p.configurationLabel(queue) {
				return "", networkPolicy{}, fmt.Errorf("smol-cloud: existing machine configuration differs from requested configuration")
			}
			if !p.sameNetwork(machine.Network) {
				return "", networkPolicy{}, fmt.Errorf("smol-cloud: existing machine network policy differs from requested policy")
			}
			return machine.ID, machine.Network, nil
		}
	}
	return "", networkPolicy{}, fmt.Errorf("smol-cloud: name %q is taken by another machine", name)
}

func splitList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	for i, part := range parts {
		parts[i] = strings.TrimSpace(part)
	}
	return parts
}

func (p *provider) networkPolicy() networkPolicy {
	return networkPolicy{Mode: p.network, CIDRs: p.cidrs, Hosts: p.hosts}
}

func (p *provider) sameNetwork(actual networkPolicy) bool {
	if actual.Mode != p.network || len(actual.CIDRs) != len(p.cidrs) || len(actual.Hosts) != len(p.hosts) {
		return false
	}
	cidrs, hosts := slices.Clone(actual.CIDRs), slices.Clone(actual.Hosts)
	wantCIDRs, wantHosts := slices.Clone(p.cidrs), slices.Clone(p.hosts)
	slices.Sort(cidrs)
	slices.Sort(hosts)
	slices.Sort(wantCIDRs)
	slices.Sort(wantHosts)
	return slices.Equal(cidrs, wantCIDRs) && slices.Equal(hosts, wantHosts)
}

func (p *provider) waitReady(ctx context.Context, id string) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		var state struct {
			State string `json:"state"`
			Ready *bool  `json:"ready"`
		}
		if err := p.request(ctx, http.MethodGet, machinePath(id), nil, &state); err != nil {
			return err
		}
		if state.State == "failed" || state.State == "deleted" || state.State == "stopped" {
			return fmt.Errorf("smol-cloud: machine %s entered %s", id, state.State)
		}
		if p.policy.Lifecycle == compute.LifecycleFresh && state.State == "paused" {
			return fmt.Errorf("smol-cloud: %s cannot adopt paused machine: %w", p.freshDiagnostic(), errors.ErrUnsupported)
		}
		if state.Ready != nil && *state.Ready {
			return nil
		}
		if state.State == "started" || state.State == "running" {
			var probe struct {
				ExitCode int `json:"exitCode"`
			}
			if err := p.request(ctx, http.MethodPost, machinePath(id)+"/exec", map[string]any{"command": []string{"sh", "-c", "true"}, "timeoutSeconds": 2}, &probe); err == nil && probe.ExitCode == 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *provider) request(ctx context.Context, method, path string, body any, dest any) error {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, payload)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &httpStatusError{status: resp.StatusCode, message: string(message)}
	}
	if dest == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		return fmt.Errorf("smol-cloud: decode %s %s: %w", method, path, err)
	}
	return nil
}

type httpStatusError struct {
	status  int
	message string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("cloud API returned %d: %s", e.status, e.message)
}
func isStatus(err error, status int) bool {
	var e *httpStatusError
	return errors.As(err, &e) && e.status == status
}
func machinePath(id string) string { return "/v1/machines/" + url.PathEscape(id) }
func machineID(status *compute.ProviderStatus) (string, error) {
	if status == nil || status.InstanceID == "" {
		return "", fmt.Errorf("smol-cloud: machine id is required")
	}
	return status.InstanceID, nil
}
func machineName(queue, snapshot string) (string, error) {
	if queue == "" {
		return "", fmt.Errorf("smol-cloud: task queue is required")
	}
	sum := sha256.Sum256([]byte(queue + "\x00" + snapshot))
	return "temporal-" + hex.EncodeToString(sum[:8]), nil
}
