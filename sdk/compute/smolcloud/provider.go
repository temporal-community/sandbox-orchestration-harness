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
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

const providerType = compute.ProviderTypeSmolCloud

func init() { compute.Register(providerType, New) }

// New constructs the provider. The image is the only required config value;
// network defaults to blocked. Use "network": "open" for guest internet.
func New(config map[string]string) (compute.Provider, error) {
	image := strings.TrimSpace(config["image"])
	if image == "" {
		return nil, fmt.Errorf("smol-cloud: image is required")
	}
	network := config["network"]
	if network == "" {
		network = "blocked"
	}
	if network != "open" && network != "blocked" && network != "allowCidrs" {
		return nil, fmt.Errorf("smol-cloud: network must be open, blocked, or allowCidrs")
	}
	cidrs := splitList(config["allow-cidrs"])
	hosts := splitList(config["allow-hosts"])
	if network == "allowCidrs" && len(cidrs)+len(hosts) == 0 {
		return nil, fmt.Errorf("smol-cloud: allowCidrs requires allow-cidrs or allow-hosts")
	}
	if network != "allowCidrs" && len(cidrs)+len(hosts) != 0 {
		return nil, fmt.Errorf("smol-cloud: allow-cidrs and allow-hosts require network=allowCidrs")
	}
	for _, prefix := range cidrs {
		if _, err := netip.ParsePrefix(prefix); err != nil {
			return nil, fmt.Errorf("smol-cloud: invalid CIDR %q: %w", prefix, err)
		}
	}
	for _, host := range hosts {
		if strings.ContainsAny(host, " /*:") || !strings.Contains(host, ".") {
			return nil, fmt.Errorf("smol-cloud: invalid host %q", host)
		}
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
	p := &provider{baseURL: strings.TrimRight(base, "/"), token: os.Getenv("SMOL_CLOUD_TOKEN"), image: image, network: network, cidrs: cidrs, hosts: hosts, cpus: 1, memoryMB: 512, client: &http.Client{}}
	for key, dst := range map[string]*int{"cpus": &p.cpus, "memory-mb": &p.memoryMB, "disk-gb": &p.diskGB} {
		if config[key] != "" {
			v, err := strconv.Atoi(config[key])
			if err != nil || v <= 0 {
				return nil, fmt.Errorf("smol-cloud: %s must be a positive integer", key)
			}
			*dst = v
		}
	}
	return p, nil
}

type provider struct {
	baseURL, token, image, network string
	cidrs, hosts                   []string
	cpus, memoryMB, diskGB         int
	client                         *http.Client
}

var _ compute.Provider = (*provider)(nil)

// String prevents the activity worker's credentials from appearing in logs.
func (p *provider) String() string { return "smol-cloud provider" }

func (p *provider) cleanup(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = p.request(ctx, http.MethodDelete, machinePath(id), nil, nil)
}

func (p *provider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	name, err := machineName(taskQueueName, "")
	if err != nil {
		return nil, err
	}
	resources := map[string]any{}
	if p.cpus != 0 {
		resources["cpus"] = p.cpus
	}
	if p.memoryMB != 0 {
		resources["memoryMb"] = p.memoryMB
	}
	if p.diskGB != 0 {
		resources["diskGb"] = p.diskGB
	}
	var created struct {
		ID string `json:"id"`
	}
	err = p.request(ctx, http.MethodPost, "/v1/machines", map[string]any{
		"name":       name,
		"source":     map[string]string{"type": "image", "reference": p.image},
		"resources":  resources,
		"network":    map[string]string{"mode": p.network},
		"branchable": true,
		"forkable":   true, // accepted by older cloud controls
		"env":        map[string]string{"TEMPORAL_TASK_QUEUE": taskQueueName},
		"labels":     map[string]string{"temporal-harness": "smol-cloud"},
	}, &created)
	recovered := isStatus(err, http.StatusConflict)
	if recovered {
		created.ID, _, err = p.findOwned(ctx, name)
	}
	if err != nil {
		return nil, fmt.Errorf("smol-cloud: create: %w", err)
	}
	if created.ID == "" {
		return nil, fmt.Errorf("smol-cloud: create returned no machine id")
	}
	start := p.start
	if recovered {
		start = p.startRecovered
	}
	if err := start(ctx, created.ID); err != nil {
		p.cleanup(created.ID)
		return nil, err
	}
	return &compute.ProviderStatus{InstanceID: created.ID}, nil
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
	id, err := machineID(status)
	if err != nil {
		return err
	}
	return p.request(ctx, http.MethodPost, machinePath(id)+"/pause", nil, nil)
}

func (p *provider) Resume(ctx context.Context, status *compute.ProviderStatus) error {
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

func (p *provider) StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	if snapshot == nil || snapshot.SnapshotID == "" {
		return nil, fmt.Errorf("smol-cloud: snapshot id is required")
	}
	name, err := machineName(taskQueueName, snapshot.SnapshotID)
	if err != nil {
		return nil, err
	}
	var created struct {
		ID      string        `json:"id"`
		Network networkPolicy `json:"network"`
	}
	err = p.request(ctx, http.MethodPost, "/v1/checkpoints/"+url.PathEscape(snapshot.SnapshotID)+"/restore", map[string]any{
		"name": name, "network": p.networkPolicy(),
	}, &created)
	recovered := isStatus(err, http.StatusConflict)
	if recovered {
		created.ID, created.Network, err = p.findOwned(ctx, name)
	}
	if err != nil {
		return nil, fmt.Errorf("smol-cloud: restore: %w", err)
	}
	if created.ID == "" {
		return nil, fmt.Errorf("smol-cloud: restore returned no machine id")
	}
	if !p.sameNetwork(created.Network) {
		p.cleanup(created.ID)
		return nil, fmt.Errorf("smol-cloud: restored machine network policy differs from requested policy")
	}
	start := p.start
	if recovered {
		start = p.startRecovered
	}
	if err := start(ctx, created.ID); err != nil {
		p.cleanup(created.ID)
		return nil, err
	}
	return &compute.ProviderStatus{InstanceID: created.ID}, nil
}

func (p *provider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) error {
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
	return p.start(ctx, id)
}

func (p *provider) start(ctx context.Context, id string) error {
	if err := p.request(ctx, http.MethodPost, machinePath(id)+"/start?forkable=true", nil, nil); err != nil && !isStatus(err, http.StatusConflict) {
		return fmt.Errorf("smol-cloud: start: %w", err)
	}
	return p.waitReady(ctx, id)
}

// findOwned recovers an already-created VM after Temporal retries an activity.
// Never attach to a machine merely because a human assigned the same name.
func (p *provider) findOwned(ctx context.Context, name string) (string, networkPolicy, error) {
	var payload json.RawMessage
	if err := p.request(ctx, http.MethodGet, "/v1/machines", nil, &payload); err != nil {
		return "", networkPolicy{}, err
	}
	type record struct {
		ID      string            `json:"id"`
		Name    string            `json:"name"`
		Labels  map[string]string `json:"labels"`
		Network networkPolicy     `json:"network"`
	}
	var rows []record
	if len(payload) > 0 && payload[0] == '{' {
		var envelope struct {
			Machines []record `json:"machines"`
		}
		if err := json.Unmarshal(payload, &envelope); err != nil {
			return "", networkPolicy{}, err
		}
		rows = envelope.Machines
	} else if err := json.Unmarshal(payload, &rows); err != nil {
		return "", networkPolicy{}, err
	}
	for _, machine := range rows {
		if machine.Name == name && machine.Labels["temporal-harness"] == "smol-cloud" && machine.ID != "" {
			if !p.sameNetwork(machine.Network) {
				return "", networkPolicy{}, fmt.Errorf("smol-cloud: existing machine network policy differs from requested policy")
			}
			return machine.ID, machine.Network, nil
		}
	}
	return "", networkPolicy{}, fmt.Errorf("smol-cloud: name %q is taken by another machine", name)
}

type networkPolicy struct {
	Mode  string   `json:"mode"`
	CIDRs []string `json:"cidrs,omitempty"`
	Hosts []string `json:"hosts,omitempty"`
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
