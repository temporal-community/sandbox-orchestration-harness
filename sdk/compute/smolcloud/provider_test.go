package smolcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func TestCloudLifecycleAndSnapshot(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	calls := []string{}
	var created, restored map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing auth")
		}
		if strings.HasSuffix(r.URL.Path, "/start") && r.URL.Query().Get("forkable") != "true" {
			t.Error("branchable start missing forkable query")
		}
		calls = append(calls, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/machines":
			_ = json.NewDecoder(r.Body).Decode(&created)
			fmt.Fprint(w, `{"id":"mach-original","network":{"mode":"blocked"}}`)
		case "POST /v1/checkpoints/cp-123/restore":
			_ = json.NewDecoder(r.Body).Decode(&restored)
			fmt.Fprint(w, `{"id":"mach-restored","network":{"mode":"blocked"}}`)
		case "GET /v1/machines/mach-original", "GET /v1/machines/mach-restored":
			fmt.Fprint(w, `{"state":"started","ready":true}`)
		case "POST /v1/machines/mach-original/exec":
			var body struct {
				Command []string `json:"command"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.Command) != 3 || body.Command[2] != "exit 7" {
				t.Errorf("unexpected command: %v", body.Command)
			}
			fmt.Fprint(w, `{"stdout":"hello","stderr":"oops","exitCode":7}`)
		case "POST /v1/machines/mach-original/checkpoints":
			fmt.Fprint(w, `{"id":"cp-123"}`)
		case "POST /v1/machines/mach-original/start", "POST /v1/machines/mach-restored/start",
			"POST /v1/machines/mach-original/pause", "POST /v1/machines/mach-original/resume",
			"DELETE /v1/machines/mach-original", "DELETE /v1/machines/mach-restored",
			"DELETE /v1/checkpoints/cp-123":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	if !compute.IsRegistered(providerType) {
		t.Fatal("provider not registered")
	}
	instance, err := compute.Lookup(providerType, map[string]string{"image": "python:3.12", "memory-mb": "1024"})
	if err != nil {
		t.Fatal(err)
	}
	p := instance.(*provider)
	ctx := context.Background()
	status, err := p.Start(ctx, "queue-1")
	if err != nil || status.InstanceID != "mach-original" {
		t.Fatalf("start: %v %v", status, err)
	}
	if created["source"].(map[string]any)["reference"] != "python:3.12" || created["resources"].(map[string]any)["memoryMb"] != float64(1024) || created["env"].(map[string]any)["TEMPORAL_TASK_QUEUE"] != "queue-1" {
		t.Fatalf("create request: %v", created)
	}
	if created["network"].(map[string]any)["mode"] != "blocked" || created["forkable"] != nil || created["branchable"] != true || created["resources"].(map[string]any)["cpus"] != float64(1) {
		t.Fatal("unexpected sandbox defaults or missing checkpoint support")
	}
	result, err := p.ExecuteCommand(ctx, status, "exit 7")
	if err != nil || result.ExitCode != 7 || result.Stdout != "hello" || result.Stderr != "oops" {
		t.Fatalf("exec: %v %v", result, err)
	}
	if err := p.Suspend(ctx, status); err != nil {
		t.Fatal(err)
	}
	if err := p.Resume(ctx, status); err != nil {
		t.Fatal(err)
	}
	state, snap, err := p.Snapshot(ctx, status)
	if err != nil || state != compute.SandboxPostSnapshotRunning || snap.SnapshotID != "cp-123" {
		t.Fatalf("snapshot: %v %v %v", state, snap, err)
	}
	fork, err := p.StartFromSnapshot(ctx, "queue-2", snap)
	if err != nil || fork.InstanceID != "mach-restored" {
		t.Fatalf("restore: %v %v", fork, err)
	}
	if restored["name"] == created["name"] || restored["network"].(map[string]any)["mode"] != "blocked" {
		t.Fatalf("restore request: %v", restored)
	}
	if err := p.Stop(ctx, fork); err != nil {
		t.Fatal(err)
	}
	if err := p.Stop(ctx, status); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	if len(calls) < 12 {
		t.Fatalf("missing lifecycle calls: %v", calls)
	}
}

func TestInvalidConfigAndCredentialIsolation(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "secret-from-worker")
	t.Setenv("SMOL_CLOUD_URL", "http://127.0.0.1:1234")
	for _, config := range []map[string]string{{}, {"image": "alpine", "network": "invalid"}, {"image": "alpine", "cpus": "-1"}} {
		if _, err := New(config); err == nil {
			t.Fatalf("accepted invalid config %v", config)
		}
	}
	p, err := New(map[string]string{"image": "alpine"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprintf("%v", p), "secret-from-worker") {
		t.Fatal("provider prints token")
	}
	if _, err := p.Start(context.Background(), ""); err == nil {
		t.Fatal("empty task queue accepted")
	}
}

func TestFailedStartDeletesMachineAndStopHandlesMissing(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/machines":
			fmt.Fprint(w, `{"id":"mach-1","network":{"mode":"blocked"}}`)
		case "POST /v1/machines/mach-1/start":
			w.WriteHeader(http.StatusBadRequest)
		case "DELETE /v1/machines/mach-1":
			deleted = true
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	instance, err := New(map[string]string{"image": "alpine"})
	if err != nil {
		t.Fatal(err)
	}
	p := instance.(*provider)
	if _, err := p.Start(context.Background(), "queue"); err == nil || !deleted {
		t.Fatalf("failed start must clean up: deleted=%t err=%v", deleted, err)
	}
	if err := p.Stop(context.Background(), &compute.ProviderStatus{InstanceID: "mach-1"}); err != nil {
		t.Fatal(err)
	}
}

func TestRetryRecoversOnlyOwnedMachine(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	created := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/machines":
			created++
			w.WriteHeader(http.StatusConflict)
		case "GET /v1/machines":
			name, _ := machineName("queue-1", "")
			fmt.Fprintf(w, `{"machines":[{"id":"foreign","name":%q,"labels":{},"network":{"mode":"blocked"}},{"id":"owned","name":%q,"labels":{"temporal-harness":"smol-cloud"},"network":{"mode":"blocked"}}]}`, name, name)
		case "GET /v1/machines/owned":
			fmt.Fprint(w, `{"state":"started","ready":true}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	instance, err := New(map[string]string{"image": "alpine"})
	if err != nil {
		t.Fatal(err)
	}
	status, err := instance.Start(context.Background(), "queue-1")
	if err != nil || status.InstanceID != "owned" || created != 1 {
		t.Fatalf("retry recovery: %v %v created %d", status, err, created)
	}
}

func TestRestoreRejectsNetworkPolicyChangeBeforeBoot(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	deleted, started := false, false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/checkpoints/cp-123/restore":
			var request struct {
				Network networkPolicy `json:"network"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.Network.Mode != "blocked" {
				t.Errorf("restore network: %+v", request.Network)
			}
			fmt.Fprint(w, `{"id":"mach-bad","network":{"mode":"open"}}`)
		case "DELETE /v1/machines/mach-bad":
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		case "POST /v1/machines/mach-bad/start":
			started = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	instance, err := New(map[string]string{"image": "alpine"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = instance.StartFromSnapshot(context.Background(), "queue", &compute.ProviderSnapshot{SnapshotID: "cp-123"})
	if err == nil || !deleted || started {
		t.Fatalf("network mismatch must cleanup without boot: deleted=%t started=%t err=%v", deleted, started, err)
	}
}

func TestRestrictedEgressValidationAndRestore(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	t.Setenv("SMOL_CLOUD_URL", "http://127.0.0.1:1234")
	for _, config := range []map[string]string{
		{"image": "alpine", "network": "allowCidrs"},
		{"image": "alpine", "network": "allowCidrs", "allow-cidrs": "not-a-cidr"},
		{"image": "alpine", "network": "blocked", "allow-hosts": "api.github.com"},
		{"image": "alpine", "network": "allowCidrs", "allow-hosts": "https://api.github.com"},
	} {
		if _, err := New(config); err == nil {
			t.Fatalf("accepted invalid policy %v", config)
		}
	}
	instance, err := New(map[string]string{"image": "alpine", "network": "allowCidrs", "allow-hosts": "api.github.com, registry-1.docker.io", "allow-cidrs": "192.0.2.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	p := instance.(*provider)
	policy := p.networkPolicy()
	if policy.Mode != "allowCidrs" || len(policy.Hosts) != 2 || policy.CIDRs[0] != "192.0.2.0/24" {
		t.Fatalf("policy: %+v", policy)
	}
	if !p.sameNetwork(networkPolicy{Mode: "allowCidrs", Hosts: []string{"registry-1.docker.io", "api.github.com"}, CIDRs: []string{"192.0.2.0/24"}}) {
		t.Fatal("order should not affect allowlist comparison")
	}
	if p.sameNetwork(networkPolicy{Mode: "allowCidrs", Hosts: []string{"api.github.com", "evil.example"}, CIDRs: []string{"192.0.2.0/24"}}) {
		t.Fatal("different allowlist must be rejected")
	}
}

func TestCreateRejectsUnexpectedNetworkBeforeBoot(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	deleted, started := false, false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/machines":
			fmt.Fprint(w, `{"id":"mach-unexpected","network":{"mode":"open"}}`)
		case "DELETE /v1/machines/mach-unexpected":
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		case "POST /v1/machines/mach-unexpected/start":
			started = true
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	instance, err := New(map[string]string{"image": "alpine"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = instance.Start(context.Background(), "queue")
	if err == nil || !deleted || started {
		t.Fatalf("unexpected policy must cleanup before boot: deleted=%t started=%t err=%v", deleted, started, err)
	}
}

func TestRecoveredStartFailureKeepsExistingMachine(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/machines":
			w.WriteHeader(http.StatusConflict)
		case "GET /v1/machines":
			name, _ := machineName("queue", "")
			fmt.Fprintf(w, `{"machines":[{"id":"owned","name":%q,"labels":{"temporal-harness":"smol-cloud"},"network":{"mode":"blocked"}}]}`, name)
		case "GET /v1/machines/owned":
			w.WriteHeader(http.StatusInternalServerError)
		case "DELETE /v1/machines/owned":
			deleted = true
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	instance, err := New(map[string]string{"image": "alpine"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.Start(context.Background(), "queue"); err == nil || deleted {
		t.Fatalf("failed recovery must preserve existing VM: deleted=%t err=%v", deleted, err)
	}
}

func TestRestrictedEgressSentOnCreate(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	var policy networkPolicy
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/machines":
			var body struct {
				Network networkPolicy `json:"network"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			policy = body.Network
			fmt.Fprint(w, `{"id":"restricted","network":{"mode":"allowCidrs","cidrs":["192.0.2.0/24"],"hosts":["example.com"]}}`)
		case "POST /v1/machines/restricted/start":
			w.WriteHeader(http.StatusNoContent)
		case "GET /v1/machines/restricted":
			fmt.Fprint(w, `{"state":"started","ready":true}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	instance, err := New(map[string]string{"image": "alpine", "network": "allowCidrs", "allow-cidrs": "192.0.2.0/24", "allow-hosts": "example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.Start(context.Background(), "queue"); err != nil {
		t.Fatal(err)
	}
	if !instance.(*provider).sameNetwork(policy) {
		t.Fatalf("allowlist omitted from create: %+v", policy)
	}
}

func TestRestoreRequestsNetworkPolicy(t *testing.T) {
	t.Setenv("SMOL_CLOUD_TOKEN", "test-token")
	policy := networkPolicy{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/checkpoints/checkpoint/restore":
			var request struct {
				Network networkPolicy `json:"network"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			policy = request.Network
			fmt.Fprint(w, `{"id":"restored","network":{"mode":"allowCidrs","hosts":["example.com"]}}`)
		case "POST /v1/machines/restored/start":
			w.WriteHeader(http.StatusNoContent)
		case "GET /v1/machines/restored":
			fmt.Fprint(w, `{"state":"started","ready":true}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	instance, err := New(map[string]string{"image": "alpine", "network": "allowCidrs", "allow-hosts": "example.com"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = instance.StartFromSnapshot(context.Background(), "queue", &compute.ProviderSnapshot{SnapshotID: "checkpoint"})
	if err != nil {
		t.Fatal(err)
	}
	if !instance.(*provider).sameNetwork(policy) {
		t.Fatalf("restore policy missing: %+v", policy)
	}
}
