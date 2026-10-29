package blaxel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

// fakeBlaxel serves the control plane under /v0 and every sandbox under /sbx.
func fakeBlaxel(t *testing.T, handle func(w http.ResponseWriter, r *http.Request, route string)) *httptest.Server {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("BL_WORKSPACE", "test-workspace")
	t.Setenv("BL_API_KEY", "test-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" || r.Header.Get("X-Blaxel-Workspace") != "test-workspace" {
			t.Error("missing auth or workspace header")
		}
		w.Header().Set("Content-Type", "application/json")
		handle(w, r, r.Method+" "+r.URL.Path)
	}))
	t.Cleanup(server.Close)
	t.Setenv("BL_API_URL", server.URL+"/v0")
	return server
}

func sandboxJSON(server *httptest.Server, name, status string) string {
	return fmt.Sprintf(`{"metadata":{"name":%q,"url":%q},"status":%q}`, name, server.URL+"/sbx", status)
}

func TestLifecycleAndSnapshot(t *testing.T) {
	calls := []string{}
	var created, forked, execed map[string]any
	var server *httptest.Server
	server = fakeBlaxel(t, func(w http.ResponseWriter, r *http.Request, route string) {
		calls = append(calls, route)
		name := strings.TrimPrefix(r.URL.Path, "/v0/sandboxes/")
		switch {
		case route == "POST /v0/sandboxes":
			if r.URL.Query().Get("createIfNotExist") != "true" {
				t.Error("create is not idempotent")
			}
			_ = json.NewDecoder(r.Body).Decode(&created)
			fmt.Fprint(w, sandboxJSON(server, created["metadata"].(map[string]any)["name"].(string), "DEPLOYING"))
		case route == "GET /v0/sandboxes/"+name:
			fmt.Fprint(w, sandboxJSON(server, name, "DEPLOYED"))
		case route == "POST /sbx/process":
			_ = json.NewDecoder(r.Body).Decode(&execed)
			fmt.Fprint(w, `{"name":"proc-1","status":"running"}`)
		case route == "GET /sbx/process/proc-1":
			fmt.Fprint(w, `{"name":"proc-1","status":"completed","exitCode":7,"stdout":"hel","stderr":"oo"}`)
		case route == "GET /sbx/process/proc-1/logs":
			fmt.Fprint(w, `{"stdout":"hello","stderr":"oops","logs":"hellooops"}`)
		case route == "POST /v0/snapshots":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["source"].(map[string]any)["name"] != created["metadata"].(map[string]any)["name"] {
				t.Errorf("snapshot source: %v", body)
			}
			fmt.Fprint(w, `{"id":"snap-123","name":"snap-123","status":"ready"}`)
		case route == "POST /v0/snapshots/snap-123/fork":
			_ = json.NewDecoder(r.Body).Decode(&forked)
			fmt.Fprintf(w, `{"name":%q,"type":"sandbox"}`, forked["targetName"])
		case route == "DELETE /v0/sandboxes/"+name, route == "DELETE /v0/snapshots/snap-123":
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":"not found"}`)
		default:
			t.Errorf("unexpected request %s", route)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	if !compute.IsRegistered(providerType) {
		t.Fatal("provider not registered")
	}
	instance, err := compute.Lookup(providerType, map[string]string{"image": "blaxel/py-app:latest", "memory-mb": "2048", "region": "us-pdx-1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	status, err := instance.Start(ctx, "queue-1")
	if err != nil || !strings.HasPrefix(status.InstanceID, "temporal-") {
		t.Fatalf("start: %v %v", status, err)
	}
	spec := created["spec"].(map[string]any)
	runtime := spec["runtime"].(map[string]any)
	env := runtime["envs"].([]any)[0].(map[string]any)
	if runtime["image"] != "blaxel/py-app:latest" || runtime["memory"] != float64(2048) || spec["region"] != "us-pdx-1" ||
		env["name"] != "TEMPORAL_TASK_QUEUE" || env["value"] != "queue-1" ||
		created["metadata"].(map[string]any)["labels"].(map[string]any)["temporal-harness"] != "blaxel" {
		t.Fatalf("create request: %v", created)
	}
	result, err := instance.ExecuteCommand(ctx, status, "exit 7")
	if err != nil || result.ExitCode != 7 || result.Stdout != "hello" || result.Stderr != "oops" {
		t.Fatalf("exec: %v %v", result, err)
	}
	if execed["command"] != "exit 7" || execed["keepAlive"] != true || execed["timeout"].(float64) < 50 {
		t.Fatalf("exec request: %v", execed)
	}
	if err := instance.Suspend(ctx, status); err != nil {
		t.Fatal(err)
	}
	if err := instance.Resume(ctx, status); err != nil {
		t.Fatal(err)
	}
	state, snap, err := instance.Snapshot(ctx, status)
	if err != nil || state != compute.SandboxPostSnapshotRunning || snap.SnapshotID != "snap-123" {
		t.Fatalf("snapshot: %v %v %v", state, snap, err)
	}
	fork, err := instance.StartFromSnapshot(ctx, "queue-2", snap)
	if err != nil || fork.InstanceID == status.InstanceID || fork.InstanceID != forked["targetName"] {
		t.Fatalf("fork: %v %v", fork, err)
	}
	env = forked["envs"].([]any)[0].(map[string]any)
	if forked["targetType"] != "sandbox" || env["value"] != "queue-2" {
		t.Fatalf("fork request: %v", forked)
	}
	// Deleting what is already gone succeeds, so retried cleanup is safe.
	if err := instance.Stop(ctx, status); err != nil {
		t.Fatal(err)
	}
	if err := instance.DeleteSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 11 {
		t.Fatalf("unexpected calls: %v", calls)
	}
}

func TestRetriedForkAndDisabledSnapshots(t *testing.T) {
	var server *httptest.Server
	server = fakeBlaxel(t, func(w http.ResponseWriter, r *http.Request, route string) {
		switch {
		case route == "POST /v0/snapshots/snap-123/fork":
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"error":"already exists"}`)
		case strings.HasPrefix(route, "GET /v0/sandboxes/"):
			fmt.Fprint(w, sandboxJSON(server, strings.TrimPrefix(r.URL.Path, "/v0/sandboxes/"), "DEPLOYED"))
		case route == "POST /v0/snapshots", route == "DELETE /v0/snapshots/snap-123", route == "POST /v0/snapshots/snap-456/fork":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":"Sandbox snapshot/fork feature is not enabled for this workspace"}`)
		default:
			t.Errorf("unexpected request %s", route)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	p, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	snap := &compute.ProviderSnapshot{SnapshotID: "snap-123"}
	if fork, err := p.StartFromSnapshot(ctx, "queue-2", snap); err != nil || fork.InstanceID == "" {
		t.Fatalf("retried fork must adopt the sandbox the first attempt created: %v %v", fork, err)
	}
	if _, _, err := p.Snapshot(ctx, &compute.ProviderStatus{InstanceID: "temporal-1"}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("snapshot in a workspace without snapshots: %v", err)
	}
	if err := p.DeleteSnapshot(ctx, snap); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("delete snapshot in a workspace without snapshots: %v", err)
	}
	if _, err := p.StartFromSnapshot(ctx, "queue-3", &compute.ProviderSnapshot{SnapshotID: "snap-456"}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("fork in a workspace without snapshots: %v", err)
	}
}

func TestNothingRepeatedOrPartial(t *testing.T) {
	posts, polls, snapshots, deleted := 0, 0, 0, false
	var server *httptest.Server
	server = fakeBlaxel(t, func(w http.ResponseWriter, r *http.Request, route string) {
		switch route {
		case "GET /v0/sandboxes/temporal-1":
			fmt.Fprint(w, sandboxJSON(server, "temporal-1", "DEPLOYED"))
		case "POST /sbx/process":
			// The first command may have started before its response was lost.
			if posts++; posts == 1 {
				w.WriteHeader(http.StatusBadGateway)
				fmt.Fprint(w, `{"error":"bad gateway"}`)
				return
			}
			fmt.Fprint(w, `{"name":"proc-1","status":"running"}`)
		case "GET /sbx/process/proc-1":
			if polls++; polls == 1 {
				fmt.Fprint(w, `{"name":"proc-1","status":"running"}`)
				return
			}
			fmt.Fprint(w, `{"name":"proc-1","status":"completed","exitCode":0}`)
		case "GET /sbx/process/proc-1/logs":
			fmt.Fprintf(w, `{"stdout":%q,"stderr":""}`, truncated+"\n...the tail")
		case "POST /v0/snapshots":
			if snapshots++; snapshots == 1 {
				w.WriteHeader(http.StatusBadGateway)
				fmt.Fprint(w, `{"error":"bad gateway"}`)
				return
			}
			fmt.Fprint(w, `{"id":"snap-bad","name":"snap-bad","status":"failed"}`)
		case "DELETE /v0/snapshots/snap-bad":
			deleted = true
			fmt.Fprint(w, `{}`)
		default:
			t.Errorf("unexpected request %s", route)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	p, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	status := &compute.ProviderStatus{InstanceID: "temporal-1"}
	if _, err := p.ExecuteCommand(ctx, status, "make deploy"); err == nil || posts != 1 {
		t.Fatalf("command must be sent once and left to Temporal's retry: posts=%d err=%v", posts, err)
	}
	if result, err := p.ExecuteCommand(ctx, status, "yes | head -c 10000000"); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "truncated") || polls != 2 {
		t.Fatalf("truncated output returned: polls=%d result=%+v err=%v", polls, result, err)
	}
	if _, _, err := p.Snapshot(ctx, status); err == nil || snapshots != 1 {
		t.Fatalf("snapshot must be requested once and left to Temporal's retry: snapshots=%d err=%v", snapshots, err)
	}
	if _, snap, err := p.Snapshot(ctx, status); err == nil || !deleted {
		t.Fatalf("failed snapshot returned or left behind: %v %v", snap, err)
	}
}

func TestWorkerRegionAndFailedSandbox(t *testing.T) {
	var server *httptest.Server
	server = fakeBlaxel(t, func(w http.ResponseWriter, r *http.Request, route string) {
		var created map[string]any
		_ = json.NewDecoder(r.Body).Decode(&created)
		if route != "POST /v0/sandboxes" || created["spec"].(map[string]any)["region"] != "eu-lon-1" {
			t.Errorf("unexpected request %s %v", route, created)
		}
		fmt.Fprint(w, sandboxJSON(server, "temporal-1", "FAILED"))
	})
	t.Setenv("BL_REGION", "eu-lon-1")
	p, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := p.Start(context.Background(), "queue-1"); err == nil {
		t.Fatalf("failed sandbox returned: %v", status)
	}
}

func TestInvalidConfigAndCredentialIsolation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("BL_WORKSPACE", "")
	t.Setenv("BL_API_KEY", "secret-from-worker")
	if _, err := New(nil); err == nil {
		t.Fatal("accepted a worker without a workspace")
	}
	t.Setenv("BL_WORKSPACE", "test-workspace")
	t.Setenv("BL_API_KEY", "")
	t.Setenv("BL_CLIENT_CREDENTIALS", "")
	if _, err := New(nil); err == nil {
		t.Fatal("accepted a worker without credentials")
	}
	t.Setenv("BL_API_KEY", "secret-from-worker")
	if _, err := New(map[string]string{"memory-mb": "-1"}); err == nil {
		t.Fatal("accepted invalid memory-mb")
	}
	p, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprintf("%v %+v", p, p), "secret-from-worker") {
		t.Fatal("provider prints API key")
	}
	if _, err := p.Start(context.Background(), ""); err == nil {
		t.Fatal("empty task queue accepted")
	}
}
