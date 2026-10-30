package blaxel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
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

// runActivity runs exec as the activity of a workflow, retried by Temporal.
func runActivity(t *testing.T, exec func(context.Context) (*compute.CommandResult, error)) (*compute.CommandResult, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(5 * time.Minute)
	env.RegisterActivityWithOptions(exec, activity.RegisterOptions{Name: "exec"})
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (*compute.CommandResult, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 2 * time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Millisecond, MaximumAttempts: 3},
		})
		var result *compute.CommandResult
		err := workflow.ExecuteActivity(ctx, "exec").Get(ctx, &result)
		return result, err
	}, workflow.RegisterOptions{Name: "exec"})
	env.ExecuteWorkflow("exec")
	if err := env.GetWorkflowError(); err != nil {
		return nil, err
	}
	var result *compute.CommandResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	return result, nil
}

// isFinal reports whether Temporal will not retry err.
func isFinal(err error, errType string) bool {
	var e *temporal.ApplicationError
	return errors.As(err, &e) && e.NonRetryable() && e.Type() == errType
}

// envs lists a request's environment variables as NAME=value, in order.
func envs(request map[string]any) string {
	var pairs []string
	for _, env := range request["envs"].([]any) {
		env := env.(map[string]any)
		pairs = append(pairs, fmt.Sprintf("%s=%s", env["name"], env["value"]))
	}
	return strings.Join(pairs, " ")
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
	if runtime["image"] != "blaxel/py-app:latest" || runtime["memory"] != float64(2048) || spec["region"] != "us-pdx-1" ||
		envs(runtime) != "TEMPORAL_TASK_QUEUE=queue-1 SANDBOX_DISABLE_PROCESS_LOGGING=true" ||
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
	if forked["targetType"] != "sandbox" || envs(forked) != "TEMPORAL_TASK_QUEUE=queue-2 SANDBOX_DISABLE_PROCESS_LOGGING=true" {
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

func TestCommandRunsOncePerActivity(t *testing.T) {
	var name string
	posts, polls, attempts := 0, 0, 0
	var server *httptest.Server
	server = fakeBlaxel(t, func(w http.ResponseWriter, r *http.Request, route string) {
		switch {
		case route == "GET /v0/sandboxes/temporal-1":
			fmt.Fprint(w, sandboxJSON(server, "temporal-1", "DEPLOYED"))
		case route == "POST /sbx/process":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			posts++
			name, _ = body["name"].(string)
			// The command starts, but its response is lost.
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprint(w, `{"error":"bad gateway"}`)
		case name != "" && route == "GET /sbx/process/"+name+"/logs":
			fmt.Fprint(w, `{"stdout":"deployed","stderr":""}`)
		case name != "" && route == "GET /sbx/process/"+name:
			switch polls++; polls {
			case 2:
				// The first attempt fails while the command keeps running.
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, `{"error":"bad request"}`)
			case 4:
				fmt.Fprintf(w, `{"name":%q,"status":"completed","exitCode":3}`, name)
			default:
				fmt.Fprintf(w, `{"name":%q,"status":"running"}`, name)
			}
		case strings.HasPrefix(route, "GET /sbx/process/"):
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":"process not found"}`)
		default:
			t.Errorf("unexpected request %s", route)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	p, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runActivity(t, func(ctx context.Context) (*compute.CommandResult, error) {
		attempts++
		return p.ExecuteCommand(ctx, &compute.ProviderStatus{InstanceID: "temporal-1"}, "make deploy")
	})
	if err != nil || result.ExitCode != 3 || result.Stdout != "deployed" || attempts != 2 || posts != 1 || !strings.HasPrefix(name, "temporal-") {
		t.Fatalf("command must run once across attempts: attempts=%d posts=%d name=%q result=%+v err=%v", attempts, posts, name, result, err)
	}
}

func TestNothingRepeatedOrPartial(t *testing.T) {
	outputs := map[string]string{
		"yes | head -c 10000000": truncated + "\n...the tail",
		// 600 KB of output that JSON escapes to 3.6 MB.
		"cat page.html": strings.Repeat("<p>", 200_000),
	}
	var commands []string
	polls, snapshots, deleted := 0, 0, false
	var server *httptest.Server
	server = fakeBlaxel(t, func(w http.ResponseWriter, r *http.Request, route string) {
		process := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sbx/process/proc-"), "/logs")
		command := ""
		if i, err := strconv.Atoi(process); err == nil && i <= len(commands) {
			command = commands[i-1]
		}
		switch {
		case route == "GET /v0/sandboxes/temporal-1":
			fmt.Fprint(w, sandboxJSON(server, "temporal-1", "DEPLOYED"))
		case route == "POST /sbx/process":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if _, named := body["name"]; named {
				t.Error("named a process outside an activity")
			}
			commands = append(commands, body["command"].(string))
			// The first command may have started before its response was lost.
			if len(commands) == 1 {
				w.WriteHeader(http.StatusBadGateway)
				fmt.Fprint(w, `{"error":"bad gateway"}`)
				return
			}
			fmt.Fprintf(w, `{"name":"proc-%d","status":"running"}`, len(commands))
		case strings.HasSuffix(route, "/logs"):
			fmt.Fprintf(w, `{"stdout":%q,"stderr":""}`, outputs[command])
		case command == "sleep 900":
			fmt.Fprint(w, `{"status":"killed","exitCode":-1}`)
		case strings.HasPrefix(route, "GET /sbx/process/"):
			if polls++; polls == 1 {
				fmt.Fprint(w, `{"status":"running"}`)
				return
			}
			fmt.Fprint(w, `{"status":"completed","exitCode":0}`)
		case route == "POST /v0/snapshots":
			if snapshots++; snapshots == 1 {
				w.WriteHeader(http.StatusBadGateway)
				fmt.Fprint(w, `{"error":"bad gateway"}`)
				return
			}
			fmt.Fprint(w, `{"id":"snap-bad","name":"snap-bad","status":"failed"}`)
		case route == "DELETE /v0/snapshots/snap-bad":
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
	if _, err := p.ExecuteCommand(ctx, status, "make deploy"); err == nil || len(commands) != 1 {
		t.Fatalf("command must be sent once and left to Temporal's retry: commands=%v err=%v", commands, err)
	}
	if result, err := p.ExecuteCommand(ctx, status, "yes | head -c 10000000"); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "truncated") || polls != 2 {
		t.Fatalf("truncated output returned: polls=%d result=%+v err=%v", polls, result, err)
	}
	if result, err := p.ExecuteCommand(ctx, status, "cat page.html"); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("output over Temporal's payload limit returned: result=%t err=%v", result != nil, err)
	}
	if result, err := p.ExecuteCommand(ctx, status, "sleep 900"); !isFinal(err, "CommandKilled") {
		t.Fatalf("killed command returned or left to retry: result=%+v err=%v", result, err)
	}
	if _, _, err := p.Snapshot(ctx, status); err == nil || snapshots != 1 {
		t.Fatalf("snapshot must be requested once and left to Temporal's retry: snapshots=%d err=%v", snapshots, err)
	}
	if _, snap, err := p.Snapshot(ctx, status); err == nil || !deleted {
		t.Fatalf("failed snapshot returned or left behind: %v %v", snap, err)
	}
}

func TestRejectedCredentialsAreFinal(t *testing.T) {
	fakeBlaxel(t, func(w http.ResponseWriter, r *http.Request, route string) {
		switch route {
		case "POST /v0/sandboxes":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"code":403,"error":"UNAUTHORIZED_WORKSPACE"}`)
		case "GET /v0/sandboxes/temporal-1":
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"code":401,"error":"Unauthorized"}`)
		case "POST /v0/snapshots":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"code":403,"error":"This API key does not have permission to perform this operation."}`)
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
	status := &compute.ProviderStatus{InstanceID: "temporal-1"}
	if _, err := p.Start(ctx, "queue-1"); !isFinal(err, "Unauthorized") {
		t.Fatalf("start with a rejected workspace left to retry: %v", err)
	}
	if _, err := p.ExecuteCommand(ctx, status, "true"); !isFinal(err, "Unauthorized") {
		t.Fatalf("command with a rejected key left to retry: %v", err)
	}
	// Only the snapshot feature's own 403 means snapshots are unsupported.
	if _, _, err := p.Snapshot(ctx, status); !isFinal(err, "Unauthorized") || errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("snapshot without permission: %v", err)
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
