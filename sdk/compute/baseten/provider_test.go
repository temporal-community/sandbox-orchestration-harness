package baseten

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

// fakeBaseten serves the management API under /v1 and every sandbox under
// /sbx, and returns a provider pointed at it. It answers the token exchange
// itself and checks that sandbox requests carry the token, not the API key.
func fakeBaseten(t *testing.T, config map[string]string, handle func(w http.ResponseWriter, r *http.Request, route string)) (*provider, *httptest.Server) {
	t.Helper()
	t.Setenv("BASETEN_API_KEY", "test-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method+" "+r.URL.Path == "POST /v1/token" {
			if r.Header.Get("Authorization") != "Bearer test-key" {
				t.Error("token exchange without the API key")
			}
			fmt.Fprintf(w, `{"token":"test-token","expires_at":%q}`, time.Now().Add(2*time.Hour).Format(time.RFC3339))
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("%s %s without the sandbox token", r.Method, r.URL.Path)
		}
		handle(w, r, r.Method+" "+r.URL.Path)
	}))
	t.Cleanup(server.Close)
	p, err := newProvider(config, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return p.(*provider), server
}

func sandboxJSON(server *httptest.Server, name, status string) string {
	return fmt.Sprintf(`{"name":%q,"url":%q,"status":%q}`, name, server.URL+"/sbx", status)
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

func TestLifecycle(t *testing.T) {
	calls := []string{}
	var created, execed map[string]any
	var server *httptest.Server
	p, server := fakeBaseten(t, map[string]string{"image": "baseten/node:latest", "memory-mb": "2048", "region": "us-pdx-1"}, func(w http.ResponseWriter, r *http.Request, route string) {
		calls = append(calls, route)
		name := strings.TrimPrefix(r.URL.Path, "/v1/sandboxes/instances/")
		switch {
		case route == "POST /v1/sandboxes/instances":
			_ = json.NewDecoder(r.Body).Decode(&created)
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, sandboxJSON(server, created["name"].(string), "DEPLOYING"))
		case route == "GET /v1/sandboxes/instances/"+name:
			fmt.Fprint(w, sandboxJSON(server, name, "DEPLOYED"))
		case route == "POST /sbx/process":
			_ = json.NewDecoder(r.Body).Decode(&execed)
			fmt.Fprint(w, `{"name":"proc-1","status":"running","pid":"1","command":"exit 7"}`)
		case route == "GET /sbx/process/proc-1":
			fmt.Fprint(w, `{"name":"proc-1","status":"completed","exitCode":7,"stdout":"hel","stderr":"oo"}`)
		case route == "GET /sbx/process/proc-1/logs":
			fmt.Fprint(w, `{"stdout":"hello","stderr":"oops","logs":"hellooops"}`)
		case route == "DELETE /v1/sandboxes/instances/"+name:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":"not_found","message":"sandbox not found"}`)
		default:
			t.Errorf("unexpected request %s", route)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	if !compute.IsRegistered(providerType) {
		t.Fatal("provider not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	status, err := p.Start(ctx, "queue-1")
	if err != nil || !strings.HasPrefix(status.InstanceID, "temporal-") {
		t.Fatalf("start: %v %v", status, err)
	}
	envs := map[string]any{}
	for _, env := range created["envs"].([]any) {
		env := env.(map[string]any)
		envs[env["name"].(string)] = env["value"]
	}
	if created["create_if_not_exists"] != true || created["image"] != "baseten/node:latest" || created["memory"] != float64(2048) ||
		created["region"] != "us-pdx-1" || envs["TEMPORAL_TASK_QUEUE"] != "queue-1" || envs["SANDBOX_DISABLE_PROCESS_LOGGING"] != "true" ||
		created["labels"].(map[string]any)["temporal-harness"] != "baseten" {
		t.Fatalf("create request: %v", created)
	}
	result, err := p.ExecuteCommand(ctx, status, "exit 7")
	if err != nil || result.ExitCode != 7 || result.Stdout != "hello" || result.Stderr != "oops" {
		t.Fatalf("exec: %v %v", result, err)
	}
	if execed["command"] != "exit 7" || execed["keepAlive"] != true || execed["timeout"].(float64) < 50 {
		t.Fatalf("exec request: %v", execed)
	}
	if err := p.Suspend(ctx, status); err != nil {
		t.Fatal(err)
	}
	if err := p.Resume(ctx, status); err != nil {
		t.Fatal(err)
	}
	// Deleting what is already gone succeeds, so retried cleanup is safe.
	if err := p.Stop(ctx, status); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 7 {
		t.Fatalf("unexpected calls: %v", calls)
	}
	if _, _, err := p.Snapshot(ctx, status); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("snapshot: %v", err)
	}
	if _, err := p.StartFromSnapshot(ctx, "queue-2", &compute.ProviderSnapshot{SnapshotID: "s"}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("start from snapshot: %v", err)
	}
	if err := p.DeleteSnapshot(ctx, &compute.ProviderSnapshot{SnapshotID: "s"}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("delete snapshot: %v", err)
	}
}

func TestCommandRunsOncePerActivity(t *testing.T) {
	var name string
	posts, polls, attempts := 0, 0, 0
	var server *httptest.Server
	p, server := fakeBaseten(t, nil, func(w http.ResponseWriter, r *http.Request, route string) {
		switch {
		case route == "GET /v1/sandboxes/instances/temporal-1":
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
	polls := 0
	var server *httptest.Server
	p, server := fakeBaseten(t, nil, func(w http.ResponseWriter, r *http.Request, route string) {
		process := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sbx/process/proc-"), "/logs")
		command := ""
		if i, err := strconv.Atoi(process); err == nil && i <= len(commands) {
			command = commands[i-1]
		}
		switch {
		case route == "GET /v1/sandboxes/instances/temporal-1":
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
		default:
			t.Errorf("unexpected request %s", route)
			w.WriteHeader(http.StatusNotFound)
		}
	})
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
}

func TestRejectedCredentialsAreFinal(t *testing.T) {
	t.Setenv("BASETEN_API_KEY", "bad-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method+" "+r.URL.Path != "POST /v1/token" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"unauthorized","message":"Invalid API key"}`)
	}))
	defer server.Close()
	p, err := newProvider(nil, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := p.Start(ctx, "queue-1"); !isFinal(err, "Unauthorized") {
		t.Fatalf("start with a rejected key left to retry: %v", err)
	}
	if _, err := p.ExecuteCommand(ctx, &compute.ProviderStatus{InstanceID: "temporal-1"}, "true"); !isFinal(err, "Unauthorized") {
		t.Fatalf("command with a rejected key left to retry: %v", err)
	}
}

func TestFailedSandbox(t *testing.T) {
	var server *httptest.Server
	p, server := fakeBaseten(t, map[string]string{"team-id": "team-1"}, func(w http.ResponseWriter, r *http.Request, route string) {
		if route != "POST /v1/sandboxes/instances" || r.URL.Query().Get("team_id") != "team-1" {
			t.Errorf("unexpected request %s?%s", route, r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, sandboxJSON(server, "temporal-1", "FAILED"))
	})
	if status, err := p.Start(context.Background(), "queue-1"); err == nil {
		t.Fatalf("failed sandbox returned: %v", status)
	}
}

func TestInvalidConfigAndCredentialIsolation(t *testing.T) {
	t.Setenv("BASETEN_API_KEY", "")
	if _, err := New(nil); err == nil {
		t.Fatal("accepted a worker without an API key")
	}
	t.Setenv("BASETEN_API_KEY", "secret-from-worker")
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
