package sandbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/smolcloud"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const externalProcessScript = "printf process-output; exec sleep 2"

type externalProcessInput struct {
	Config map[string]string
	Cancel bool
}

// externalProcessWorkflow keeps resource ownership in the existing controller.
// It observes the bounded entrypoint through a provider command before stopping.
func externalProcessWorkflow(ctx workflow.Context, input externalProcessInput) (err error) {
	ready := false
	if err := workflow.SetQueryHandler(ctx, "external-process-ready", func() (bool, error) { return ready, nil }); err != nil {
		return err
	}
	s, err := sandbox.NewSandbox(ctx, sandbox.Provider{Type: compute.ProviderTypeSmolCloud, Config: input.Config}, sandbox.WithIdleTimeout(sandbox.NoIdleTimeout))
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		if stopErr := s.Stop(cleanup); stopErr != nil {
			err = stopErr
		}
	}()
	ready = true
	if input.Cancel {
		return workflow.Await(ctx, func() bool { return false })
	}
	result, err := s.ExecuteCommand(ctx, "wait-for-process")
	if err != nil {
		return err
	}
	if result.ExitCode != 0 || result.Stdout != "process-output" {
		return fmt.Errorf("entrypoint failed: %+v", result)
	}
	return nil
}

// TestLocalExternalProcessExample runs the real provider against a loopback API.
// The API runs a fixed local process instead of allocating cloud compute.
func TestLocalExternalProcessExample(t *testing.T) {
	if os.Getenv("HARNESS_LOCAL_TEST") != "1" {
		t.Skip("set HARNESS_LOCAL_TEST=1 and start the local server on 127.0.0.1:17293")
	}
	dockerfile, err := os.ReadFile(filepath.Join("..", "examples", "external-process", "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dockerfile), `ENTRYPOINT ["/bin/sh", "-c", "`+externalProcessScript+`"]`) {
		t.Fatal("image entrypoint differs from the local process")
	}
	for _, scenario := range []string{"completion", "cancellation", "unsupported-profile"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var mu sync.Mutex
			var launch map[string]json.RawMessage
			var machine map[string]any
			var requests []string
			var output bytes.Buffer
			var processErr error
			var cmd *exec.Cmd
			started, done := make(chan struct{}), make(chan struct{})
			creates, deletes := 0, 0
			processCtx, stopProcess := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopProcess()
			// The test fallback reaps the process even when a controller assertion fails.
			defer func() {
				stopProcess()
				if cmd != nil {
					<-done
				}
			}()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				requests = append(requests, r.Method+" "+r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				if r.Header.Get("Authorization") != "Bearer fake-provider-token" {
					t.Error("provider credential missing from outside API request")
				}
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines":
					creates++
					if err := json.NewDecoder(r.Body).Decode(&launch); err != nil {
						t.Error(err)
					}
					machine = map[string]any{"id": "local-process", "state": "started", "ready": true}
					for _, key := range []string{"name", "labels", "network"} {
						machine[key] = launch[key]
					}
					_ = json.NewEncoder(w).Encode(machine)
				case "POST /v1/machines/local-process/start":
					if cmd != nil {
						t.Error("entrypoint started twice")
						w.WriteHeader(http.StatusConflict)
						return
					}
					cmd = exec.CommandContext(processCtx, "sh", "-c", externalProcessScript)
					cmd.Env = []string{"PATH=/usr/bin:/bin"}
					cmd.Dir = t.TempDir()
					cmd.Stdout = &output
					if err := cmd.Start(); err != nil {
						cmd = nil
						t.Error(err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					go func() { processErr = cmd.Wait(); close(done) }()
					close(started)
					w.WriteHeader(http.StatusNoContent)
				case "GET /v1/machines/local-process":
					_ = json.NewEncoder(w).Encode(machine)
				case "POST /v1/machines/local-process/exec":
					var body struct {
						Command []string `json:"command"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !reflect.DeepEqual(body.Command, []string{"/bin/sh", "-lc", "wait-for-process"}) {
						t.Errorf("unexpected observer command: %+v, %v", body, err)
					}
					select {
					case <-done:
						if processErr != nil {
							t.Errorf("entrypoint: %v", processErr)
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"stdout": output.String(), "exitCode": 0})
					case <-r.Context().Done():
					}
				case "DELETE /v1/machines/local-process":
					deletes++
					stopProcess()
					if cmd != nil {
						<-done
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected fake API request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			t.Setenv("SMOL_CLOUD_URL", server.URL)
			t.Setenv("SMOL_CLOUD_TOKEN", "fake-provider-token")
			t.Setenv("TEMPORAL_API_KEY", "fake-namespace-credential")
			c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			queue := fmt.Sprintf("external-process-%s-%d", scenario, time.Now().UnixNano())
			w := worker.New(c, queue, worker.Options{})
			sandbox.Register(w, c)
			w.RegisterWorkflow(externalProcessWorkflow)
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			defer w.Stop()
			config := map[string]string{"image": "external-process:local", "bootstrap": "none", "lifecycle": "fresh", "branchable": "false", "network": "blocked"}
			if scenario == "unsupported-profile" {
				config["branchable"] = "true"
			}
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, externalProcessWorkflow, externalProcessInput{Config: config, Cancel: scenario == "cancellation"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.CancelWorkflow(context.Background(), run.GetID(), run.GetRunID())
			if scenario == "cancellation" {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					value, queryErr := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "external-process-ready")
					var ready bool
					if queryErr == nil {
						if err := value.Get(&ready); err != nil {
							t.Fatal(err)
						}
					}
					if ready {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("owner did not become ready:", ctx.Err())
					case <-ticker.C:
					}
				}
				select {
				case <-done:
					t.Fatal("process exited before cancellation")
				default:
				}
				if err := c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()); err != nil {
					t.Fatal(err)
				}
			}
			err = run.Get(ctx, nil)
			mu.Lock()
			defer mu.Unlock()
			if scenario == "unsupported-profile" {
				var app *temporal.ApplicationError
				if !errors.As(err, &app) || !strings.Contains(err.Error(), "fresh lifecycle requires branchable=false") || creates != 0 || len(requests) != 0 || cmd != nil {
					t.Fatalf("refusal must precede allocation: err=%v requests=%v", err, requests)
				}
				t.Logf("refused before allocation: %v; API requests=0", err)
				return
			}
			if scenario == "cancellation" {
				if !temporal.IsCanceledError(err) {
					t.Fatalf("expected cancellation: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if creates != 1 || deletes != 1 || cmd == nil {
				t.Fatalf("creates=%d deletes=%d process=%v", creates, deletes, cmd)
			}
			select {
			case <-done:
			default:
				t.Fatal("controller returned before the local process was reaped")
			}
			if scenario == "completion" && (processErr != nil || output.String() != "process-output") {
				t.Fatalf("output=%q error=%v", output.String(), processErr)
			}
			if scenario == "cancellation" && processErr == nil {
				t.Fatal("cancellation did not interrupt the process")
			}
			var guestEnv map[string]string
			if err := json.Unmarshal(launch["env"], &guestEnv); err != nil || guestEnv == nil || len(guestEnv) != 0 {
				t.Fatalf("guest environment: %s, %v", launch["env"], err)
			}
			encoded, _ := json.Marshal(launch)
			for _, forbidden := range []string{"TEMPORAL_", "fake-provider-token", "fake-namespace-credential"} {
				if bytes.Contains(encoded, []byte(forbidden)) {
					t.Fatalf("launch contains %q", forbidden)
				}
			}
			parent := readHistory(t, ctx, c, run.GetID(), run.GetRunID())
			var childID, childRun string
			children := 0
			for _, event := range parent.Events {
				if a := event.GetChildWorkflowExecutionStartedEventAttributes(); a != nil {
					children++
					childID, childRun = a.WorkflowExecution.WorkflowId, a.WorkflowExecution.RunId
				}
			}
			if children != 1 {
				t.Fatalf("resource controllers=%d", children)
			}
			child := readHistory(t, ctx, c, childID, childRun)
			stops := 0
			for _, name := range scheduledActivityNames(child) {
				if name == "StopSandbox" {
					stops++
				}
			}
			if stops != 1 {
				t.Fatalf("controller stop Activities=%d", stops)
			}
			for _, event := range child.Events {
				if event.GetTimerStartedEventAttributes() != nil {
					t.Fatal("NoIdleTimeout scheduled an idle timer")
				}
			}
			t.Logf("%s: controllers=1 creates=1 deletes=1 process reaped; launch=%s", scenario, encoded)
		})
	}
}
