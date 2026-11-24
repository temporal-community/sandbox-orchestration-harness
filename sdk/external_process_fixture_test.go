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
	"reflect"
	"sync"
	"syscall"
)

// lifecycleFixtureAPI serves the machine calls used by the lifecycle regression. It executes real
// shell commands in one temporary directory, without container or network isolation.
type lifecycleFixtureAPI struct {
	mu                         sync.Mutex
	launch                     map[string]json.RawMessage
	machine                    map[string]any
	requests, creates, deletes int
	cmd                        *exec.Cmd
	done                       chan struct{}
	processErr                 error
	doneBeforeDelete           bool
	interruptedOnDelete        bool
	dir                        string
	stop                       context.CancelFunc
	ctx                        context.Context
}

func (a *lifecycleFixtureAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests++
	w.Header().Set("Content-Type", "application/json")
	if r.Header.Get("Authorization") != "Bearer synthetic-lifecycle-token" {
		http.Error(w, "invalid local credential", http.StatusUnauthorized)
		return
	}
	switch r.Method + " " + r.URL.Path {
	case "POST /v1/machines":
		if a.machine != nil {
			w.WriteHeader(http.StatusConflict)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&a.launch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		a.creates++
		a.machine = map[string]any{"id": "local-process", "state": "started", "ready": true}
		for _, key := range []string{"name", "labels", "network"} {
			a.machine[key] = a.launch[key]
		}
		_ = json.NewEncoder(w).Encode(a.machine)
	case "GET /v1/machines":
		_ = json.NewEncoder(w).Encode([]any{a.machine})
	case "POST /v1/machines/local-process/start":
		if a.cmd != nil {
			w.WriteHeader(http.StatusConflict)
			return
		}
		a.cmd = exec.CommandContext(a.ctx, "/bin/sh", "-c", lifecycleFixtureProcessScript)
		a.cmd.Env = []string{"PATH=/usr/bin:/bin"}
		a.cmd.Dir = a.dir
		if err := a.cmd.Start(); err != nil {
			a.cmd = nil
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		go func() { a.processErr = a.cmd.Wait(); close(a.done) }()
		w.WriteHeader(http.StatusNoContent)
	case "GET /v1/machines/local-process":
		_ = json.NewEncoder(w).Encode(a.machine)
	case "POST /v1/machines/local-process/exec":
		var body struct {
			Command []string `json:"command"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !reflect.DeepEqual(body.Command, []string{"/bin/sh", "-lc", lifecycleFixtureReadReport}) {
			http.Error(w, "unexpected command", http.StatusBadRequest)
			return
		}
		// Execute the observer shell in the process working directory.
		cmd := exec.CommandContext(r.Context(), "/bin/sh", "-c", lifecycleFixtureReadReport)
		cmd.Env, cmd.Dir = []string{"PATH=/usr/bin:/bin"}, a.dir
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		code := 0
		if err != nil {
			if cmd.ProcessState == nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			code = cmd.ProcessState.ExitCode()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"stdout": stdout.String(), "stderr": stderr.String(), "exitCode": code})
	case "DELETE /v1/machines/local-process":
		a.deletes++
		select {
		case <-a.done:
			a.doneBeforeDelete = true
		default:
			a.interruptedOnDelete = a.ctx.Err() == nil
		}
		a.stop()
		if a.cmd != nil {
			<-a.done
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unsupported local endpoint", http.StatusNotFound)
	}
}

// startLifecycleFixture creates a disposable local endpoint and process owner. The cleanup
// function is fallback cleanup; successful runs must prove controller deletion first.
func startLifecycleFixture(ctx context.Context) (*lifecycleFixtureAPI, *httptest.Server, func(), error) {
	dir, err := os.MkdirTemp("", "synthetic-lifecycle-")
	if err != nil {
		return nil, nil, nil, err
	}
	processCtx, stop := context.WithCancel(ctx)
	a := &lifecycleFixtureAPI{dir: dir, ctx: processCtx, stop: stop, done: make(chan struct{})}
	srv := httptest.NewServer(http.HandlerFunc(a.serve))
	cleanup := func() {
		stop()
		srv.Close()
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.cmd != nil {
			<-a.done
		}
		_ = os.RemoveAll(dir)
	}
	return a, srv, cleanup, nil
}

func (a *lifecycleFixtureAPI) check(scenario string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if scenario == "unsupported-profile" {
		if a.requests != 0 || a.cmd != nil {
			return "", fmt.Errorf("refusal allocated a resource: requests=%d", a.requests)
		}
		return "API requests=0", nil
	}
	if a.creates != 1 || a.deletes != 1 || a.cmd == nil {
		return "", fmt.Errorf("creates=%d deletes=%d process=%v", a.creates, a.deletes, a.cmd)
	}
	select {
	case <-a.done:
	default:
		return "", fmt.Errorf("controller returned before process reaping")
	}
	if scenario == "completion" {
		if !a.doneBeforeDelete || a.processErr != nil {
			return "", fmt.Errorf("process did not complete with exit zero before cleanup: %v", a.processErr)
		}
	} else {
		var exitErr *exec.ExitError
		if !a.interruptedOnDelete || !errors.As(a.processErr, &exitErr) {
			return "", fmt.Errorf("Stop did not interrupt a live process: %v", a.processErr)
		}
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			return "", fmt.Errorf("expected Stop SIGKILL, got %v", a.processErr)
		}
	}
	var env map[string]string
	if err := json.Unmarshal(a.launch["env"], &env); err != nil || env == nil || len(env) != 0 {
		return "", fmt.Errorf("guest environment: %s", a.launch["env"])
	}
	encoded, _ := json.Marshal(a.launch)
	for _, forbidden := range []string{"TEMPORAL_", "synthetic-lifecycle-token", "local-namespace-credential"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			return "", fmt.Errorf("launch contains %q", forbidden)
		}
	}
	return fmt.Sprintf("creates=1 deletes=1 process reaped; launch=%s", encoded), nil
}
