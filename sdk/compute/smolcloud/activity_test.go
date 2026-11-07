package smolcloud_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/smolcloud"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func startActivityWorkflow(ctx workflow.Context, input controller.StartSandboxInput) (controller.StartSandboxOutput, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Millisecond, MaximumAttempts: 3},
	})
	var output controller.StartSandboxOutput
	err := workflow.ExecuteActivity(ctx, controller.StartSandbox, input).Get(ctx, &output)
	return output, err
}

func TestSmolCloudFreshExistingActivityRetry(t *testing.T) {
	var requests [][]byte
	var record struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	}
	starts := 0
	state := "stopped"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/machines":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			requests = append(requests, body)
			if err := json.Unmarshal(body, &record); err != nil {
				t.Error(err)
			}
			if len(requests) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
			} else {
				w.WriteHeader(http.StatusConflict)
			}
		case "GET /v1/machines":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "activity-owned", "name": record.Name, "labels": record.Labels, "network": map[string]any{"mode": "allowCidrs", "cidrs": []string{"192.0.2.0/24", "198.51.100.0/24"}, "hosts": []string{"registry-a.example", "registry-z.example"}}}})
		case "GET /v1/machines/activity-owned":
			fmt.Fprintf(w, `{"state":%q,"ready":true}`, state)
		case "POST /v1/machines/activity-owned/start":
			starts++
			state = "running"
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	t.Setenv("SMOL_CLOUD_TOKEN", "outside-only")
	input := controller.StartSandboxInput{Provider: compute.ProviderDetails{Type: compute.ProviderTypeSmolCloud, Config: map[string]string{
		smolcloud.ConfigKeyBootstrap: "none", smolcloud.ConfigKeyLifecycle: "fresh", "image": "example/process@sha256:def", "cpus": "2", "memory-mb": "1024", "disk-gb": "4", "network": "allowCidrs", "allow-cidrs": "198.51.100.0/24,192.0.2.0/24", "allow-hosts": "registry-z.example,registry-a.example",
	}}, TaskQueueName: "activity-queue"}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(controller.StartSandbox)
	env.ExecuteWorkflow(startActivityWorkflow, input)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var output controller.StartSandboxOutput
	if err := env.GetWorkflowResult(&output); err != nil {
		t.Fatal(err)
	}
	if output.Status == nil || output.Status.InstanceID != "activity-owned" || len(requests) != 2 || !bytes.Equal(requests[0], requests[1]) || starts != 1 {
		t.Fatalf("activity retry: output=%+v requests=%q starts=%d", output, requests, starts)
	}
	var body map[string]any
	if err := json.Unmarshal(requests[0], &body); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(body["resources"], map[string]any{"cpus": float64(2), "memoryMb": float64(1024), "diskGb": float64(4)}) || !reflect.DeepEqual(body["env"], map[string]any{}) || body["branchable"] != false || !reflect.DeepEqual(body["source"], map[string]any{"type": "image", "reference": "example/process@sha256:def"}) || !reflect.DeepEqual(body["network"], map[string]any{"mode": "allowCidrs", "cidrs": []any{"198.51.100.0/24", "192.0.2.0/24"}, "hosts": []any{"registry-z.example", "registry-a.example"}}) {
		t.Fatalf("configured create: %v", body)
	}
	// Literal wire encoding fixes the binding independently of the production types.
	binding := `{"Version":2,"Queue":"activity-queue","Bootstrap":"none","Lifecycle":"fresh","Branchable":false,"Source":{"type":"image","reference":"example/process@sha256:def"},"Resources":{"cpus":2,"memoryMb":1024,"diskGb":4},"Network":{"mode":"allowCidrs","cidrs":["192.0.2.0/24","198.51.100.0/24"],"hosts":["registry-a.example","registry-z.example"]}}`
	sum := sha256.Sum256([]byte(binding))
	wantLabels := map[string]string{"temporal-harness": "smol-cloud", "temporal-harness-config": hex.EncodeToString(sum[:])}
	if !reflect.DeepEqual(record.Labels, wantLabels) {
		t.Fatalf("configured binding: %v", record.Labels)
	}
	reordered := input
	reordered.Provider.Config = maps.Clone(input.Provider.Config)
	reordered.Provider.Config["allow-cidrs"] = "192.0.2.0/24,198.51.100.0/24"
	reordered.Provider.Config["allow-hosts"] = "registry-a.example,registry-z.example"
	again, err := controller.StartSandbox(context.Background(), reordered)
	if err != nil || again.Status == nil || again.Status.InstanceID != "activity-owned" || len(requests) != 3 || starts != 1 || !reflect.DeepEqual(record.Labels, wantLabels) {
		t.Fatalf("order-only configuration change must retain binding: status=%+v err=%v labels=%v requests=%d starts=%d", again, err, record.Labels, len(requests), starts)
	}
}

func TestSmolCloudFreshDirectActivityAdmission(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(http.StatusBadRequest) }))
	defer server.Close()
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	t.Setenv("SMOL_CLOUD_TOKEN", "outside-only")
	for _, tc := range []struct {
		config     map[string]string
		diagnostic string
	}{
		{map[string]string{"lifecycle": "unknown", "image": "image"}, "lifecycle must be"},
		{map[string]string{"lifecycle": "fresh", "branchable": "true", "image": "image"}, "fresh lifecycle requires branchable=false"},
		{map[string]string{"lifecycle": "fresh"}, "image is required"},
		{map[string]string{"lifecycle": "fresh", "image": "image", "cpus": "1099511627776"}, "cpus must be"},
		{map[string]string{"lifecycle": "fresh", "image": "image", "memory-mb": "1099511627776"}, "memory-mb must be"},
	} {
		_, err := controller.StartSandbox(context.Background(), controller.StartSandboxInput{Provider: compute.ProviderDetails{Type: compute.ProviderTypeSmolCloud, Config: tc.config}, TaskQueueName: "queue"})
		if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
			t.Fatalf("configuration %v: err=%v, want %q", tc.config, err, tc.diagnostic)
		}
	}
	_, err := controller.StartSandboxFromSnapshot(context.Background(), controller.StartSandboxFromSnapshotInput{Provider: compute.ProviderDetails{Type: compute.ProviderTypeSmolCloud, Config: map[string]string{"lifecycle": "fresh", "image": "image"}}, TaskQueueName: "queue", Snapshot: &compute.ProviderSnapshot{SnapshotID: "checkpoint"}})
	if err == nil || !strings.Contains(err.Error(), "fresh lifecycle does not support restore") || calls != 0 {
		t.Fatalf("direct refusal: err=%v calls=%d", err, calls)
	}
}
