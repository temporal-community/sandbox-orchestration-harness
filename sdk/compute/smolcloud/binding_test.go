package smolcloud

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"reflect"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
)

func boundStatus(p *provider) *compute.ProviderStatus {
	return &compute.ProviderStatus{InstanceID: "owned", ResourceBinding: &compute.ResourceBinding{
		ProviderType: providerType, TaskQueueName: "queue", InstanceID: "owned", ConfigurationDigest: p.configurationLabel("queue"),
	}}
}

func TestBoundProviderCommandAndStopMetadata(t *testing.T) {
	for _, operation := range []string{"command", "stop"} {
		for _, conflict := range []string{"none", "absent", "id", "name", "ownership", "configuration", "network", "missing-metadata"} {
			t.Run(operation+"/"+conflict, func(t *testing.T) {
				var status *compute.ProviderStatus
				var p *provider
				var requests []string
				fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
					requests = append(requests, r.Method+" "+r.URL.Path)
					if r.Method == http.MethodGet {
						if conflict == "absent" {
							w.WriteHeader(http.StatusNotFound)
							return
						}
						name, _ := machineName("queue", "")
						labels := map[string]string{"temporal-harness": "smol-cloud", "temporal-harness-config": status.ResourceBinding.ConfigurationDigest}
						machine := map[string]any{"id": "owned", "name": name, "labels": labels, "network": p.networkPolicy()}
						switch conflict {
						case "id", "name":
							machine[conflict] = "other"
						case "ownership":
							labels["temporal-harness"] = "other"
						case "configuration":
							labels["temporal-harness-config"] = "other"
						case "network":
							machine["network"] = networkPolicy{Mode: "open"}
						case "missing-metadata":
							machine = map[string]any{}
						}
						_ = json.NewEncoder(w).Encode(machine)
						return
					}
					if operation == "command" && r.Method == http.MethodPost && r.URL.Path == "/v1/machines/owned/exec" {
						_, _ = w.Write([]byte(`{"stdout":"ok","exitCode":0}`))
						return
					}
					if operation == "stop" && r.Method == http.MethodDelete && r.URL.Path == "/v1/machines/owned" {
						w.WriteHeader(http.StatusNoContent)
						return
					}
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				})
				p = newTestProvider(t, freshConfig()).(*provider)
				status = boundStatus(p)
				var err error
				if operation == "command" {
					var result *compute.CommandResult
					result, err = p.ExecuteCommand(context.Background(), status, "true")
					if err == nil && result.Stdout != "ok" {
						t.Fatalf("command output: %+v", result)
					}
				} else {
					err = p.Stop(context.Background(), status)
				}
				wantSuccess := conflict == "none" || (operation == "stop" && conflict == "absent")
				if (err == nil) != wantSuccess {
					t.Fatalf("result: %v", err)
				}
				want := []string{"GET /v1/machines/owned"}
				if conflict == "none" {
					if operation == "command" {
						want = append(want, "POST /v1/machines/owned/exec")
					} else {
						want = append(want, "DELETE /v1/machines/owned")
					}
				}
				if !reflect.DeepEqual(requests, want) {
					t.Fatalf("requests: %v, want %v", requests, want)
				}
			})
		}
	}
}

func TestBoundProviderRefusesLostOrConflictingContextBeforeHTTP(t *testing.T) {
	calls := 0
	fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		t.Errorf("unexpected HTTP: %s %s", r.Method, r.URL)
	})
	p := newTestProvider(t, freshConfig()).(*provider)
	for _, change := range []string{"nil-status", "missing-binding", "provider", "instance", "queue", "digest", "incomplete", "lost-config", "changed-lifecycle-and-digest", "changed-image", "changed-bootstrap", "changed-resources", "changed-network"} {
		t.Run(change, func(t *testing.T) {
			status := boundStatus(p)
			config := maps.Clone(freshConfig())
			switch change {
			case "nil-status":
				status = nil
			case "missing-binding":
				status.ResourceBinding = nil
			case "provider":
				status.ResourceBinding.ProviderType = compute.ProviderTypeDaytona
			case "instance":
				status.InstanceID = "other"
			case "queue":
				status.ResourceBinding.TaskQueueName = "other"
			case "digest":
				status.ResourceBinding.ConfigurationDigest = "other"
			case "incomplete":
				status.ResourceBinding.TaskQueueName = ""
			case "lost-config":
				config = map[string]string{"image": config["image"]}
			case "changed-lifecycle-and-digest":
				config[ConfigKeyLifecycle] = "standard"
			case "changed-image":
				config["image"] = "other"
			case "changed-bootstrap":
				config[ConfigKeyBootstrap] = "worker"
			case "changed-resources":
				config["cpus"] = "2"
			case "changed-network":
				config["network"] = "open"
			}
			selected := newTestProvider(t, config)
			if change == "changed-lifecycle-and-digest" {
				status.ResourceBinding.ConfigurationDigest = selected.(*provider).configurationLabel("queue")
			}
			if _, err := selected.ExecuteCommand(context.Background(), status, "true"); err == nil {
				t.Fatal("command accepted conflicting context")
			}
			if err := selected.Stop(context.Background(), status); err == nil {
				t.Fatal("stop accepted conflicting context")
			}
			if err := selected.Suspend(context.Background(), status); err == nil {
				t.Fatal("suspend accepted conflicting context")
			}
			if err := selected.Resume(context.Background(), status); err == nil {
				t.Fatal("resume accepted conflicting context")
			}
			if _, _, err := selected.Snapshot(context.Background(), status); err == nil {
				t.Fatal("snapshot accepted conflicting context")
			}
		})
	}
	if calls != 0 {
		t.Fatalf("conflicts reached HTTP: %d", calls)
	}
}

func TestDirectActivitiesRefuseBoundContextLoss(t *testing.T) {
	calls := 0
	fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) { calls++; t.Errorf("unexpected HTTP") })
	p := newTestProvider(t, freshConfig()).(*provider)
	for _, change := range []string{"nil-status", "missing", "provider", "instance", "incomplete", "nil-config", "empty-config", "lost-config", "changed-image"} {
		t.Run(change, func(t *testing.T) {
			status := boundStatus(p)
			details := compute.ProviderDetails{Type: providerType, Config: maps.Clone(freshConfig())}
			switch change {
			case "nil-status":
				status = nil
			case "missing":
				status.ResourceBinding = nil
			case "provider":
				status.ResourceBinding.ProviderType = compute.ProviderTypeDaytona
			case "instance":
				status.InstanceID = "other"
			case "incomplete":
				status.ResourceBinding.ConfigurationDigest = ""
			case "lost-config":
				details.Config = map[string]string{"image": details.Config["image"]}
			case "nil-config":
				details.Config = nil
			case "empty-config":
				details.Config = map[string]string{}
			case "changed-image":
				details.Config["image"] = "other"
			}
			ctx := context.Background()
			if _, err := controller.ExecuteCommand(ctx, controller.ExecuteCommandInput{Provider: details, Status: status, Command: "true"}); err == nil {
				t.Fatal("command accepted invalid binding")
			}
			if err := controller.StopSandbox(ctx, controller.StopSandboxInput{Provider: details, Status: status}); err == nil {
				t.Fatal("stop accepted invalid binding")
			}
			if err := controller.SuspendSandbox(ctx, controller.SuspendSandboxInput{Provider: details, Status: status}); err == nil {
				t.Fatal("suspend accepted invalid binding")
			}
			if err := controller.ResumeSandbox(ctx, controller.ResumeSandboxInput{Provider: details, Status: status}); err == nil {
				t.Fatal("resume accepted invalid binding")
			}
			if _, err := controller.SnapshotSandbox(ctx, controller.SnapshotSandboxInput{Provider: details, Status: status}); err == nil {
				t.Fatal("snapshot accepted invalid binding")
			}
		})
	}
	if calls != 0 {
		t.Fatalf("invalid Activity context reached HTTP: %d", calls)
	}
}

func TestResourceBindingAbsentForLegacyWire(t *testing.T) {
	encoded, err := json.Marshal(&compute.ProviderStatus{InstanceID: "legacy"})
	if err != nil || string(encoded) != `{"instance_id":"legacy"}` {
		t.Fatalf("legacy wire: %s %v", encoded, err)
	}
}

func TestResourceBindingIssuedOnlyForFreshStarts(t *testing.T) {
	for _, config := range []map[string]string{
		{"image": "image"},
		{"image": "image", "bootstrap": "none"},
		{"image": "image", "lifecycle": "standard", "branchable": "false"},
		{"image": "image", "bootstrap": "worker", "lifecycle": "fresh"},
		{"image": "image", "bootstrap": "none", "lifecycle": "fresh"},
	} {
		t.Run(config["bootstrap"]+"/"+config["lifecycle"]+"/"+config["branchable"], func(t *testing.T) {
			fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines":
					_, _ = w.Write([]byte(`{"id":"owned","network":{"mode":"blocked"}}`))
				case "POST /v1/machines/owned/start":
					w.WriteHeader(http.StatusNoContent)
				case "GET /v1/machines/owned":
					_, _ = w.Write([]byte(`{"ready":true,"state":"running"}`))
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
			})
			p := newTestProvider(t, config).(*provider)
			status, err := p.Start(context.Background(), "queue")
			if err != nil {
				t.Fatal(err)
			}
			if config["lifecycle"] == "fresh" {
				if !reflect.DeepEqual(status, boundStatus(p)) {
					t.Fatalf("fresh binding: %+v", status)
				}
			} else if status.ResourceBinding != nil {
				t.Fatalf("legacy status gains binding: %+v", status)
			}
		})
	}
}
