package smolcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/sdk/temporal"
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
				if errors.Is(err, compute.ErrInvalidResourceBinding) {
					t.Fatalf("remote metadata refusal classified as local binding failure: %v", err)
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
	for _, change := range []string{"nil-status", "empty-id", "missing-binding", "provider", "instance", "queue", "digest", "incomplete", "lost-config", "changed-lifecycle-and-digest", "changed-image", "changed-bootstrap", "changed-resources", "changed-network"} {
		t.Run(change, func(t *testing.T) {
			status := boundStatus(p)
			config := maps.Clone(freshConfig())
			switch change {
			case "nil-status":
				status = nil
			case "empty-id":
				status.InstanceID = ""
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
			for _, invoke := range []func() error{
				func() error { _, err := selected.ExecuteCommand(context.Background(), status, "true"); return err },
				func() error { return selected.Stop(context.Background(), status) },
			} {
				err := invoke()
				wantTagged := change != "lost-config" && change != "changed-lifecycle-and-digest"
				if err == nil || errors.Is(fmt.Errorf("outer: %w", err), compute.ErrInvalidResourceBinding) != wantTagged {
					t.Fatalf("local refusal identity: %v, want tagged=%t", err, wantTagged)
				}
				if (change == "nil-status" || change == "empty-id") && !strings.Contains(err.Error(), "machine id is required") {
					t.Fatalf("machine ID must fail first: %v", err)
				}
			}
			for _, invoke := range []func() error{
				func() error { return selected.Suspend(context.Background(), status) },
				func() error { return selected.Resume(context.Background(), status) },
				func() error { _, _, err := selected.Snapshot(context.Background(), status); return err },
			} {
				err := invoke()
				if err == nil || (selected.(*provider).policy.Lifecycle == compute.LifecycleFresh && !errors.Is(err, errors.ErrUnsupported)) {
					t.Fatalf("fresh unsupported operation must fail before binding: %v", err)
				}
			}
		})
	}
	if calls != 0 {
		t.Fatalf("conflicts reached HTTP: %d", calls)
	}
}

func TestBoundActivityRemoteRetryEligibility(t *testing.T) {
	for _, operation := range []string{"command", "stop"} {
		for _, failure := range []string{"none", "metadata", "get-429", "get-503", "get-decode", "effect-429", "effect-503", "absent", "delete-absent", "canceled"} {
			if operation == "command" && failure == "delete-absent" {
				continue
			}
			t.Run(operation+"/"+failure, func(t *testing.T) {
				var p *provider
				var requests []string
				fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
					requests = append(requests, r.Method)
					if r.Method == http.MethodGet {
						switch failure {
						case "get-429":
							w.WriteHeader(http.StatusTooManyRequests)
							return
						case "get-503":
							w.WriteHeader(http.StatusServiceUnavailable)
							return
						case "absent":
							w.WriteHeader(http.StatusNotFound)
							return
						case "get-decode":
							_, _ = w.Write([]byte("invalid-json"))
							return
						}
						name, _ := machineName("queue", "")
						if failure == "metadata" {
							name = "other"
						}
						_ = json.NewEncoder(w).Encode(machineDetails{
							ID:      "owned",
							Name:    name,
							Labels:  map[string]string{"temporal-harness": "smol-cloud", "temporal-harness-config": p.configurationLabel("queue")},
							Network: p.networkPolicy(),
						})
						return
					}
					switch failure {
					case "effect-429":
						w.WriteHeader(http.StatusTooManyRequests)
					case "effect-503":
						w.WriteHeader(http.StatusServiceUnavailable)
					case "delete-absent":
						w.WriteHeader(http.StatusNotFound)
					default:
						if operation == "command" {
							_, _ = w.Write([]byte(`{"stdout":"ok","exitCode":0}`))
						} else {
							w.WriteHeader(http.StatusNoContent)
						}
					}
				})
				p = newTestProvider(t, freshConfig()).(*provider)
				details := compute.ProviderDetails{Type: providerType, Config: freshConfig()}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if failure == "canceled" {
					cancel()
				}
				var err error
				if operation == "command" {
					_, err = controller.ExecuteCommand(ctx, controller.ExecuteCommandInput{Provider: details, Status: boundStatus(p), Command: "true"})
				} else {
					err = controller.StopSandbox(ctx, controller.StopSandboxInput{Provider: details, Status: boundStatus(p)})
				}
				wantSuccess := failure == "none" || (operation == "stop" && (failure == "absent" || failure == "delete-absent"))
				var app *temporal.ApplicationError
				if (err == nil) != wantSuccess || errors.Is(err, compute.ErrInvalidResourceBinding) || errors.As(err, &app) {
					t.Fatalf("remote result lost retry eligibility: %v", err)
				}
				want := []string{http.MethodGet}
				if failure == "none" || strings.HasPrefix(failure, "effect-") || failure == "delete-absent" {
					method := http.MethodPost
					if operation == "stop" {
						method = http.MethodDelete
					}
					want = append(want, method)
				}
				if failure == "canceled" {
					want = nil
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation identity: %v", err)
					}
				}
				if !reflect.DeepEqual(requests, want) {
					t.Fatalf("requests=%v, want %v", requests, want)
				}
			})
		}
	}
}

func TestBoundActivityLocalErrorClassification(t *testing.T) {
	requests := 0
	fakeEndpoint(t, func(http.ResponseWriter, *http.Request) { requests++; t.Error("local refusal reached HTTP") })
	p := newTestProvider(t, freshConfig()).(*provider)
	for _, fresh := range []bool{false, true} {
		for _, change := range []string{"nil-status", "empty-id", "missing-binding", "digest"} {
			// Legacy provider tests cover valid unbound standard status.
			if !fresh && change == "missing-binding" {
				continue
			}
			t.Run(fmt.Sprintf("fresh=%t/%s", fresh, change), func(t *testing.T) {
				config := maps.Clone(freshConfig())
				if !fresh {
					config[ConfigKeyLifecycle] = "standard"
				}
				status := boundStatus(p)
				switch change {
				case "nil-status":
					status = nil
				case "empty-id":
					status = &compute.ProviderStatus{}
				case "missing-binding":
					status.ResourceBinding = nil
				case "digest":
					status.ResourceBinding.ConfigurationDigest = "other"
				}
				details := compute.ProviderDetails{Type: providerType, Config: config}
				for _, invoke := range []func() error{
					func() error {
						return controller.StopSandbox(context.Background(), controller.StopSandboxInput{Provider: details, Status: status})
					},
					func() error {
						_, err := controller.ExecuteCommand(context.Background(), controller.ExecuteCommandInput{Provider: details, Status: status})
						return err
					},
				} {
					err := invoke()
					var app *temporal.ApplicationError
					if err == nil || errors.Is(err, compute.ErrInvalidResourceBinding) != fresh {
						t.Fatalf("local classification: %v", err)
					}
					if fresh {
						if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != "ProviderConfigError" || !errors.Is(app.Unwrap(), compute.ErrInvalidResourceBinding) {
							t.Fatalf("fresh Activity classification: %v", err)
						}
					} else if errors.As(err, &app) {
						t.Fatalf("standard refusal lost retry eligibility: %v", err)
					}
				}
			})
		}
	}
	if requests != 0 {
		t.Fatalf("local refusals made %d requests", requests)
	}
}

type bindingTransportFunc func(*http.Request) (*http.Response, error)

func (f bindingTransportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestBoundProviderTransportError(t *testing.T) {
	fakeEndpoint(t, func(http.ResponseWriter, *http.Request) { t.Error("custom transport reached HTTP") })
	p := newTestProvider(t, freshConfig()).(*provider)
	transportErr := errors.New("transport unavailable")
	requests := 0
	p.client.Transport = bindingTransportFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return nil, transportErr
	})
	for _, invoke := range []func() error{
		func() error { return p.Stop(context.Background(), boundStatus(p)) },
		func() error { _, err := p.ExecuteCommand(context.Background(), boundStatus(p), "true"); return err },
	} {
		err := invoke()
		if !errors.Is(err, transportErr) || errors.Is(err, compute.ErrInvalidResourceBinding) {
			t.Fatalf("transport identity or retry eligibility changed: %v", err)
		}
	}
	if requests != 2 {
		t.Fatalf("transport requests=%d, want 2", requests)
	}
}

func TestBoundActivityConstructorErrorPrecedence(t *testing.T) {
	for _, failure := range []string{"image", "url", "credential"} {
		t.Run(failure, func(t *testing.T) {
			requests := 0
			fakeEndpoint(t, func(http.ResponseWriter, *http.Request) { requests++; t.Error("constructor refusal reached HTTP") })
			config := maps.Clone(freshConfig())
			want := ""
			switch failure {
			case "image":
				delete(config, "image")
				want = "image"
			case "url":
				t.Setenv("SMOL_CLOUD_URL", "://invalid")
				want = "invalid SMOL_CLOUD_URL"
			case "credential":
				t.Setenv("SMOL_CLOUD_TOKEN", "")
				want = "set SMOL_CLOUD_TOKEN"
			}
			details := compute.ProviderDetails{Type: providerType, Config: config}
			for _, malformed := range []bool{false, true} {
				status := &compute.ProviderStatus{InstanceID: "owned"}
				if malformed {
					status.ResourceBinding = &compute.ResourceBinding{}
				}
				for _, invoke := range []func() error{
					func() error {
						return controller.StopSandbox(context.Background(), controller.StopSandboxInput{Provider: details, Status: status})
					},
					func() error {
						_, err := controller.ExecuteCommand(context.Background(), controller.ExecuteCommandInput{Provider: details, Status: status})
						return err
					},
				} {
					err := invoke()
					var app *temporal.ApplicationError
					if !errors.As(err, &app) || app.Type() != "ProviderConfigError" || !app.NonRetryable() {
						t.Fatalf("constructor classification: %v", err)
					}
					if malformed {
						if !errors.Is(err, compute.ErrInvalidResourceBinding) || !strings.Contains(err.Error(), "incomplete resource binding") {
							t.Fatalf("supplied binding must fail before constructor: %v", err)
						}
					} else if errors.Is(err, compute.ErrInvalidResourceBinding) || !strings.Contains(err.Error(), want) {
						t.Fatalf("constructor must fail before missing binding: %v, want %s", err, want)
					}
				}
			}
			if requests != 0 {
				t.Fatalf("constructor failures made %d requests", requests)
			}
		})
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
