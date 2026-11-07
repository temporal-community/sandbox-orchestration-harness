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
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

// freshConfig selects a process without Worker bootstrap under the fresh lifecycle.
func freshConfig() map[string]string {
	return map[string]string{ConfigKeyBootstrap: "none", ConfigKeyLifecycle: "fresh", "image": "example/process@sha256:abc", "network": "blocked"}
}

// fakeEndpoint always replaces both provider credentials and the endpoint.
// A missing fake route fails the test instead of reaching a cloud service.
func fakeEndpoint(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer outside-only" {
			t.Error("provider credential missing from control request")
		}
		w.Header().Set("Content-Type", "application/json")
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	t.Setenv("SMOL_CLOUD_URL", server.URL)
	t.Setenv("SMOL_CLOUD_TOKEN", "outside-only")
}

func newTestProvider(t *testing.T, config map[string]string) compute.Provider {
	t.Helper()
	p, err := compute.Lookup(compute.ProviderTypeSmolCloud, config)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFreshRequestAndLostCreateReply(t *testing.T) {
	var bodies [][]byte
	var record createMachineRequest
	starts, deletes := 0, 0
	state := "stopped"
	fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/machines":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			bodies = append(bodies, body)
			if err := json.Unmarshal(body, &record); err != nil {
				t.Error(err)
			}
			if len(bodies) == 1 {
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = connection.Close()
				return
			}
			w.WriteHeader(http.StatusConflict)
		case "GET /v1/machines":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": "unrelated", "name": "another-name", "labels": record.Labels, "network": record.Network},
				{"id": "original", "name": record.Name, "labels": record.Labels, "network": record.Network},
			})
		case "GET /v1/machines/unrelated":
			fmt.Fprint(w, `{"state":"running","ready":true}`)
		case "GET /v1/machines/original":
			fmt.Fprintf(w, `{"state":%q,"ready":true}`, state)
		case "POST /v1/machines/original/start":
			if r.URL.RawQuery != "" {
				t.Errorf("fresh start enables branching: %s", r.URL)
			}
			starts++
			state = "running"
			w.WriteHeader(http.StatusNoContent)
		case "DELETE /v1/machines/original":
			deletes++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	t.Setenv("TEMPORAL_NAMESPACE", "namespace-canary")
	t.Setenv("TEMPORAL_API_KEY", "worker-key-canary")
	t.Setenv("TEMPORAL_TASK_QUEUE", "ambient-queue-canary")
	p := newTestProvider(t, freshConfig())
	if status, err := p.Start(context.Background(), "process-queue"); err == nil || status != nil {
		t.Fatalf("lost reply must fail the attempt: %v %v", status, err)
	}
	for i := 0; i < 2; i++ {
		status, err := p.Start(context.Background(), "process-queue")
		if err != nil || status == nil || status.InstanceID != "original" {
			t.Fatalf("retry: %v %v", status, err)
		}
	}
	if len(bodies) != 3 || !bytes.Equal(bodies[0], bodies[1]) || !bytes.Equal(bodies[0], bodies[2]) || starts != 1 || deletes != 0 {
		t.Fatalf("retry changed request or resource: bodies=%q starts=%d deletes=%d", bodies, starts, deletes)
	}
	wantName, _ := machineName("process-queue", "")
	if record.Name != wantName || record.Source != (machineSource{"image", "example/process@sha256:abc"}) || record.Resources != (machineResources{1, 512, 0}) || !reflect.DeepEqual(record.Network, networkPolicy{Mode: "blocked"}) || record.Branchable || record.Env == nil || len(record.Env) != 0 {
		t.Fatalf("unexpected fresh create request: %+v", record)
	}
	// Literal wire encoding fixes the binding independently of the production types.
	binding := `{"Version":2,"Queue":"process-queue","Bootstrap":"none","Lifecycle":"fresh","Branchable":false,"Source":{"type":"image","reference":"example/process@sha256:abc"},"Resources":{"cpus":1,"memoryMb":512},"Network":{"mode":"blocked"}}`
	sum := sha256.Sum256([]byte(binding))
	if record.Labels["temporal-harness"] != "smol-cloud" || record.Labels["temporal-harness-config"] != hex.EncodeToString(sum[:]) || len(record.Labels) != 2 {
		t.Fatalf("missing binding: %v", record.Labels)
	}
	for _, forbidden := range []string{"TEMPORAL_", "outside-only", "namespace-canary", "worker-key-canary", "ambient-queue-canary"} {
		if bytes.Contains(bodies[0], []byte(forbidden)) {
			t.Fatalf("bootstrap or credential in body: %q", forbidden)
		}
	}
	if err := p.Stop(context.Background(), &compute.ProviderStatus{InstanceID: "original"}); err != nil || deletes != 1 {
		t.Fatalf("stop: %v deletes=%d", err, deletes)
	}
}

func TestFreshConfigurationAdmission(t *testing.T) {
	calls := 0
	fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(http.StatusBadRequest) })
	for _, tc := range []struct{ key, value, diagnostic string }{
		{"bootstrap", "unknown", "bootstrap must be"},
		{"lifecycle", "Fresh", "lifecycle must be"},
		{"branchable", "true", "fresh lifecycle requires branchable=false"},
		{"image", "", "image is required"},
		{"image", "  ", "image is required"},
		{"network", "unknown", "network must be"},
		{"network", "allowCidrs", "allowCidrs requires"},
		{"cpus", "0", "cpus must be"},
		{"cpus", "999999999999999999999999999999", "cpus must be"},
		{"cpus", "2147483648", "cpus must be"},
		{"cpus", "1099511627776", "cpus must be"},
		{"memory-mb", "-1", "memory-mb must be"},
		{"memory-mb", "2147483648", "memory-mb must be"},
		{"memory-mb", "1099511627776", "memory-mb must be"},
		{"disk-gb", "text", "disk-gb must be"},
		{"disk-gb", "0", "disk-gb must be"},
		{"disk-gb", "-1", "disk-gb must be"},
		{"disk-gb", "999999999999999999999999999999", "disk-gb must be"},
		{"allow-hosts", "localhost", "invalid host"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			config := freshConfig()
			config[tc.key] = tc.value
			if tc.key == "allow-hosts" {
				config["network"] = "allowCidrs"
			}
			if _, err := New(config); err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("admission: %v", err)
			}
		})
	}
	p := newTestProvider(t, freshConfig())
	if _, err := p.Start(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "task queue is required") {
		t.Fatalf("identity: %v", err)
	}
	for _, snapshot := range []*compute.ProviderSnapshot{nil, {}, {SnapshotID: "checkpoint"}} {
		if _, err := p.StartFromSnapshot(context.Background(), "queue", snapshot); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "fresh lifecycle does not support restore") {
			t.Fatalf("restore: %v", err)
		}
	}
	if calls != 0 {
		t.Fatalf("refused configuration reached endpoint %d times", calls)
	}
	for _, config := range []map[string]string{
		{"image": "legacy", "opaque-future-key": "accepted"},
		{"image": "image", "lifecycle": "fresh", "opaque-future-key": "accepted"},
		{"image": "image", "lifecycle": "fresh", "cpus": "2147483647", "memory-mb": "2147483647", "disk-gb": "1099511627776"},
		{"image": "legacy", "cpus": "2147483647", "memory-mb": "2147483647"},
	} {
		if _, err := New(config); err != nil {
			t.Fatalf("valid configuration %v rejected: %v", config, err)
		}
	}
}

func TestFreshAdoptionRefusals(t *testing.T) {
	for _, tc := range []struct{ name, diagnostic string }{
		{"legacy", "configuration differs"}, {"image", "configuration differs"},
		{"cpus", "configuration differs"}, {"memory-mb", "configuration differs"}, {"disk-gb", "configuration differs"},
		{"queue", "configuration differs"}, {"owner", "taken by another machine"}, {"id", "taken by another machine"},
		{"missing", "exactly one"}, {"duplicate", "exactly one"}, {"foreign-duplicate", "exactly one"},
		{"network", "network policy differs"}, {"cidrs", "network policy differs"}, {"hosts", "network policy differs"},
		{"cidr-values", "network policy differs"}, {"host-values", "network policy differs"},
		{"paused", "cannot adopt paused machine: compute: fresh lifecycle does not support resume"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var request createMachineRequest
			mutations := 0
			fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines":
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					w.WriteHeader(http.StatusConflict)
				case "GET /v1/machines":
					labels := maps.Clone(request.Labels)
					id, network := "conflict", request.Network
					switch tc.name {
					case "legacy":
						delete(labels, "temporal-harness-config")
					case "owner":
						labels["temporal-harness"] = "foreign"
					case "id":
						id = ""
					case "image", "cpus", "memory-mb", "disk-gb", "queue":
						config := freshConfig()
						queue := "queue"
						if tc.name == "queue" {
							queue = "other-queue"
						} else if tc.name == "image" {
							config[tc.name] = "other-image"
						} else {
							config[tc.name] = "2"
						}
						other := newTestProvider(t, config).(*provider)
						labels["temporal-harness-config"] = other.configurationLabel(queue)
					case "network":
						network.Mode = "open"
					case "cidrs":
						network.CIDRs = []string{"192.0.2.0/24"}
					case "hosts":
						network.Hosts = []string{"unexpected.example"}
					case "cidr-values":
						network.CIDRs = []string{"198.51.100.0/24"}
					case "host-values":
						network.Hosts = []string{"unexpected.example"}
					}
					rows := []map[string]any{{"id": id, "name": request.Name, "labels": labels, "network": network}}
					if tc.name == "missing" {
						rows = nil
					}
					if tc.name == "duplicate" {
						rows = append(rows, rows[0])
					}
					if tc.name == "foreign-duplicate" {
						rows = append(rows, map[string]any{"id": "foreign", "name": request.Name})
					}
					_ = json.NewEncoder(w).Encode(rows)
				case "GET /v1/machines/conflict":
					fmt.Fprintf(w, `{"state":%q,"ready":true}`, tc.name)
				default:
					mutations++
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			config := freshConfig()
			if tc.name == "cidr-values" || tc.name == "host-values" {
				config["network"] = "allowCidrs"
				config["allow-cidrs"] = "192.0.2.0/24"
				config["allow-hosts"] = "expected.example"
			}
			p := newTestProvider(t, config)
			status, err := p.Start(context.Background(), "queue")
			if err == nil || status != nil || !strings.Contains(err.Error(), tc.diagnostic) || mutations != 0 {
				t.Fatalf("conflict must remain untouched: status=%v err=%v mutations=%d", status, err, mutations)
			}
		})
	}
}

// TestFreshRetryStartsOwnedMachine proves that only the policy restricts fresh adoption:
// a retry starts its own machine from any state except paused, as standard lifecycle does.
func TestFreshRetryStartsOwnedMachine(t *testing.T) {
	for _, tc := range []struct {
		state   string
		succeed bool
	}{{"stopped", true}, {"created", true}, {"starting", true}, {"failed", false}} {
		t.Run(tc.state, func(t *testing.T) {
			var request createMachineRequest
			state, starts, deletes := tc.state, 0, 0
			fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines":
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					w.WriteHeader(http.StatusConflict)
				case "GET /v1/machines":
					_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "owned", "name": request.Name, "labels": request.Labels, "network": request.Network}})
				case "GET /v1/machines/owned":
					fmt.Fprintf(w, `{"state":%q,"ready":%t}`, state, state == "running")
				case "POST /v1/machines/owned/start":
					starts++
					if r.URL.RawQuery != "" {
						t.Errorf("fresh start enables branching: %s", r.URL)
					}
					if tc.succeed {
						state = "running"
					}
					w.WriteHeader(http.StatusNoContent)
				case "DELETE /v1/machines/owned":
					deletes++
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			status, err := newTestProvider(t, freshConfig()).Start(context.Background(), "queue")
			if tc.succeed {
				if err != nil || status == nil || status.InstanceID != "owned" || starts != 1 || deletes != 0 {
					t.Fatalf("adoption from %s: status=%v err=%v starts=%d deletes=%d", tc.state, status, err, starts, deletes)
				}
			} else if err == nil || status != nil || !strings.Contains(err.Error(), "entered failed") || starts != 1 || deletes != 0 {
				t.Fatalf("failed machine: status=%v err=%v starts=%d deletes=%d", status, err, starts, deletes)
			}
		})
	}
}

func TestFreshOwnedCreateCleanup(t *testing.T) {
	for _, failure := range []string{"network", "start", "readiness", "decode"} {
		t.Run(failure, func(t *testing.T) {
			deletes, starts := 0, 0
			fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines":
					if failure == "decode" {
						fmt.Fprint(w, `{"id":"owned","network":{"mode":123}}`)
						return
					}
					mode := "blocked"
					if failure == "network" {
						mode = "open"
					}
					fmt.Fprintf(w, `{"id":"owned","network":{"mode":%q}}`, mode)
				case "POST /v1/machines/owned/start":
					starts++
					if failure == "start" {
						w.WriteHeader(http.StatusBadRequest)
					} else {
						w.WriteHeader(http.StatusNoContent)
					}
				case "GET /v1/machines/owned":
					fmt.Fprint(w, `{"state":"failed","ready":false}`)
				case "DELETE /v1/machines/owned":
					deletes++
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			p := newTestProvider(t, freshConfig())
			if _, err := p.Start(context.Background(), "queue"); err == nil || deletes != 1 || ((failure == "network" || failure == "decode") && starts != 0) || (failure == "decode" && !strings.Contains(err.Error(), "decode POST /v1/machines")) {
				t.Fatalf("owned cleanup: err=%v deletes=%d starts=%d", err, deletes, starts)
			}
		})
	}
}

func TestFreshCreateRequiresIdentity(t *testing.T) {
	followups := 0
	fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/machines" {
			fmt.Fprint(w, `{"network":{"mode":"blocked"}}`)
			return
		}
		followups++
		w.WriteHeader(http.StatusBadRequest)
	})
	status, err := newTestProvider(t, freshConfig()).Start(context.Background(), "queue")
	if status != nil || err == nil || !strings.Contains(err.Error(), "create returned no machine id") || followups != 0 {
		t.Fatalf("missing identity must refuse before follow-up requests: status=%v err=%v followups=%d", status, err, followups)
	}
}

func TestFreshPausedDuringReadiness(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		t.Run(fmt.Sprintf("recovered=%t", recovered), func(t *testing.T) {
			var request createMachineRequest
			reads, deletes, starts := 0, 0, 0
			fakeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines":
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if recovered {
						w.WriteHeader(http.StatusConflict)
					} else {
						fmt.Fprint(w, `{"id":"owned","network":{"mode":"blocked"}}`)
					}
				case "GET /v1/machines":
					_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "owned", "name": request.Name, "labels": request.Labels, "network": request.Network}})
				case "GET /v1/machines/owned":
					reads++
					if recovered && reads == 1 {
						fmt.Fprint(w, `{"state":"running","ready":true}`)
					} else {
						fmt.Fprint(w, `{"state":"paused","ready":true}`)
					}
				case "POST /v1/machines/owned/start":
					starts++
					w.WriteHeader(http.StatusNoContent)
				case "DELETE /v1/machines/owned":
					deletes++
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			status, err := newTestProvider(t, freshConfig()).Start(context.Background(), "queue")
			wantReads, wantDeletes, wantStarts := 1, 1, 1
			if recovered {
				wantReads, wantDeletes, wantStarts = 2, 0, 0
			}
			if status != nil || !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "cannot adopt paused machine: compute: fresh lifecycle does not support resume") || reads != wantReads || deletes != wantDeletes || starts != wantStarts {
				t.Fatalf("paused readiness: status=%v err=%v reads=%d deletes=%d starts=%d", status, err, reads, deletes, starts)
			}
		})
	}
}
