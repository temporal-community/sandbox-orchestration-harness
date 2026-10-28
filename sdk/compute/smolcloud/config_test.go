package smolcloud

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func TestExplicitSettingsCreateRequests(t *testing.T) {
	for _, bootstrap := range []string{"worker", "none"} {
		for _, lifecycle := range []string{"standard", "fresh"} {
			for _, branchable := range []bool{false, true} {
				if lifecycle == "fresh" && branchable {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%t", bootstrap, lifecycle, branchable), func(t *testing.T) {
					var request createMachineRequest
					starts := 0
					nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
						switch r.Method + " " + r.URL.Path {
						case "POST /v1/machines":
							if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
								t.Error(err)
							}
							fmt.Fprint(w, `{"id":"machine","network":{"mode":"blocked"}}`)
						case "POST /v1/machines/machine/start":
							starts++
							want := ""
							if branchable {
								want = "forkable=true"
							}
							if r.URL.RawQuery != want {
								t.Errorf("start query %q, want %q", r.URL.RawQuery, want)
							}
							w.WriteHeader(http.StatusNoContent)
						case "GET /v1/machines/machine":
							fmt.Fprint(w, `{"state":"running","ready":true}`)
						default:
							t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
							w.WriteHeader(http.StatusBadRequest)
						}
					})
					p := newNativeProvider(t, map[string]string{"image": "image", ConfigKeyBootstrap: bootstrap, ConfigKeyLifecycle: lifecycle, ConfigKeyBranchable: fmt.Sprint(branchable)})
					if status, err := p.Start(context.Background(), "stable-queue"); err != nil || status.InstanceID != "machine" {
						t.Fatalf("start: %v %v", status, err)
					}
					wantEnv := map[string]string{}
					if bootstrap == "worker" {
						wantEnv["TEMPORAL_TASK_QUEUE"] = "stable-queue"
					}
					wantName, _ := machineName("stable-queue", "")
					if request.Name != wantName || !reflect.DeepEqual(request.Env, wantEnv) || request.Branchable != branchable || starts != 1 || request.Labels["temporal-harness-config"] == "" {
						t.Fatalf("unexpected request %+v; starts=%d", request, starts)
					}
				})
			}
		}
	}
}

func TestSettingsAdmissionAndPurePolicy(t *testing.T) {
	// Policy admission must work in a Workflow without endpoint or credential access.
	t.Setenv("SMOL_CLOUD_TOKEN", "")
	t.Setenv("SMOL_CLOUD_URL", "invalid endpoint")
	for _, config := range []map[string]string{
		{"image": "image"},
		{"image": "image", "bootstrap": "none"},
		{"image": "image", "lifecycle": "fresh"},
		nativeConfig(),
	} {
		policy, err := compute.ResolvePolicy(compute.ProviderDetails{Type: providerType, Config: config})
		if err != nil {
			t.Fatal(err)
		}
		fresh := config["lifecycle"] == "fresh" || config["workload"] == "native-process"
		if (policy.Lifecycle == compute.LifecycleFresh) != fresh {
			t.Fatalf("policy %v for %v", policy, config)
		}
	}
	for _, settings := range []map[string]string{
		{"bootstrap": ""}, {"bootstrap": "unknown"}, {"lifecycle": ""}, {"lifecycle": "unknown"},
		{"branchable": ""}, {"branchable": "TRUE"}, {"branchable": "1"},
		{"lifecycle": "fresh", "branchable": "true"},
		{"workload": "native-process", "bootstrap": "worker"},
		{"workload": "native-process", "lifecycle": "standard"},
		{"workload": "native-process", "branchable": "true"},
		{"lifecycle": "fresh", "env": "secret"},
		{"lifecycle": "fresh", "command": "ignored"},
		{"lifecycle": "fresh", "opaque-future-key": "ignored"},
	} {
		config := maps.Clone(settings)
		config["image"] = "image"
		_, policyErr := compute.ResolvePolicy(compute.ProviderDetails{Type: providerType, Config: config})
		_, constructorErr := New(config)
		if policyErr == nil || constructorErr == nil || policyErr.Error() != constructorErr.Error() {
			t.Fatalf("admission mismatch for %v: policy=%v constructor=%v", config, policyErr, constructorErr)
		}
	}
	for _, settings := range []map[string]string{{}, {"bootstrap": "worker"}, {"bootstrap": "none", "lifecycle": "standard"}} {
		settings["image"], settings["opaque-future-key"] = "image", "accepted"
		if _, err := compute.ResolvePolicy(compute.ProviderDetails{Type: providerType, Config: settings}); err != nil {
			t.Fatalf("standard configuration flexibility: %v", err)
		}
	}
}

func TestAPIResourceValidationIndependentOfSettings(t *testing.T) {
	calls := 0
	nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(http.StatusBadRequest) })
	// Default configurations obey the API wire bounds too, even without settings keys.
	for _, key := range []string{"cpus", "memory-mb"} {
		if _, err := New(map[string]string{"image": "image", key: "2147483648"}); err == nil || !strings.Contains(err.Error(), key+" must be") {
			t.Fatalf("default configuration accepted API-invalid %s: %v", key, err)
		}
	}
	for _, bootstrap := range []string{"", "worker", "none"} {
		for _, lifecycle := range []string{"standard", "fresh"} {
			for _, key := range []string{"cpus", "memory-mb", "disk-gb"} {
				bad := []string{"0", "-1", "not-a-number", "9223372036854775808"}
				if key != "disk-gb" {
					bad = append(bad, "2147483648", "1099511627776")
				}
				for _, value := range bad {
					config := map[string]string{"image": "image", "lifecycle": lifecycle, key: value}
					if bootstrap != "" {
						config["bootstrap"] = bootstrap
					}
					if _, err := New(config); err == nil || !strings.Contains(err.Error(), key+" must be") {
						t.Fatalf("API-invalid %v accepted: %v", config, err)
					}
				}
			}
			valid := map[string]string{"image": "image", "lifecycle": lifecycle, "cpus": "2147483647", "memory-mb": "2147483647", "disk-gb": "1099511627776"}
			if bootstrap != "" {
				valid["bootstrap"] = bootstrap
			}
			if _, err := New(valid); err != nil {
				t.Fatalf("valid API boundaries: %v", err)
			}
		}
	}
	if calls != 0 {
		t.Fatalf("invalid resources reached HTTP: %d", calls)
	}
}

func TestFreshProviderLifecycleRefusesBeforeHTTP(t *testing.T) {
	calls := 0
	nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(http.StatusBadRequest) })
	for _, config := range []map[string]string{
		nativeConfig(),
		{"image": "image", "bootstrap": "worker", "lifecycle": "fresh"},
		{"image": "image", "bootstrap": "none", "lifecycle": "fresh"},
	} {
		p := newNativeProvider(t, config)
		status := &compute.ProviderStatus{InstanceID: "machine"}
		snapshot := &compute.ProviderSnapshot{SnapshotID: "snapshot"}
		for _, empty := range []bool{false, true} {
			if empty {
				status, snapshot = nil, nil
			}
			_, _, snapshotErr := p.Snapshot(context.Background(), status)
			_, restoreErr := p.StartFromSnapshot(context.Background(), "queue", snapshot)
			for _, err := range []error{p.Suspend(context.Background(), status), p.Resume(context.Background(), status), snapshotErr, restoreErr, p.DeleteSnapshot(context.Background(), snapshot)} {
				if !errors.Is(err, errors.ErrUnsupported) {
					t.Fatalf("fresh lifecycle failed admission for %v: %v", config, err)
				}
			}
		}
	}
	if calls != 0 {
		t.Fatalf("fresh operations reached HTTP: %d", calls)
	}
}

func TestBranchabilityIndependentOfBootstrapAndStandardLifecycle(t *testing.T) {
	for _, bootstrap := range []string{"worker", "none"} {
		t.Run(bootstrap, func(t *testing.T) {
			calls := 0
			nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines/machine/pause", "POST /v1/machines/machine/resume":
					w.WriteHeader(http.StatusNoContent)
				case "GET /v1/machines/machine":
					fmt.Fprint(w, `{"state":"running","ready":true}`)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			p := newNativeProvider(t, map[string]string{"image": "image", "bootstrap": bootstrap, "branchable": "false"})
			status := &compute.ProviderStatus{InstanceID: "machine"}
			_, _, snapshotErr := p.Snapshot(context.Background(), status)
			_, restoreErr := p.StartFromSnapshot(context.Background(), "queue", &compute.ProviderSnapshot{SnapshotID: "snapshot"})
			if !errors.Is(snapshotErr, errors.ErrUnsupported) || !errors.Is(restoreErr, errors.ErrUnsupported) || calls != 0 {
				t.Fatalf("nonbranchable admission: %v %v calls=%d", snapshotErr, restoreErr, calls)
			}
			if err := p.Suspend(context.Background(), status); err != nil {
				t.Fatal(err)
			}
			if err := p.Resume(context.Background(), status); err != nil {
				t.Fatal(err)
			}
			if calls != 3 {
				t.Fatalf("standard lifecycle requests=%d", calls)
			}
		})
	}
}

func TestConfigurationDigestCompatibilityAndIndependence(t *testing.T) {
	nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) { t.Fatal("unexpected HTTP") })
	// This is the original v1 JSON encoding, independent of the current binding type.
	old := `{"Version":1,"Queue":"queue","Workload":"native-process","Source":{"type":"image","reference":"image"},"Resources":{"cpus":1,"memoryMb":512},"Network":{"mode":"blocked"}}`
	sum := sha256.Sum256([]byte(old))
	want := hex.EncodeToString(sum[:])
	for _, config := range []map[string]string{
		{"image": "image", "workload": "native-process"},
		{"image": "image", "bootstrap": "none", "lifecycle": "fresh"},
		{"image": "image", "workload": "native-process", "bootstrap": "none", "lifecycle": "fresh", "branchable": "false"},
	} {
		p := newNativeProvider(t, config).(*provider)
		if got := p.configurationLabel("queue"); got != want {
			t.Fatalf("historical digest mismatch: %s != %s", got, want)
		}
	}
	seen := map[string]bool{want: true}
	for _, config := range []map[string]string{
		{"image": "image", "bootstrap": "worker", "lifecycle": "fresh"},
		{"image": "image", "bootstrap": "none", "lifecycle": "standard", "branchable": "false"},
		{"image": "image", "bootstrap": "none", "lifecycle": "standard", "branchable": "true"},
		{"image": "image", "bootstrap": "worker", "lifecycle": "standard", "branchable": "true"},
		{"image": "image", "bootstrap": "worker", "lifecycle": "standard", "branchable": "false"},
	} {
		p := newNativeProvider(t, config).(*provider)
		digest := p.configurationLabel("queue")
		if seen[digest] {
			t.Fatalf("settings share a digest: %v", config)
		}
		seen[digest] = true
	}
}

func TestV2ConfigurationDigestGolden(t *testing.T) {
	nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected HTTP") })
	p := newNativeProvider(t, map[string]string{
		"image": "example/worker@sha256:abcdef", "bootstrap": "none", "lifecycle": "standard", "branchable": "true",
		"cpus": "3", "memory-mb": "1536", "disk-gb": "17", "network": "allowCidrs",
		"allow-cidrs": "198.51.100.0/24,192.0.2.0/24", "allow-hosts": "z.example,a.example",
	}).(*provider)
	// Literal wire encoding fixes every field independently of the production types.
	encoded := `{"Version":2,"Queue":"v2-queue","Bootstrap":"none","Lifecycle":"","Branchable":true,"Source":{"type":"image","reference":"example/worker@sha256:abcdef"},"Resources":{"cpus":3,"memoryMb":1536,"diskGb":17},"Network":{"mode":"allowCidrs","cidrs":["192.0.2.0/24","198.51.100.0/24"],"hosts":["a.example","z.example"]}}`
	sum := sha256.Sum256([]byte(encoded))
	if got, want := p.configurationLabel("v2-queue"), hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("v2 digest differs from independent encoding: got %s want %s", got, want)
	}
}

func TestV2ConfigurationBindingSensitivityAndAdoption(t *testing.T) {
	for _, tc := range []struct{ name, key, value string }{
		{"queue", "", "other-queue"},
		{"image", "image", "example/other@sha256:123456"},
		{"cpu", "cpus", "4"},
		{"memory", "memory-mb", "2048"},
		{"disk", "disk-gb", "18"},
		{"network-mode", "network", "open"},
		{"cidr-values", "allow-cidrs", "203.0.113.0/24,192.0.2.0/24"},
		{"host-values", "allow-hosts", "other.example,a.example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := map[string]string{
				"image": "example/worker@sha256:abcdef", "bootstrap": "none", "lifecycle": "standard", "branchable": "true",
				"cpus": "3", "memory-mb": "1536", "disk-gb": "17", "network": "allowCidrs",
				"allow-cidrs": "198.51.100.0/24,192.0.2.0/24", "allow-hosts": "z.example,a.example",
			}
			if tc.name == "network-mode" {
				// Change only mode, with no lists to obscure omission of the mode field.
				config["network"] = "blocked"
				delete(config, "allow-cidrs")
				delete(config, "allow-hosts")
			}
			otherConfig := maps.Clone(config)
			queue, otherQueue := "v2-queue", "v2-queue"
			if tc.name == "queue" {
				otherQueue = tc.value
			} else {
				otherConfig[tc.key] = tc.value
			}
			var p *provider
			var requested createMachineRequest
			var otherLabel string
			calls, effects := 0, 0
			nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines":
					if err := json.NewDecoder(r.Body).Decode(&requested); err != nil {
						t.Error(err)
					}
					w.WriteHeader(http.StatusConflict)
				case "GET /v1/machines":
					// Ownership, name, and actual network match; only the binding differs.
					_ = json.NewEncoder(w).Encode([]map[string]any{{
						"id": "conflict", "name": requested.Name, "network": p.networkPolicy(),
						"labels": map[string]string{"temporal-harness": "smol-cloud", "temporal-harness-config": otherLabel},
					}})
				default:
					effects++
					t.Errorf("mismatched v2 binding reached resource: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			p = newNativeProvider(t, config).(*provider)
			other := newNativeProvider(t, otherConfig).(*provider)
			otherLabel = other.configurationLabel(otherQueue)
			if p.configurationLabel(queue) == otherLabel {
				t.Fatalf("v2 digest does not bind %s", tc.name)
			}
			status, err := p.Start(context.Background(), queue)
			if status != nil || err == nil || !strings.Contains(err.Error(), "configuration differs") || calls != 2 || effects != 0 {
				t.Fatalf("mismatched v2 binding adopted: status=%v err=%v calls=%d effects=%d", status, err, calls, effects)
			}
		})
	}
	t.Run("allowlist-order", func(t *testing.T) {
		nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected HTTP") })
		config := map[string]string{
			"image": "example/worker@sha256:abcdef", "bootstrap": "none", "lifecycle": "standard", "branchable": "true",
			"cpus": "3", "memory-mb": "1536", "disk-gb": "17", "network": "allowCidrs",
			"allow-cidrs": "198.51.100.0/24,192.0.2.0/24", "allow-hosts": "z.example,a.example",
		}
		p := newNativeProvider(t, config).(*provider)
		config["allow-cidrs"], config["allow-hosts"] = "192.0.2.0/24,198.51.100.0/24", "a.example,z.example"
		other := newNativeProvider(t, config).(*provider)
		if p.configurationLabel("v2-queue") != other.configurationLabel("v2-queue") {
			t.Fatal("allowlist order changed the v2 binding")
		}
	})
}

func TestExplicitSettingsAdoption(t *testing.T) {
	for _, tc := range []string{"matching", "bootstrap", "lifecycle", "branchable", "missing-label", "duplicate", "owner", "network"} {
		t.Run(tc, func(t *testing.T) {
			var request createMachineRequest
			reads := 0
			config := map[string]string{"image": "image", "bootstrap": "worker", "lifecycle": "standard", "branchable": "false"}
			var p *provider
			nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/machines":
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					w.WriteHeader(http.StatusConflict)
				case "GET /v1/machines":
					labels := maps.Clone(request.Labels)
					network := request.Network
					switch tc {
					case "bootstrap", "lifecycle", "branchable":
						otherConfig := maps.Clone(config)
						otherConfig[tc] = map[string]string{"bootstrap": "none", "lifecycle": "fresh", "branchable": "true"}[tc]
						other := newNativeProvider(t, otherConfig).(*provider)
						labels["temporal-harness-config"] = other.configurationLabel("queue")
					case "missing-label":
						delete(labels, "temporal-harness-config")
					case "owner":
						labels["temporal-harness"] = "foreign"
					case "network":
						network.Mode = "open"
					}
					row := map[string]any{"id": "owned", "name": request.Name, "labels": labels, "network": network}
					rows := []map[string]any{row}
					if tc == "duplicate" {
						rows = append(rows, row)
					}
					_ = json.NewEncoder(w).Encode(rows)
				case "GET /v1/machines/owned":
					reads++
					fmt.Fprint(w, `{"state":"running","ready":true}`)
				default:
					t.Errorf("conflicting resource mutated: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			p = newNativeProvider(t, config).(*provider)
			status, err := p.Start(context.Background(), "queue")
			if tc == "matching" {
				if err != nil || status.InstanceID != "owned" || reads != 2 {
					t.Fatalf("matching adoption: %v %v reads=%d", status, err, reads)
				}
			} else if err == nil || status != nil || reads != 0 {
				t.Fatalf("conflict adopted: %v %v reads=%d", status, err, reads)
			}
		})
	}
}

func TestNoBootstrapStandardRestoreAndConflictAdmission(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			var restored map[string]any
			mutations := 0
			nativeEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/checkpoints/checkpoint/restore":
					_ = json.NewDecoder(r.Body).Decode(&restored)
					if conflict {
						w.WriteHeader(http.StatusConflict)
					} else {
						fmt.Fprint(w, `{"id":"restored","network":{"mode":"blocked"}}`)
					}
				case "GET /v1/machines":
					// Restore has no supported label override; a missing binding fails closed.
					_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "restored", "name": restored["name"], "labels": map[string]string{"temporal-harness": "smol-cloud"}, "network": map[string]string{"mode": "blocked"}}})
				case "POST /v1/machines/restored/start":
					mutations++
					if r.URL.RawQuery != "forkable=true" {
						t.Errorf("standard bootstrap=none lost branching: %s", r.URL)
					}
					w.WriteHeader(http.StatusNoContent)
				case "GET /v1/machines/restored":
					fmt.Fprint(w, `{"state":"running","ready":true}`)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			p := newNativeProvider(t, map[string]string{"image": "image", "bootstrap": "none"})
			status, err := p.StartFromSnapshot(context.Background(), "queue", &compute.ProviderSnapshot{SnapshotID: "checkpoint"})
			if conflict {
				if err == nil || !strings.Contains(err.Error(), "configuration differs") || status != nil || mutations != 0 {
					t.Fatalf("unbound restore adopted: %v %v mutations=%d", status, err, mutations)
				}
			} else if err != nil || status.InstanceID != "restored" || mutations != 1 {
				t.Fatalf("restore: %v %v mutations=%d", status, err, mutations)
			}
			wantName, _ := machineName("queue", "checkpoint")
			if !reflect.DeepEqual(restored, map[string]any{"name": wantName, "network": map[string]any{"mode": "blocked"}}) {
				t.Fatalf("restore body changed: %v", restored)
			}
		})
	}
}
