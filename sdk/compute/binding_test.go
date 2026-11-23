package compute_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/smolcloud"
)

// Matching zero identities must fail completeness before equality can accept them.
func TestResourceBindingLocalContract(t *testing.T) {
	for _, name := range []string{"matching-empty-provider", "matching-empty-instance", "provider-conflict", "fresh-nil-status", "fresh-unbound-status", "fresh-bound", "legacy-unbound"} {
		t.Run(name, func(t *testing.T) {
			details := compute.ProviderDetails{Type: compute.ProviderTypeSmolCloud, Config: map[string]string{"lifecycle": "fresh"}}
			status := &compute.ProviderStatus{InstanceID: "resource",
				ProviderType: details.Type, TaskQueueName: "queue", ConfigurationDigest: "provider-owned-digest",
			}
			want := ""
			switch name {
			case "matching-empty-provider":
				details.Type, status.ProviderType = "", ""
				want = "incomplete resource binding"
			case "matching-empty-instance":
				status.InstanceID = ""
				want = "incomplete resource binding"
			case "fresh-nil-status":
				status = nil
			case "fresh-unbound-status":
				status.ProviderType, status.TaskQueueName, status.ConfigurationDigest = "", "", ""
			case "provider-conflict":
				status.ProviderType = compute.ProviderTypeDaytona
				want = "conflicts with provider"
			case "legacy-unbound":
				details.Config = nil
				status.ProviderType, status.TaskQueueName, status.ConfigurationDigest = "", "", ""
			}
			err := compute.ValidateResourceBinding(details, status)
			if want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("binding validation: %v, want %q", err, want)
			} else if !errors.Is(fmt.Errorf("outer: %w", err), compute.ErrInvalidResourceBinding) {
				t.Fatalf("binding error lost identity: %v", err)
			}
		})
	}
}

func TestProviderStatusWireAndCompleteness(t *testing.T) {
	details := compute.ProviderDetails{Type: compute.ProviderTypeSmolCloud}
	for mask := 0; mask < 8; mask++ {
		status := compute.ProviderStatus{InstanceID: "resource"}
		if mask&1 != 0 {
			status.ProviderType = details.Type
		}
		if mask&2 != 0 {
			status.TaskQueueName = "queue"
		}
		if mask&4 != 0 {
			status.ConfigurationDigest = "digest"
		}
		encoded, err := json.Marshal(status)
		if err != nil {
			t.Fatal(err)
		}
		if mask == 0 && string(encoded) != `{"instance_id":"resource"}` {
			t.Fatalf("legacy wire: %s", encoded)
		}
		var decoded compute.ProviderStatus
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != status {
			t.Fatalf("round trip: %+v %v", decoded, err)
		}
		err = compute.ValidateResourceBinding(details, &decoded)
		if mask == 0 || mask == 7 {
			if err != nil {
				t.Fatalf("valid context %d: %v", mask, err)
			}
		} else if !errors.Is(err, compute.ErrInvalidResourceBinding) {
			t.Fatalf("partial context %d accepted: %v", mask, err)
		}
	}
}
