package compute_test

import (
	"strings"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/smolcloud"
)

// Matching zero identities must fail completeness before equality can accept them.
func TestResourceBindingLocalContract(t *testing.T) {
	for _, name := range []string{"matching-empty-provider", "matching-empty-instance", "fresh-nil-status", "fresh-unbound-status", "fresh-bound", "legacy-unbound"} {
		t.Run(name, func(t *testing.T) {
			details := compute.ProviderDetails{Type: compute.ProviderTypeSmolCloud, Config: map[string]string{"lifecycle": "fresh"}}
			status := &compute.ProviderStatus{InstanceID: "resource", ResourceBinding: &compute.ResourceBinding{
				ProviderType: details.Type, TaskQueueName: "queue", InstanceID: "resource", ConfigurationDigest: "provider-owned-digest",
			}}
			want := ""
			switch name {
			case "matching-empty-provider":
				details.Type, status.ResourceBinding.ProviderType = "", ""
				want = "incomplete resource binding"
			case "matching-empty-instance":
				status.InstanceID, status.ResourceBinding.InstanceID = "", ""
				want = "incomplete resource binding"
			case "fresh-nil-status":
				status = nil
				want = "fresh resource binding is required"
			case "fresh-unbound-status":
				status.ResourceBinding = nil
				want = "fresh resource binding is required"
			case "legacy-unbound":
				details.Config = nil
				status.ResourceBinding = nil
			}
			err := compute.ValidateResourceBinding(details, status)
			if want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("binding validation: %v, want %q", err, want)
			}
		})
	}
}
