package cwsandbox

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

// Opt in explicitly: this test creates billable sandboxes and a filesystem snapshot.
func TestIntegration(t *testing.T) {
	if os.Getenv("CWSANDBOX_INTEGRATION_TEST") != "1" {
		t.Skip("set CWSANDBOX_INTEGRATION_TEST=1 to test against the live API")
	}
	config := map[string]string{
		"container-image": "ubuntu:24.04", "command": `["sleep", "infinity"]`,
		"workspace-path": "/workspace", "max-lifetime-seconds": "600",
	}
	if endpoint := os.Getenv("CWSANDBOX_BASE_URL"); endpoint != "" {
		config["base-url"] = endpoint
	}
	if mode := os.Getenv("CWSANDBOX_MODE"); mode != "" {
		config["mode"] = mode
	}
	if runners := os.Getenv("CWSANDBOX_RUNNER_IDS"); runners != "" {
		config["runner-ids"] = runners
	}
	provider, err := compute.Lookup(compute.ProviderTypeCoreWeaveSandbox, config)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cleanupSandbox := func(status *compute.ProviderStatus) {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := provider.Stop(ctx, status); err != nil {
				t.Errorf("cleanup sandbox %s: %v", status.InstanceID, err)
			}
		})
	}
	original, err := provider.Start(ctx, "cwsandbox-smoke-original")
	require.NoError(t, err)
	cleanupSandbox(original)
	result, err := provider.ExecuteCommand(ctx, original, `printf '%s' "$TEMPORAL_TASK_QUEUE" > /workspace/message`)
	require.NoError(t, err)
	require.Zero(t, result.ExitCode)
	state, snapshot, err := provider.Snapshot(ctx, original)
	require.NoError(t, err)
	require.Equal(t, compute.SandboxPostSnapshotRunning, state)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := provider.DeleteSnapshot(ctx, snapshot); err != nil {
			t.Errorf("cleanup snapshot %s: %v", snapshot.SnapshotID, err)
		}
	})
	require.NoError(t, provider.Stop(ctx, original))
	restored, err := provider.StartFromSnapshot(ctx, "cwsandbox-smoke-restored", snapshot)
	require.NoError(t, err)
	cleanupSandbox(restored)
	result, err = provider.ExecuteCommand(ctx, restored, `cat /workspace/message; printf '\n%s' "$TEMPORAL_TASK_QUEUE"`)
	require.NoError(t, err)
	require.Zero(t, result.ExitCode)
	require.Equal(t, "cwsandbox-smoke-original\ncwsandbox-smoke-restored", result.Stdout)
}
