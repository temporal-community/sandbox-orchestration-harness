package smolcloud

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

// Run with SMOL_CLOUD_LIVE=1 and SMOL_CLOUD_TOKEN on an activity worker.
// Set SMOL_CLOUD_LIVE_NETWORKED=1 to verify networked restores after Cloud API rollout.
func TestCloudLiveCheckpointResume(t *testing.T) {
	if os.Getenv("SMOL_CLOUD_LIVE") != "1" {
		t.Skip("set SMOL_CLOUD_LIVE=1 and SMOL_CLOUD_TOKEN to test the live cloud")
	}
	network := "blocked"
	if os.Getenv("SMOL_CLOUD_LIVE_NETWORKED") == "1" {
		network = "open"
	}
	instance, err := New(map[string]string{"image": "alpine:3.20", "network": network})
	if err != nil {
		t.Fatal(err)
	}
	p := instance.(*provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	queue := fmt.Sprintf("temporal-smol-live-%d", time.Now().UnixNano())
	cleanupMachine := func(status *compute.ProviderStatus) {
		if status == nil {
			return
		}
		cleanupCtx, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if err := p.Stop(cleanupCtx, status); err != nil {
			t.Errorf("delete live VM %s: %v", status.InstanceID, err)
		}
	}
	original, err := p.Start(ctx, queue)
	if err != nil {
		t.Fatalf("create and start live VM: %v", err)
	}
	t.Cleanup(func() { cleanupMachine(original) })
	result, err := p.ExecuteCommand(ctx, original, "echo temporal-live > /workspace/proof && cat /workspace/proof")
	if err != nil || result.ExitCode != 0 || !strings.Contains(result.Stdout, "temporal-live") {
		t.Fatalf("live exec: result=%+v err=%v", result, err)
	}
	_, snapshot, err := p.Snapshot(ctx, original)
	if err != nil {
		t.Fatalf("live checkpoint: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if err := p.DeleteSnapshot(cleanupCtx, snapshot); err != nil {
			t.Errorf("delete live checkpoint: %v", err)
		}
	})
	fork, err := p.StartFromSnapshot(ctx, queue+"-fork", snapshot)
	if err != nil {
		t.Fatalf("live restore: %v", err)
	}
	t.Cleanup(func() { cleanupMachine(fork) })
	result, err = p.ExecuteCommand(ctx, fork, "cat /workspace/proof")
	if err != nil || result.ExitCode != 0 || strings.TrimSpace(result.Stdout) != "temporal-live" {
		t.Fatalf("restored workspace: result=%+v err=%v", result, err)
	}
	if err := p.Suspend(ctx, original); err != nil {
		t.Fatalf("live pause: %v", err)
	}
	if err := p.Resume(ctx, original); err != nil {
		t.Fatalf("live resume: %v", err)
	}
	result, err = p.ExecuteCommand(ctx, original, "cat /workspace/proof")
	if err != nil || result.ExitCode != 0 || strings.TrimSpace(result.Stdout) != "temporal-live" {
		t.Fatalf("resumed workspace: result=%+v err=%v", result, err)
	}
}
