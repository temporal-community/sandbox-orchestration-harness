package blaxel

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	blaxelsdk "github.com/blaxel-ai/sdk-go"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

// Run with BLAXEL_LIVE=1, BL_API_KEY and BL_WORKSPACE on an activity worker.
// Snapshot and fork are checked only in workspaces where they are enabled.
func TestBlaxelLiveStandbyAndSnapshot(t *testing.T) {
	if os.Getenv("BLAXEL_LIVE") != "1" {
		t.Skip("set BLAXEL_LIVE=1, BL_API_KEY and BL_WORKSPACE to test the live cloud")
	}
	instance, err := New(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	p := instance.(*provider)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	queue := fmt.Sprintf("temporal-blaxel-live-%d", time.Now().UnixNano())
	cleanupSandbox := func(status *compute.ProviderStatus) {
		if status == nil {
			return
		}
		cleanupCtx, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if err := p.Stop(cleanupCtx, status); err != nil {
			t.Errorf("delete live sandbox %s: %v", status.InstanceID, err)
		}
	}
	original, err := p.Start(ctx, queue)
	if err != nil {
		t.Fatalf("create live sandbox: %v", err)
	}
	t.Cleanup(func() { cleanupSandbox(original) })
	result, err := p.ExecuteCommand(ctx, original, "echo temporal-live > /tmp/proof && cat /tmp/proof && echo $TEMPORAL_TASK_QUEUE")
	if err != nil || result.ExitCode != 0 || result.Stdout != "temporal-live\n"+queue+"\n" {
		t.Fatalf("live exec: result=%+v err=%v", result, err)
	}
	// Temporal may retry Start; it must return the same sandbox.
	recovered, err := p.Start(ctx, queue)
	if err != nil || recovered.InstanceID != original.InstanceID {
		t.Fatalf("live retry of start: recovered=%v err=%v", recovered, err)
	}
	if err := p.Suspend(ctx, original); err != nil {
		t.Fatalf("live suspend: %v", err)
	}
	for {
		sandbox, err := p.client.Sandboxes.Get(ctx, original.InstanceID, blaxelsdk.SandboxGetParams{})
		if err != nil {
			t.Fatalf("live get: %v", err)
		}
		if sandbox.State == blaxelsdk.SandboxStateStandby {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("sandbox did not go on standby")
		case <-time.After(5 * time.Second):
		}
	}
	if err := p.Resume(ctx, original); err != nil {
		t.Fatalf("live resume: %v", err)
	}
	result, err = p.ExecuteCommand(ctx, original, "cat /tmp/proof; echo out-of-band >&2; exit 3")
	if err != nil || result.ExitCode != 3 || result.Stdout != "temporal-live\n" || result.Stderr != "out-of-band\n" {
		t.Fatalf("resumed sandbox: result=%+v err=%v", result, err)
	}
	// More output than the process logs return fails instead of coming back cut.
	if result, err := p.ExecuteCommand(ctx, original, "head -c 5000000 /dev/zero | tr '\\0' x"); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("oversized output: err=%v", err)
	} else if result != nil {
		t.Fatalf("oversized output returned %d bytes", len(result.Stdout))
	}
	_, snapshot, err := p.Snapshot(ctx, original)
	if errors.Is(err, errors.ErrUnsupported) {
		t.Logf("snapshots are not enabled in this workspace: %v", err)
		return
	}
	if err != nil {
		t.Fatalf("live snapshot: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if err := p.DeleteSnapshot(cleanupCtx, snapshot); err != nil {
			t.Errorf("delete live snapshot: %v", err)
		}
	})
	fork, err := p.StartFromSnapshot(ctx, queue+"-fork", snapshot)
	if err != nil {
		t.Fatalf("live fork: %v", err)
	}
	t.Cleanup(func() { cleanupSandbox(fork) })
	result, err = p.ExecuteCommand(ctx, fork, "cat /tmp/proof && echo $TEMPORAL_TASK_QUEUE")
	if err != nil || result.ExitCode != 0 || !strings.HasSuffix(result.Stdout, "temporal-live\n"+queue+"-fork\n") {
		t.Fatalf("forked sandbox: result=%+v err=%v", result, err)
	}
}
