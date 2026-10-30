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
	"go.temporal.io/sdk/activity"
)

// Run with BLAXEL_LIVE=1, BL_API_KEY and BL_WORKSPACE on an activity worker.
// The snapshot subtest skips itself in workspaces without snapshots.
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
	// A retried command waits for the process its first attempt started.
	result, err = runActivity(t, func(ctx context.Context) (*compute.CommandResult, error) {
		if activity.GetInfo(ctx).Attempt == 1 {
			// The first attempt's worker goes away mid-command.
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			time.AfterFunc(3*time.Second, cancel)
		}
		return p.ExecuteCommand(ctx, original, "echo run >> /tmp/runs; sleep 8; wc -l < /tmp/runs")
	})
	if err != nil || strings.TrimSpace(result.Stdout) != "1" {
		t.Fatalf("retried command: result=%+v err=%v", result, err)
	}
	// Output over Temporal's payload limit, or more than the process logs
	// return, fails instead of hanging or coming back cut.
	for command, reason := range map[string]string{
		"head -c 2500000 /dev/zero | tr '\\0' x": "limit",
		"head -c 5000000 /dev/zero | tr '\\0' x": "truncated",
	} {
		if result, err := p.ExecuteCommand(ctx, original, command); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), reason) {
			t.Fatalf("oversized output: %s: err=%v", command, err)
		} else if result != nil {
			t.Fatalf("oversized output returned %d bytes", len(result.Stdout))
		}
	}
	t.Run("snapshot", func(t *testing.T) { liveSnapshot(ctx, t, p, original, queue, cleanupSandbox) })
}

func liveSnapshot(ctx context.Context, t *testing.T, p *provider, original *compute.ProviderStatus, queue string, cleanupSandbox func(*compute.ProviderStatus)) {
	_, snapshot, err := p.Snapshot(ctx, original)
	if errors.Is(err, errors.ErrUnsupported) {
		t.Skipf("snapshots are not enabled in this workspace: %v", err)
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
	result, err := p.ExecuteCommand(ctx, fork, "cat /tmp/proof && echo $TEMPORAL_TASK_QUEUE")
	if err != nil || result.ExitCode != 0 || !strings.HasSuffix(result.Stdout, "temporal-live\n"+queue+"-fork\n") {
		t.Fatalf("forked sandbox: result=%+v err=%v", result, err)
	}
}
