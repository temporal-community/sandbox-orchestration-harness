package baseten

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/activity"
)

// Run with BASETEN_LIVE=1 and BASETEN_API_KEY on an activity worker. It uses
// one sandbox of the organization's limit and deletes it at the end.
func TestBasetenLive(t *testing.T) {
	if os.Getenv("BASETEN_LIVE") != "1" {
		t.Skip("set BASETEN_LIVE=1 and BASETEN_API_KEY to test the live cloud")
	}
	instance, err := New(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	p := instance.(*provider)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	queue := fmt.Sprintf("temporal-baseten-live-%d", time.Now().UnixNano())
	start := time.Now()
	status, err := p.Start(ctx, queue)
	if err != nil {
		t.Fatalf("create live sandbox: %v", err)
	}
	t.Logf("start took %s", time.Since(start))
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if err := p.Stop(cleanupCtx, status); err != nil {
			t.Errorf("delete live sandbox %s: %v", status.InstanceID, err)
		}
	})
	start = time.Now()
	result, err := p.ExecuteCommand(ctx, status, "echo temporal-live > /tmp/proof && cat /tmp/proof && echo $TEMPORAL_TASK_QUEUE")
	if err != nil || result.ExitCode != 0 || result.Stdout != "temporal-live\n"+queue+"\n" {
		t.Fatalf("live exec: result=%+v err=%v", result, err)
	}
	t.Logf("first command took %s", time.Since(start))
	// Temporal may retry Start; it must return the same sandbox.
	recovered, err := p.Start(ctx, queue)
	if err != nil || recovered.InstanceID != status.InstanceID {
		t.Fatalf("live retry of start: recovered=%v err=%v", recovered, err)
	}
	// Left idle, the sandbox goes on standby; the next command resumes it
	// with its files.
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(45 * time.Second):
	}
	start = time.Now()
	result, err = p.ExecuteCommand(ctx, status, "cat /tmp/proof; echo out-of-band >&2; exit 3")
	if err != nil || result.ExitCode != 3 || result.Stdout != "temporal-live\n" || result.Stderr != "out-of-band\n" {
		t.Fatalf("resumed sandbox: result=%+v err=%v", result, err)
	}
	t.Logf("command after idle took %s", time.Since(start))
	// A retried command waits for the process its first attempt started.
	result, err = runActivity(t, func(ctx context.Context) (*compute.CommandResult, error) {
		if activity.GetInfo(ctx).Attempt == 1 {
			// The first attempt's worker goes away mid-command.
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			time.AfterFunc(3*time.Second, cancel)
		}
		return p.ExecuteCommand(ctx, status, "echo run >> /tmp/runs; sleep 8; wc -l < /tmp/runs")
	})
	if err != nil || strings.TrimSpace(result.Stdout) != "1" {
		t.Fatalf("retried command: result=%+v err=%v", result, err)
	}
	// Output with many lines comes back whole and fast.
	start = time.Now()
	result, err = p.ExecuteCommand(ctx, status, "seq 1 60000")
	if err != nil {
		t.Fatalf("many lines: %v", err)
	}
	if !strings.HasPrefix(result.Stdout, "1\n2\n") || !strings.HasSuffix(result.Stdout, "\n60000\n") || len(result.Stdout) != 348894 {
		t.Fatalf("many lines: got %d bytes", len(result.Stdout))
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("60,000 lines took %s", elapsed)
	} else {
		t.Logf("60,000 lines took %s", elapsed)
	}
	// Output over Temporal's payload limit, or more than the process logs
	// return, fails instead of hanging or coming back cut.
	for command, reason := range map[string]string{
		"head -c 2500000 /dev/zero | tr '\\0' x": "limit",
		"head -c 5000000 /dev/zero | tr '\\0' x": "truncated",
	} {
		if result, err := p.ExecuteCommand(ctx, status, command); !isFinal(err, "OutputTooLarge") || !strings.Contains(err.Error(), reason) {
			t.Fatalf("oversized output: %s: err=%v", command, err)
		} else if result != nil {
			t.Fatalf("oversized output returned %d bytes", len(result.Stdout))
		}
	}
	if _, _, err := p.Snapshot(ctx, status); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("snapshot: %v", err)
	}
}
