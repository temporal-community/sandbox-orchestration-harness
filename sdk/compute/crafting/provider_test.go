package crafting

import (
	"context"
	"strings"
	"testing"

	crafting "github.com/crafting-demo/lightweight-go-client"
	"github.com/crafting-demo/lightweight-go-client/craftingtest"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

// The Crafting client is covered by its own tests, so these exercise the
// mapping this package is responsible for: harness config to client options,
// harness identifiers to Crafting names, and client results to harness types.

func testProvider(t *testing.T, runner crafting.Runner, raw map[string]string) *provider {
	t.Helper()
	if raw == nil {
		raw = map[string]string{"template": "agent-sandbox", "workspace": "dev", "folder": "lab"}
	}
	cfg, err := parseConfig(raw)
	if err != nil {
		t.Fatalf("parsing config: %v", err)
	}
	cfg.client.Runner = runner
	client, err := crafting.NewClient(cfg.client)
	if err != nil {
		t.Fatalf("building client: %v", err)
	}
	return &provider{client: client, cfg: cfg}
}

func TestProviderIsRegistered(t *testing.T) {
	if !compute.IsRegistered(compute.ProviderTypeCrafting) {
		t.Fatalf("provider type %q was not registered by init", compute.ProviderTypeCrafting)
	}
	if _, err := compute.Lookup(compute.ProviderTypeCrafting, map[string]string{
		"template":  "agent-sandbox",
		"workspace": "dev",
	}); err != nil {
		t.Fatalf("looking up registered provider: %v", err)
	}
}

// The task queue must reach the sandbox, otherwise the worker inside it never
// polls the queue the workflow is waiting on.
func TestStartInjectsTheTaskQueue(t *testing.T) {
	runner := craftingtest.NewRunner()
	p := testProvider(t, runner, nil)

	status, err := p.Start(context.Background(), "sandbox-wf-123")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(status.InstanceID, "lab/tsp-") {
		t.Errorf("instance id %q should be a folder-qualified derived name", status.InstanceID)
	}

	create := runner.JoinedArgsFor("sandbox", "create")
	for _, want := range []string{"-E TEMPORAL_TASK_QUEUE=sandbox-wf-123", "-t agent-sandbox"} {
		if !strings.Contains(create, want) {
			t.Errorf("create call missing %q\ngot: %s", want, create)
		}
	}
}

// Retrying Start after a partial failure must not leave a second sandbox behind,
// which is why the sandbox name is derived from the task queue rather than
// generated.
func TestStartIsIdempotentForTheSameTaskQueue(t *testing.T) {
	p := testProvider(t, craftingtest.NewRunner(), nil)
	ctx := context.Background()

	first, err := p.Start(ctx, "sandbox-wf-abc")
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Start(ctx, "sandbox-wf-abc")
	if err != nil {
		t.Fatal(err)
	}
	if first.InstanceID != second.InstanceID {
		t.Errorf("retry produced a different sandbox: %q then %q", first.InstanceID, second.InstanceID)
	}
}

// Forking from a snapshot must not reuse the name of the sandbox the same
// workflow already started, or the fork would collide with its own origin.
func TestStartFromSnapshotDiffersFromPlainStart(t *testing.T) {
	p := testProvider(t, craftingtest.NewRunner(), nil)
	ctx := context.Background()

	plain, err := p.Start(ctx, "sandbox-wf-1")
	if err != nil {
		t.Fatal(err)
	}
	id, err := (&crafting.CompositeSnapshot{Workspace: "dev", Home: "lab/snap-home"}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := p.StartFromSnapshot(ctx, "sandbox-wf-1", &compute.ProviderSnapshot{SnapshotID: id})
	if err != nil {
		t.Fatal(err)
	}
	if plain.InstanceID == restored.InstanceID {
		t.Error("restoring a snapshot reused the plain sandbox name")
	}
}

// The harness carries snapshots through workflow history as opaque strings, so
// a snapshot must survive the round trip and reach the client whole.
func TestSnapshotRoundTripsThroughTheHarnessID(t *testing.T) {
	runner := craftingtest.NewRunner().
		On("sandbox show", craftingtest.RunningState(), nil).
		OnExec("", "", 0)
	p := testProvider(t, runner, map[string]string{
		"template":     "agent-sandbox",
		"workspace":    "dev",
		"folder":       "lab",
		"dependencies": "db",
	})

	state, snap, err := p.Snapshot(context.Background(), &compute.ProviderStatus{InstanceID: "lab/tsp-abc"})
	if err != nil {
		t.Fatal(err)
	}
	// The sandbox keeps running, so the workflow should not change its lifecycle.
	if state != compute.SandboxPostSnapshotRunning {
		t.Errorf("post-snapshot state = %v, want running", state)
	}

	composite, err := crafting.DecodeCompositeSnapshot(snap.SnapshotID)
	if err != nil {
		t.Fatalf("snapshot id is not decodable: %v", err)
	}
	if composite.Home == "" || composite.Deps["db"] == "" {
		t.Errorf("composite lost components: %+v", composite)
	}

	// Restoring it must apply every component, or the fork comes up with a
	// workspace and a dataset that never coexisted.
	if _, err := p.StartFromSnapshot(context.Background(), "sandbox-wf-fork", snap); err != nil {
		t.Fatal(err)
	}
	create := runner.JoinedArgsFor("sandbox", "create")
	for _, want := range []string{"-D dev/home=", "-D db/snapshot="} {
		if !strings.Contains(create, want) {
			t.Errorf("restore call missing %q\ngot: %s", want, create)
		}
	}
}

// A snapshot id from another provider must be rejected rather than acted on.
func TestForeignSnapshotIDsAreRejected(t *testing.T) {
	p := testProvider(t, craftingtest.NewRunner(), nil)
	ctx := context.Background()

	if _, err := p.StartFromSnapshot(ctx, "sandbox-wf-1", &compute.ProviderSnapshot{SnapshotID: "e2b-12345"}); err == nil {
		t.Error("a foreign snapshot id should be rejected")
	}
	if err := p.DeleteSnapshot(ctx, nil); err == nil {
		t.Error("a nil snapshot should be rejected")
	}
}

// A command that runs and fails is a result the agent can act on, not an
// infrastructure error.
func TestExecuteCommandReportsExitCodeAsResult(t *testing.T) {
	runner := craftingtest.NewRunner().
		On("sandbox show", craftingtest.RunningState(), nil).
		OnExec("build output", "warning: deprecated", 2)
	p := testProvider(t, runner, nil)

	res, err := p.ExecuteCommand(context.Background(), &compute.ProviderStatus{InstanceID: "lab/tsp-abc"}, "make build")
	if err != nil {
		t.Fatalf("a failing command should not be an error: %v", err)
	}
	if res.ExitCode != 2 || res.Stdout != "build output" || res.Stderr != "warning: deprecated" {
		t.Errorf("result = %+v, want the command's own output and exit code", res)
	}
}

// Resuming before the command keeps cs resume progress out of the command's
// stderr.
func TestExecuteCommandResumesSuspendedSandboxFirst(t *testing.T) {
	runner := craftingtest.NewRunner().
		On("sandbox show", craftingtest.SuspendedState(), nil).
		OnExec("done", "", 0)
	p := testProvider(t, runner, nil)

	if _, err := p.ExecuteCommand(context.Background(), &compute.ProviderStatus{InstanceID: "lab/tsp-abc"}, "ls"); err != nil {
		t.Fatal(err)
	}
	if runner.ArgsFor("sandbox", "resume") == nil {
		t.Error("a suspended sandbox should be resumed before the command runs")
	}
}

// Teardown is retried by Temporal, so a sandbox that is already gone is success.
func TestStopTreatsMissingSandboxAsSuccess(t *testing.T) {
	runner := craftingtest.NewRunner().
		On("sandbox remove", &crafting.Result{Stderr: "Sandbox tsp-abc does not exist", ExitCode: 1}, nil)
	p := testProvider(t, runner, nil)

	if err := p.Stop(context.Background(), &compute.ProviderStatus{InstanceID: "lab/tsp-abc"}); err != nil {
		t.Errorf("an absent sandbox should be treated as removed, got %v", err)
	}
}

func TestMissingInstanceIDIsRejected(t *testing.T) {
	p := testProvider(t, craftingtest.NewRunner(), nil)
	ctx := context.Background()

	if err := p.Stop(ctx, nil); err == nil {
		t.Error("Stop with no status should fail")
	}
	if err := p.Suspend(ctx, &compute.ProviderStatus{}); err == nil {
		t.Error("Suspend with an empty instance id should fail")
	}
}

func TestConfigRequiresTemplateAndWorkspace(t *testing.T) {
	for _, raw := range []map[string]string{
		{},
		{"template": "agent-sandbox"},
		{"workspace": "dev"},
	} {
		if _, err := parseConfig(raw); err == nil {
			t.Errorf("config %v should have been rejected", raw)
		}
	}
}

func TestConfigRejectsMalformedValues(t *testing.T) {
	for name, raw := range map[string]map[string]string{
		"bad bool":     {"template": "t", "workspace": "w", "snapshot-base": "maybe"},
		"bad uid":      {"template": "t", "workspace": "w", "exec-uid": "root"},
		"bad duration": {"template": "t", "workspace": "w", "create-timeout": "soon"},
		"bad env":      {"template": "t", "workspace": "w", "extra-env": "NOEQUALS"},
	} {
		if _, err := parseConfig(raw); err == nil {
			t.Errorf("%s: expected rejection of %v", name, raw)
		}
	}
}

// Config values have to reach the CLI, so a few that are easy to drop silently
// are traced all the way through.
func TestConfigReachesTheCommandLine(t *testing.T) {
	runner := craftingtest.NewRunner().
		On("sandbox show", craftingtest.RunningState(), nil).
		OnExec("", "", 0)
	p := testProvider(t, runner, map[string]string{
		"template":   "agent-sandbox",
		"workspace":  "dev",
		"folder":     "lab",
		"org":        "eng",
		"extra-env":  "FOO=bar",
		"exec-uid":   "1001",
		"exec-dir":   "/srv",
		"use-pool":   "none",
		"config-dir": "/tmp/worker-cs",
	})

	if _, err := p.Start(context.Background(), "sandbox-wf-1"); err != nil {
		t.Fatal(err)
	}
	create := runner.JoinedArgsFor("sandbox", "create")
	for _, want := range []string{"-O eng", "--folder lab", "-E FOO=bar", "--use-pool none"} {
		if !strings.Contains(create, want) {
			t.Errorf("create call missing %q\ngot: %s", want, create)
		}
	}

	if _, err := p.ExecuteCommand(context.Background(), &compute.ProviderStatus{InstanceID: "lab/tsp-abc"}, "ls"); err != nil {
		t.Fatal(err)
	}
	exec := runner.JoinedArgsFor("exec")
	for _, want := range []string{"-u 1001", "-w /srv", "-W tsp-abc/dev"} {
		if !strings.Contains(exec, want) {
			t.Errorf("exec call missing %q\ngot: %s", want, exec)
		}
	}

	// An isolated config dir keeps the worker from disturbing a developer's own
	// cs session on the same host.
	calls := runner.Calls()
	if len(calls) == 0 || !contains(calls[0].Env, "SANDBOX_CONFIG_DIR=/tmp/worker-cs") {
		t.Error("the isolated config dir did not reach the CLI environment")
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
