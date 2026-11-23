# External process with no Temporal Worker inside

This local demonstration runs through [`TestLocalExternalProcessExample`](../../sdk/external_process_example_test.go), an integration test. It uses `sandbox.NewSandbox`, `SandboxWorkflow` and the SmolCloud provider against a loopback fake API. The fake API starts `sh` with `printf process-output; exec sleep 2`. The test starts the Temporal Worker outside that process.

This directory contains a sample [Dockerfile](Dockerfile). The demonstration runs the shell command directly; it does not build, pull or publish an image. There is no standalone starter or Worker executable for this demonstration.

## Run locally

Use Linux or macOS with Go 1.26.2 or later, Temporal CLI, `/bin/sh` and `sleep` on `/usr/bin:/bin`. Go module dependencies must be available. No Docker daemon, cloud account, provider token or model credential is required. The test uses synthetic credentials and a loopback provider endpoint.

From the repository root, start a development server on free port 17293:

```sh
temporal server start-dev --ip 127.0.0.1 --port 17293 --headless
```

In another terminal, run these commands from the repository root. Each command selects one demonstration case:

```sh
GOWORK=off HARNESS_LOCAL_TEST=1 SMOL_CLOUD_LIVE=0 HARNESS_HISTORY_CAPTURE= go -C sdk test -count=1 -timeout=90s -v -run '^TestLocalExternalProcessExample$/^completion$' .
GOWORK=off HARNESS_LOCAL_TEST=1 SMOL_CLOUD_LIVE=0 HARNESS_HISTORY_CAPTURE= go -C sdk test -count=1 -timeout=90s -v -run '^TestLocalExternalProcessExample$/^cancellation$' .
GOWORK=off HARNESS_LOCAL_TEST=1 SMOL_CLOUD_LIVE=0 HARNESS_HISTORY_CAPTURE= go -C sdk test -count=1 -timeout=90s -v -run '^TestLocalExternalProcessExample$/^unsupported-profile$' .
```

All three commands must report `PASS`, with an executed case rather than `SKIP`:

- Completion observes `process-output` and exit code zero.
- Cancellation cancels the parent Workflow while the process runs and checks that cleanup interrupts and reaps the process.
- The unsupported profile reports `fresh lifecycle requires branchable=false` and `API requests=0`.

Completion and cancellation each print `controllers=1 creates=1 deletes=1 process reaped` and the launch request. Here, `controllers=1` counts one child `SandboxWorkflow`. The launch request contains `env:{}` and no Temporal bootstrap or Namespace credentials. The test stops its Worker and fake API, reaps its subprocess and uses fallback cleanup if an assertion fails. Fallback cleanup does not satisfy its cleanup assertions. Stop the in-memory development server with Ctrl+C when finished.

## Configuration and cleanup

The test supplies this provider configuration:

```go
map[string]string{
    "image": "external-process:local",
    "bootstrap": "none",
    "lifecycle": "fresh",
    "branchable": "false",
    "network": "blocked",
}
```

`external-process:local` is a test value, not a shared image. `sandbox.NewSandbox` also receives `sandbox.WithIdleTimeout(sandbox.NoIdleTimeout)`. The unsupported case changes `branchable` to `true`; `SandboxWorkflow` rejects it before any provider request or process allocation.

The parent Workflow owns the sandbox and defers `OwnedSandbox.Stop` with a disconnected Workflow context. `SandboxWorkflow` calls `StopSandbox`, which sends one matching provider DELETE. The fake API stops the local process on DELETE. No Temporal Worker or Namespace credential runs inside the process.

For completion, `ExecuteCommand("wait-for-process")` asks the fake API to wait for the process result. That operation belongs to this test API; the sample image does not implement it. Process completion does not signal sandbox idleness. The test uses `NoIdleTimeout` and explicitly stops the sandbox.

## Limits

These checks cover local process completion, cancellation, cleanup and configuration refusal. They do not qualify real image boot, SmolCloud readiness or teardown, network isolation, host replacement or confidential execution. A real deployment needs a published image, an outside Worker and a provider-supported process observer; this directory supplies no complete live-run procedure. Native agent and skill integration require separate work.
