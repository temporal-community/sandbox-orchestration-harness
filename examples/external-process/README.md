# Controller owned external process

Run a harmless native process through the existing SmolCloud provider, `Start` Activity and sandbox controller. The local example replaces the provider API with a loopback HTTP server. That server launches `sh` with `printf process-output; exec sleep 2`. The only Temporal Worker runs outside the process and polls the orchestration queue.

## Local prerequisites and commands

Use Linux or macOS with Go 1.26.2 or later, a Temporal CLI with `server start-dev`, `/bin/sh` and `sleep` on `/usr/bin:/bin`. Go module dependencies must be available. The example needs no Docker daemon, cloud account, provider token or model credential. It overrides provider credentials with synthetic values and uses only a loopback provider endpoint.

From the repository root, start an isolated, in memory development server in one terminal. Port 17293 must be free:

```sh
temporal server start-dev --ip 127.0.0.1 --port 17293 --headless
```

In another terminal, run each case from the repository root. `GOWORK=off` avoids the example workspace's separate Go version and resolves the SDK module directly:

```sh
GOWORK=off HARNESS_LOCAL_TEST=1 SMOL_CLOUD_LIVE=0 HARNESS_HISTORY_CAPTURE= go -C sdk test -count=1 -timeout=90s -v -run '^TestLocalExternalProcessExample$/^completion$' .
GOWORK=off HARNESS_LOCAL_TEST=1 SMOL_CLOUD_LIVE=0 HARNESS_HISTORY_CAPTURE= go -C sdk test -count=1 -timeout=90s -v -run '^TestLocalExternalProcessExample$/^cancellation$' .
GOWORK=off HARNESS_LOCAL_TEST=1 SMOL_CLOUD_LIVE=0 HARNESS_HISTORY_CAPTURE= go -C sdk test -count=1 -timeout=90s -v -run '^TestLocalExternalProcessExample$/^unsupported-profile$' .
```

All three commands must report `PASS`, with an executed case rather than `SKIP`. Completion and cancellation each print `controllers=1 creates=1 deletes=1 process reaped` and the captured launch request. Completion observes output `process-output` and exit code zero. Cancellation requests Workflow cancellation while the process is running, waits for the owner to stop the controller and confirms that the process exits with an error. The refused profile prints `fresh lifecycle requires branchable=false` and `API requests=0`. The test has a separate ten second process deadline and fallback cleanup for assertion failures; these do not satisfy the controller cleanup assertions.

Stop the development server with Ctrl+C after the checks. It stores no persistent history. The test starts and stops its outside Worker and fake API automatically and reaps its local subprocess.

## Launch and ownership

The example selects these nonsecret provider settings:

```go
map[string]string{
    "image": "external-process:local",
    "bootstrap": "none",
    "lifecycle": "fresh",
    "branchable": "false",
    "network": "blocked",
}
```

`sandbox.NewSandbox` receives `sandbox.WithIdleTimeout(sandbox.NoIdleTimeout)`. Bootstrap, lifecycle and branchability remain separate settings. The refused case changes only `branchable` to `true`. The controller rejects this combination before provider HTTP or process allocation.

The captured request must contain an empty `env` object, no `TEMPORAL_` bootstrap key, no synthetic Namespace credential and no synthetic provider token. Provider authentication stays on the outside HTTP request. The local process uses an explicit `PATH` environment and a temporary working directory. The image entrypoint in [Dockerfile](Dockerfile) runs the same bounded process, with no Temporal Worker or Namespace setup.

The parent calls `NewSandbox` once. The existing controller allocates one resource and performs one `StopSandbox` Activity and one provider DELETE. A deferred `OwnedSandbox.Stop` uses a disconnected Workflow context so cancellation can finish cleanup. There is no second resource owner. Existing [attachment ownership tests](../../sdk/attachment_test.go) cover nonowning callers and delegated stop separately.

The fake API's `wait-for-process` observer returns the local entrypoint's actual exit result. It is a test endpoint operation, not a command supplied by the image. Completion of this observer does not establish autonomous runtime idleness. `NoIdleTimeout` remains active, and the owner explicitly stops the resource. The test verifies that the controller schedules no idle timer.

## Optional real SmolCloud execution, unqualified

Real execution requires separate authorization, SmolCloud capacity, an outside Temporal Worker, an admitted image registry and a published image built from this directory. The local example does not build, publish or pull an image and does not contact SmolCloud. An image tag alone is not an immutable deployment pin; record the published digest before a real run.

For a separately authorized run, build and publish this Dockerfile using the selected registry's procedure. Use the published image reference with the settings above and explicit `NoIdleTimeout`. Configure `SMOL_CLOUD_TOKEN` only on the outside Worker and register the existing sandbox Workflows and Activities there. Use the [provider configuration reference](../../README.md) for SmolCloud setup. Do not copy the fake API's `wait-for-process` command into a real deployment: choose and qualify a provider supported process observation mechanism first. Confirm that this short lived entrypoint gives the provider enough time for machine readiness; this remains untested. Use the owned `Stop` path for cleanup and confirm actual provider teardown separately.

Fake conformance proves the inspected request, controller path and this local process only. It does not prove real image boot, provider readiness, cloud teardown, network isolation, credential security against hostile code, replacement recovery or confidential execution. Forced Workflow termination, outside Worker loss and lost DELETE replies are outside this example's proof. Existing provider retry/adoption and retained replay tests remain separate evidence.

## Next integration demo

The next demo composes a harness owned local Docker host with the native Claude SDK through its existing Connector extension point and runs one representative skill with its supporting files. That work needs a reviewed binding, readiness, package and shutdown contract. It has separate implementation tickets and authorization. This example invokes no Claude process or model, and that later demo is not a prerequisite for this example or generic external runtime acceptance.
