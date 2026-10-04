# Temporal Sandbox Orchestration Example

A Go example for running shell commands inside ephemeral, isolated compute environments from Temporal workflows. Workflow developers create a sandbox, run commands in it, and let the SDK handle provisioning, suspend/resume, snapshot/fork, and teardown — all driven by a long-lived child workflow that manages the sandbox lifecycle.

## Architecture

The repository is a Go workspace with one library module and several example modules:

| Module | Role |
|--------|------|
| `sdk/` | Public API — `NewSandbox`, `AttachToSandbox`, and the sandbox methods (`ExecuteCommand`, `Suspend`, `Resume`, `Snapshot`, `DeleteSnapshot`, `Stop`, `RequestStop`, `Ref`) |
| `examples/*/` | Runnable example workflows |

### How it works

1. A workflow calls `sandbox.NewSandbox(ctx, computeProvider)`.
2. The SDK starts a **child workflow** (`SandboxWorkflow`) using a UUID as the workflow ID. By default the child runs on the parent's task queue; with `WithTaskQueue` it runs on a separate queue served by a dedicated sandbox worker (see [Dedicated sandbox worker](#dedicated-sandbox-worker)).
3. Once the child is running, the SDK sends a `sandbox-init` update containing the compute provider config and idle timeout.
4. `SandboxWorkflow` executes `StartSandbox` (or `StartSandboxFromSnapshot` when a snapshot is provided), which provisions an ephemeral compute instance.
5. Subsequent `ExecuteCommand` calls are delivered to `SandboxWorkflow` as `sandbox-execute-command` updates; the workflow runs the command on the provisioned instance and returns the result.
6. After each command, an idle timer starts. If no command arrives within the idle timeout, the sandbox is automatically suspended.
7. When `ExecuteCommand` is called on a suspended sandbox it is transparently resumed first (unless `DisableAutoResume()` is passed).
8. `Snapshot` captures the sandbox state and returns an opaque `*compute.ProviderSnapshot`; the workflow updates its internal suspended/deleted state accordingly.
9. When the workflow calls `sbx.Stop(ctx)`, a `sandbox-stop` signal causes `SandboxWorkflow` to run `StopSandbox` and exit.

```
Parent workflow
  │
  ├─ NewSandbox() ──────────► starts SandboxWorkflow (child)
  │                                │
  │                                ├─ sandbox-init update → StartSandbox activity
  │                                │    └─ provisions ephemeral compute instance
  │                                │
  ├─ ExecuteCommand() ──────► sandbox-execute-command update → ExecuteCommand activity
  │                                │
  │                                ├─ idle timer starts after each command
  │                                │    └─ auto-suspends if no command within timeout
  │                                │
  ├─ Suspend() ─────────────► sandbox-suspend update → SuspendSandbox activity
  ├─ Resume() ──────────────► sandbox-resume update → ResumeSandbox activity
  ├─ Snapshot() ────────────► sandbox-snapshot update → SnapshotSandbox activity
  │
  └─ Stop() ────────────────► sandbox-stop signal → StopSandbox activity

  NewSandbox(ctx, provider, WithSnapshot(snap))
    └─ sandbox-init update → StartSandboxFromSnapshot activity
```

## SDK reference

### Creating and stopping a sandbox

```go
sbx, err := sandbox.NewSandbox(ctx, sandbox.Provider{
    Type:   compute.ProviderTypeE2B,
    Config: map[string]string{"template-id": "base"},
})

result, err := sbx.ExecuteCommand(ctx, "echo hello")
// result.Stdout, result.Stderr, result.ExitCode

err = sbx.Stop(ctx)
```

`NewSandbox` blocks until the sandbox is provisioned and returns an `OwnedSandbox`. `sandbox.Provider`, `sandbox.CommandResult` and `sandbox.ProviderSnapshot` are aliases for the corresponding `compute` types.

There are two ways to shut a sandbox down:

| Method | Available on | Behavior |
|--------|--------------|----------|
| `Stop(ctx)` | `OwnedSandbox` (from `NewSandbox`) | Signals the sandbox to stop and blocks until its workflow has finished tearing down the compute instance. Calling it again is a no-op. |
| `RequestStop(ctx)` | `Sandbox` and `OwnedSandbox` | Signals the sandbox to stop and returns immediately. Use it from workflows that attached via a reference. |

Both treat an already-gone sandbox as success. With the default cleanup behavior you can also skip stopping: the sandbox is cancelled and torn down when the creating workflow closes.

### Options

```go
sandbox.NewSandbox(ctx, provider,
    sandbox.WithIdleTimeout(10*time.Minute),          // auto-suspend after 10 min idle (default: 5 min)
    sandbox.WithCleanup(sandbox.CleanupDisabled),     // sandbox survives parent workflow close
    sandbox.WithSnapshot(snap),                       // start from a previously taken snapshot
    sandbox.WithTaskQueue(sandbox.DefaultSandboxTaskQueue), // run the sandbox on a dedicated sandbox worker
)
```

| Option | Default | Notes |
|--------|---------|-------|
| `WithIdleTimeout(d)` | 5 minutes | `0` means the default, not "disabled". Pass `sandbox.NoIdleTimeout` to never auto-suspend. Other negative values make `NewSandbox` return an error. |
| `WithCleanup(b)` | `CleanupWithWorkflow` | `CleanupWithWorkflow` cancels the sandbox when the creating workflow closes. `CleanupDisabled` leaves it running; something must eventually call `Stop` or `RequestStop`. |
| `WithSnapshot(snap)` | fresh start | Start from a snapshot returned by `Snapshot`. |
| `WithTaskQueue(name)` | parent's task queue | See [Dedicated sandbox worker](#dedicated-sandbox-worker). `DefaultSandboxTaskQueue` is `"temporal-sandbox"`. |

### Running commands

```go
result, err := sbx.ExecuteCommand(ctx, "ls -la /tmp")
if err != nil {
    return err // the command could not be run
}
if result.ExitCode != 0 {
    // the command ran and failed; see result.Stderr
}
```

`ExecuteCommand(ctx, cmd, opts...)` runs `cmd` in the sandbox, blocks until it finishes, and returns a `*CommandResult` with `Stdout`, `Stderr` and `ExitCode`.

- **A non-zero exit code is not an error.** `err` is reserved for failures to run the command at all (sandbox gone, provider error, invalid state). Check `ExitCode` yourself.
- **The command is a shell string**, but the shell depends on the provider: Modal runs `sh -c`, E2B runs `bash -c`, and Daytona, AgentCore and GKE pass the string to their own command APIs. Stick to POSIX `sh` syntax for portability.
- **Each call is independent.** Files persist between calls, but shell state (working directory, environment variables) does not; use `cd /dir && ...` within one command.
- **Each call resets the idle timer** (see `WithIdleTimeout`). If the sandbox is suspended, it is resumed first unless you pass `DisableAutoResume()` (see below).
- **Commands may run more than once.** The provider call runs in an activity with a 10-minute start-to-close timeout and Temporal's default retry policy, so a transient failure or a command running past 10 minutes is retried. Prefer idempotent commands, and keep each one under 10 minutes.

### Suspend and resume

```go
err = sbx.Suspend(ctx)  // explicit suspend
err = sbx.Resume(ctx)   // explicit resume
```

By default, `ExecuteCommand` transparently resumes a suspended sandbox before running the command. To receive an error instead:

```go
result, err := sbx.ExecuteCommand(ctx, "ls", sandbox.DisableAutoResume())
```

### Snapshot and fork

`Snapshot` captures the sandbox filesystem state and returns a `*compute.ProviderSnapshot`. Pass it to `WithSnapshot` when creating a new sandbox to start from that state:

```go
snap, err := origin.Snapshot(ctx)

forkA, err := sandbox.NewSandbox(ctx, provider, sandbox.WithSnapshot(snap))
forkB, err := sandbox.NewSandbox(ctx, provider, sandbox.WithSnapshot(snap))
```

Each fork is fully independent — writes in one are invisible to the others and to the origin. The `SandboxPostSnapshotState` returned by the provider indicates whether the origin sandbox is still running, was suspended, or was deleted as a side-effect of snapshotting.

`Snapshot` is only valid while the sandbox is running; it returns an error if the sandbox is pending, suspended, failed or deleted.

Snapshots you take belong to you: the SDK never deletes them, even when the sandbox stops. Delete one through any live sandbox of the same provider once you no longer need it:

```go
err = sbx.DeleteSnapshot(ctx, snap)
```

Deleting the snapshot a sandbox is currently suspended on fails with a `SnapshotInUse` error.

### Sandbox references

A sandbox reference is an opaque string that lets a child or sibling workflow route commands to an existing sandbox without taking ownership of its lifecycle:

```go
ref, err := sbx.Ref()  // in the creator workflow

// in another workflow:
sbx, err := sandbox.AttachToSandbox(ref)
result, err := sbx.ExecuteCommand(ctx, "ls")
```

`AttachToSandbox` returns a `Sandbox`, which has every method except the blocking `Stop`; use `RequestStop` to shut the sandbox down from an attached workflow.

### Errors

Sandbox operations fail with Temporal `ApplicationError`s whose type identifies the cause, for example `SandboxNotFound`, `Suspended` (command sent with `DisableAutoResume` to a suspended sandbox), `AlreadySuspended`, `InvalidSandboxState` or `SnapshotInUse`. Check the type with `errors.As` and `(*temporal.ApplicationError).Type()`. The full list is in [docs/design/wire-contract.md](docs/design/wire-contract.md#errors).

### Worker registration

Call `sandbox.Register` once when setting up your worker. It registers `SandboxWorkflow` and all supporting activities:

```go
sandbox.Register(w, temporalClient)
```

`Register` is the combination of two narrower functions, for workers that need only one half:

| Function | Registers | Needs provider credentials |
|----------|-----------|:--------------------------:|
| `RegisterSandboxWorkflow(w)` | `SandboxWorkflow` and the lifecycle activities that call compute providers | yes |
| `RegisterClientActivities(w, c)` | `SendSandbox*` activities that forward requests to `SandboxWorkflow` as updates | no |

### Dedicated sandbox worker

By default the sandbox runs on the task queue of the workflow that created it, so that workflow's worker must call `Register` and hold the compute-provider credentials. With `WithTaskQueue`, the sandbox child workflow and the `SendSandbox*` activities run on a separate queue instead, served by the standalone sandbox worker:

```sh
make sdk-sandbox-worker
SANDBOX_TASK_QUEUE=temporal-sandbox ./sdk/sandbox-worker   # SANDBOX_TASK_QUEUE is optional; this is the default
```

It connects using the same `TEMPORAL_HOST_PORT`, `TEMPORAL_NAMESPACE` and `TEMPORAL_API_KEY` variables as the examples. Workers for workflows that use `WithTaskQueue` then register only their own workflows. Sandbox references created this way carry the task queue, so `AttachToSandbox` routes to it automatically.

The dedicated worker is also how workflows written in other languages use sandboxes. The payloads they exchange with it are specified in [docs/design/wire-contract.md](docs/design/wire-contract.md).

## Compute providers

Five providers are included. All implement `compute.Provider` and self-register via `init()`. Methods not supported by a provider return `errors.ErrUnsupported`.

| Provider | Type constant | Blank-import |
|----------|--------------|--------------|
| E2B | `compute.ProviderTypeE2B` | `sdk/compute/e2b` |
| Daytona | `compute.ProviderTypeDaytona` | `sdk/compute/daytona` |
| AgentCore Runtime | `compute.ProviderTypeAgentCoreRuntime` | `sdk/compute/agentcore` |
| Modal | `compute.ProviderTypeModal` | `sdk/compute/modal` |
| GKE Agent Sandbox | `compute.ProviderTypeGKEAgentSandbox` | `sdk/compute/gkeagentsandbox` |

### Feature coverage

| Provider | Start | Stop | Suspend | Resume | ExecuteCommand | Snapshot | StartFromSnapshot |
|----------|:-----:|:----:|:-------:|:------:|:--------------:|:--------:|:-----------------:|
| E2B | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Daytona | ✓ | ✓ | ✓ | ✓ | ✓ | | |
| AgentCore Runtime | ✓ | ✓ | ✓ | ✓ | ✓ | | |
| Modal | ✓ | ✓ | | | ✓ | ✓ | ✓ |
| GKE Agent Sandbox | ✓ | ✓ | | | ✓ | ✓† | ✓† |

Providers without native Suspend/Resume (Modal, GKE) automatically get suspend support through the snapshot fallback: when the SDK's idle-timeout or explicit `Suspend` call hits `ErrUnsupported`, the workflow snapshots the sandbox, stops it, and later restores it via `StartFromSnapshot`. No extra configuration is required — the fallback is transparent as long as the provider supports both `Snapshot` and `StartFromSnapshot`.

† GKE snapshots use the `podsnapshot.gke.io` CRD and require gVisor on the cluster. `Snapshot` checkpoints the pod and suspends it (scales to 0); `StartFromSnapshot` resumes the same pod (the controller restores from the checkpoint). True forking — multiple independent sandboxes from one snapshot — is not supported by the GKE PodSnapshot API.

### Credentials

No credentials are stored in this repository. Each process reads what it needs from its environment or from the vendor's standard config files.

**Temporal** — every worker and starter, and `sdk/sandbox-worker`:

| Variable | Description |
|----------|-------------|
| `TEMPORAL_HOST_PORT` | Server address (default: `localhost:7233`) |
| `TEMPORAL_NAMESPACE` | Namespace (default: `default`) |
| `TEMPORAL_API_KEY` | If set, connect with this API key over TLS (Temporal Cloud) |

**Compute providers** — only the worker that registers `SandboxWorkflow` (via `Register` or `RegisterSandboxWorkflow`) needs these, because providers are constructed inside the sandbox lifecycle activities. With a [dedicated sandbox worker](#dedicated-sandbox-worker), that is only `sdk/sandbox-worker`.

| Provider | Credentials |
|----------|-------------|
| Modal | `MODAL_TOKEN_ID` and `MODAL_TOKEN_SECRET`, or the active profile in `~/.modal.toml` (written by `modal setup`) |
| E2B | `E2B_API_KEY` (required) |
| Daytona | `DAYTONA_API_KEY`, or `DAYTONA_JWT_TOKEN` with `DAYTONA_ORGANIZATION_ID`; optionally `DAYTONA_API_URL` |
| AgentCore Runtime | The standard AWS credential chain: `AWS_*` environment variables, `~/.aws` profiles (`AWS_PROFILE`), SSO, or an instance/task role |
| GKE Agent Sandbox | The in-cluster service account, otherwise `$KUBECONFIG` or `~/.kube/config` |

Do not put secrets in a provider's `Config` map: it is passed as workflow and activity input and stored in plain text in Temporal's event history.

### Provider configuration

**E2B** — `compute.ProviderTypeE2B`

| Key | Description |
|-----|-------------|
| `template-id` | E2B sandbox template ID. API key is read from `E2B_API_KEY`. |
| `timeout` | Sandbox timeout in seconds (default: 3600) |

**Daytona** — `compute.ProviderTypeDaytona`

| Key | Description |
|-----|-------------|
| `image` | Docker image to run (required) |
| `region` | Daytona target region (optional) |

**AgentCore Runtime** — `compute.ProviderTypeAgentCoreRuntime`

| Key | Description |
|-----|-------------|
| `agent-runtime-arn` | ARN of the AgentCore Runtime to invoke |

**Modal** — `compute.ProviderTypeModal`

| Key | Description |
|-----|-------------|
| `image` | Container image reference (required) |
| `app-name` | Modal app name (default: `temporal-sandbox-example`) |

**GKE Agent Sandbox** — `compute.ProviderTypeGKEAgentSandbox`

| Key | Description |
|-----|-------------|
| `template` | Agent Sandbox template name (required) |
| `namespace` | Kubernetes namespace (default: `default`) |

Snapshot operations require the `podsnapshot.gke.io/v1alpha1` CRDs and a gVisor-enabled node pool. Kubernetes credentials are resolved from the in-cluster service account or `$KUBECONFIG`.

### Adding a provider

1. Create a package under `sdk/compute/<name>/`.
2. Implement `compute.Provider`. Return `errors.ErrUnsupported` for unimplemented operations.
3. Call `compute.Register(myType, constructor)` in an `init()` function.
4. Blank-import the package wherever you register workers.

## Examples

Each example is a self-contained Go module with a `starter` binary and a `worker` binary.

| Example | What it shows |
|---------|---------------|
| `examples/file-management` | Sequential shell commands (create, read, list files); sandbox runs on the dedicated sandbox worker |
| `examples/auto-suspend` | Auto-suspend via `WithIdleTimeout`; file persists across suspend/resume |
| `examples/explicit-suspend-resume` | Manual `Suspend` and `Resume` calls |
| `examples/shared-sandbox` | Two child workflows sharing one sandbox via `Ref`/`AttachToSandbox` |
| `examples/detached-sandbox` | `CleanupDisabled` sandbox handed off to an independent workflow |
| `examples/snapshot-fork` | Snapshot an origin sandbox then branch two independent forks from it |

### Running an example

**Prerequisites**

- Go 1.26+
- A running Temporal server (`temporal server start-dev`)
- Credentials for the compute provider used by the example (all examples use Modal; see [Credentials](#credentials))

**Build**

```sh
make bins   # builds starter and worker binaries for all examples, plus sdk/sandbox-worker
make test   # runs the SDK tests
```

Or build a single example:

```sh
cd examples/file-management && go build -o starter ./cmd/starter && go build -o worker ./cmd/worker
```

**Run**

`file-management` uses a dedicated sandbox worker, so start that first; it holds the compute-provider credentials:

```sh
./sdk/sandbox-worker
```

The other examples run the sandbox on their own worker and do not need it.

Start the example's worker:

```sh
./examples/file-management/worker
```

Start the workflow:

```sh
./examples/file-management/starter
```

## Repository layout

```
sdk/
  sandbox.go              # Sandbox interface, NewSandbox, AttachToSandbox, options
  sandbox_activity.go     # Register*(), SendSandbox* activities
  contract_test.go        # checks Go payload types against contract/fixtures
  cmd/sandbox-worker/     # standalone worker for a dedicated sandbox task queue
  compute/
    provider.go           # Provider interface, types, constants
    registry.go           # Register / Lookup
    agentcore/provider.go # AWS AgentCore Runtime provider
    daytona/provider.go   # Daytona provider
    e2b/provider.go       # E2B provider
    modal/provider.go     # Modal provider
    gkeagentsandbox/      # GKE Agent Sandbox provider
  workflow/
    interface.go          # Update/signal names, timeout constants, shared types
    workflow.go           # SandboxWorkflow
    activities.go         # StartSandbox, StopSandbox, Suspend/Resume/Snapshot/ExecuteCommand

examples/
  auto-suspend/
  detached-sandbox/
  explicit-suspend-resume/
  file-management/
  shared-sandbox/
  snapshot-fork/

contract/fixtures/        # canonical JSON payloads shared by all language clients

docs/
  decisions/              # ADRs
  design/                 # wire contract
  plans/                  # implementation plans
```
