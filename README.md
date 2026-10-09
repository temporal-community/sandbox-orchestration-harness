# Temporal Sandbox Orchestration Example

A Go example for running shell commands inside ephemeral, isolated compute environments from Temporal workflows. Workflow developers create a sandbox, run commands in it, and let the SDK handle provisioning, suspend/resume, snapshot/fork, and teardown — all driven by a long-lived child workflow that manages the sandbox lifecycle.

## Architecture

The repository is a Go workspace with one library module and several example modules:

| Module | Role |
|--------|------|
| `sdk/` | Public API — `NewSandbox`, `ExecuteCommand`, `Suspend`, `Resume`, `Snapshot`, `Stop`, `AttachToSandbox` |
| `examples/*/` | Runnable example workflows |

### How it works

1. A workflow calls `sandbox.NewSandbox(ctx, computeProvider)`.
2. The SDK starts a **child workflow** (`SandboxWorkflow`) using a UUID as the workflow ID.
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
sbx, err := sandbox.NewSandbox(ctx, compute.ProviderDetails{
    Type:   compute.ProviderTypeE2B,
    Config: map[string]string{"template-id": "base"},
})

result, err := sbx.ExecuteCommand(ctx, "echo hello")
// result.Stdout, result.Stderr, result.ExitCode

err = sbx.Stop(ctx)
```

### Options

```go
sandbox.NewSandbox(ctx, provider,
    sandbox.WithIdleTimeout(10*time.Minute),          // auto-suspend after 10 min idle (default: 5 min)
    sandbox.WithCleanup(sandbox.CleanupDisabled),     // sandbox survives parent workflow close
    sandbox.WithSnapshot(snap),                       // start from a previously taken snapshot
)
```

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

### Sandbox references

A sandbox reference is an opaque string that lets a child or sibling workflow route commands to an existing sandbox without taking ownership of its lifecycle:

```go
ref, err := sbx.Ref()  // in the creator workflow

// in another workflow:
sbx, err := sandbox.AttachToSandbox(ref)
result, err := sbx.ExecuteCommand(ctx, "ls")
```

### Worker registration

Call `sandbox.Register` once when setting up your worker. It registers `SandboxWorkflow` and all supporting activities:

```go
err := sandbox.Register(w, temporalClient)
```

## Compute providers

Seven providers are included. All implement `compute.Provider` and self-register via `init()`. Methods not supported by a provider return `errors.ErrUnsupported`.

| Provider | Type constant | Blank-import |
|----------|--------------|--------------|
| Crafting | `compute.ProviderTypeCrafting` | `sdk/compute/crafting` |
| Smol Machines Cloud | `compute.ProviderTypeSmolCloud` | `sdk/compute/smolcloud` |
| E2B | `compute.ProviderTypeE2B` | `sdk/compute/e2b` |
| Daytona | `compute.ProviderTypeDaytona` | `sdk/compute/daytona` |
| AgentCore Runtime | `compute.ProviderTypeAgentCoreRuntime` | `sdk/compute/agentcore` |
| Modal | `compute.ProviderTypeModal` | `sdk/compute/modal` |
| GKE Agent Sandbox | `compute.ProviderTypeGKEAgentSandbox` | `sdk/compute/gkeagentsandbox` |

### Feature coverage

| Provider | Start | Stop | Suspend | Resume | ExecuteCommand | Snapshot | StartFromSnapshot |
|----------|:-----:|:----:|:-------:|:------:|:--------------:|:--------:|:-----------------:|
| Crafting | ✓ | ✓ | ✓ | ✓ | ✓ | ✓‡ | ✓‡ |
| Smol Machines Cloud | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| E2B | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Daytona | ✓ | ✓ | ✓ | ✓ | ✓ | | |
| AgentCore Runtime | ✓ | ✓ | ✓ | ✓ | ✓ | | |
| Modal | ✓ | ✓ | | | ✓ | ✓ | ✓ |
| GKE Agent Sandbox | ✓ | ✓ | | | ✓ | ✓† | ✓† |

Providers without native Suspend/Resume (Modal, GKE) automatically get suspend support through the snapshot fallback: when the SDK's idle-timeout or explicit `Suspend` call hits `ErrUnsupported`, the workflow snapshots the sandbox, stops it, and later restores it via `StartFromSnapshot`. No extra configuration is required — the fallback is transparent as long as the provider supports both `Snapshot` and `StartFromSnapshot`.

‡ Crafting snapshots are composite: the workspace home directory, the data of each dependency named in `dependencies`, and optionally the workspace root filesystem, recorded under one opaque snapshot ID. Work must live under `$HOME` to be captured unless `snapshot-base` is enabled. Forks are fully independent in both files and data.

† GKE snapshots use the `podsnapshot.gke.io` CRD and require gVisor on the cluster. `Snapshot` checkpoints the pod and suspends it (scales to 0); `StartFromSnapshot` resumes the same pod (the controller restores from the checkpoint). True forking — multiple independent sandboxes from one snapshot — is not supported by the GKE PodSnapshot API.

### Provider configuration

**Smol Machines Cloud** — `compute.ProviderTypeSmolCloud`

Set `SMOL_CLOUD_TOKEN` on the Temporal activity worker and blank-import
`github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/smolcloud`
where the worker registers activities. The token is read from the worker's
environment, never from the workflow's provider config. An optional
`SMOL_CLOUD_URL` on the worker selects another control-plane endpoint.

```go
provider := sandbox.Provider{
    Type: compute.ProviderTypeSmolCloud,
    Config: map[string]string{
        "image": "python:3.12-alpine",
        "network": "open", // required for uncached image pulls; allows guest egress
    },
}
```

The `image` is required. Optional config keys are `cpus`, `memory-mb`, and
`disk-gb` (positive integers) and `network` (`blocked`, `open`, or
`allowCidrs`). The default size is 1 vCPU and 512 MB; machines are created
branchable so they can produce checkpoints.

Network access defaults to `blocked`. An uncached registry image cannot be
pulled in that mode. For restricted egress, set `network` to `allowCidrs` and
provide comma-separated `allow-cidrs` and/or `allow-hosts` entries, including
all registry hosts needed for the image pull. `open` allows unrestricted guest
egress. Snapshot returns a durable checkpoint while the source keeps running;
restore creates a separate VM. The cloud restore API currently enforces blocked
network access; the provider refuses to boot a restored VM when the requested
policy is different. If your workflow needs both network access and checkpoint
restores, wait for the cloud API to support a network policy on restore. HTTP
API calls run on the Temporal activity worker; the sandbox image does not need
a Temporal worker.

**Crafting** — `compute.ProviderTypeCrafting`

Requires the `cs` CLI on the worker host. A Crafting sandbox is a workspace plus the dependency workloads it talks to, so snapshots are composite: naming `dependencies` captures their data alongside the workspace home, and a fork restores both together.

The provider is a thin adapter over [`crafting-demo/lightweight-go-client`](https://github.com/crafting-demo/lightweight-go-client), a standalone Go client for Crafting sandboxes that carries the CLI mechanics and has no dependencies outside the standard library.

| Key | Description |
|-----|-------------|
| `template` | Crafting Template new sandboxes are created from (required) |
| `workspace` | Workspace workload used for commands and snapshots (required) |
| `dependencies` | Comma-separated dependency workloads whose data is part of a snapshot |
| `folder` / `snapshot-folder` / `org` / `region` | Placement (optional) |
| `use-pool` | Claim from a Sandbox Pool for faster starts (default: `auto`) |
| `snapshot-base` | Also capture the workspace root filesystem (default: `false`) |
| `home-includes` / `home-excludes` | Paths captured in a home snapshot (default: all of `$HOME`) |
| `exec-uid` / `exec-dir` | Identity and working directory for commands (default: uid 1000) |
| `token` / `config-dir` | Service-account token and isolated CLI state. Omit `token` when the worker runs inside a Crafting sandbox and already has credentials |

`sdk/compute/crafting/agent-sandbox.yaml` is a reference template with a workspace and a Postgres dependency. Create it once per organization:

```sh
cs template create agent-sandbox sdk/compute/crafting/agent-sandbox.yaml
```

To run the `database-fork` example against it:

```sh
export SANDBOX_PROVIDER=crafting
export SANDBOX_PROVIDER_CONFIG='{"template":"agent-sandbox","workspace":"dev","dependencies":"db"}'
```

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
| `examples/file-management` | Sequential shell commands (create, read, list files) |
| `examples/auto-suspend` | Auto-suspend via `WithIdleTimeout`; file persists across suspend/resume |
| `examples/explicit-suspend-resume` | Manual `Suspend` and `Resume` calls |
| `examples/shared-sandbox` | Two child workflows sharing one sandbox via `Ref`/`AttachToSandbox` |
| `examples/detached-sandbox` | `CleanupDisabled` sandbox handed off to an independent workflow |
| `examples/snapshot-fork` | Snapshot an origin sandbox then branch two independent forks from it |
| `examples/database-fork` | Snapshot a sandbox that holds database state as well as files, then apply a different migration in each fork |

### Running an example

**Prerequisites**

- Go 1.26+
- A running Temporal server (`temporal server start-dev`)
- Credentials for the compute provider used by the example

**Build**

```sh
make bins   # builds starter and worker binaries for all examples
```

Or build a single example:

```sh
cd examples/file-management && go build -o starter ./cmd/starter && go build -o worker ./cmd/worker
```

**Run**

Start the worker:

```sh
./examples/file-management/worker
```

Start the workflow:

```sh
./examples/file-management/starter
```

The `database-fork` example is provider-agnostic and reads its provider from the environment. The sandbox needs a `psql` client and the standard libpq variables (`PGHOST`, `PGUSER`, ...) pointing at a Postgres database. Forks only get isolated rows when the provider's snapshots include that database.

```sh
export SANDBOX_PROVIDER=<provider type>                     # required, e.g. the value of a compute.ProviderType* constant
export SANDBOX_PROVIDER_CONFIG='{"key":"value"}'            # provider configuration as a JSON object of strings
```

## Repository layout

```
sdk/
  sandbox.go              # Sandbox interface, NewSandbox, AttachToSandbox, WithSnapshot
  sandbox_activity.go     # Register(), SendSandbox* activities
  compute/
    provider.go           # Provider interface, types, constants
    registry.go           # Register / Lookup
    agentcore/provider.go # AWS AgentCore Runtime provider
    crafting/             # Crafting provider + reference sandbox template
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
  database-fork/
  detached-sandbox/
  explicit-suspend-resume/
  file-management/
  shared-sandbox/
  snapshot-fork/
```
