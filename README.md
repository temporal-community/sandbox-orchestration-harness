# Temporal Sandbox SDK

A framework for running Temporal activities inside ephemeral, isolated compute environments. Workflow developers use the SDK to transparently proxy activity calls into a dynamically-provisioned sandbox (AWS Lambda, ECS Fargate, or AgentCore Runtime). The sandbox is started on first use and stopped when the workflow is done, with its full lifecycle managed by a dedicated backend workflow.

## Architecture

The repository is a Go workspace with three modules:

| Module | Role |
|--------|------|
| `sdk/` | Public API for workflow developers — `NewSandbox`, `ExecuteActivity`, `Stop` |
| `backend/` | Sandbox lifecycle management — `SandboxWorkflow`, compute provider registry |
| `consumer/` | Example application — `FileOpsWorkflow` and all worker binaries |

### How it works

1. A workflow calls `sandbox.NewSandbox(ctx, computeProvider)` from the SDK.
2. The SDK starts a **child workflow** (`SandboxWorkflow`) on the backend task queue, using a UUID as the workflow ID.
3. Once the child workflow is running, the SDK sends a `sandbox-init` update containing the compute provider config.
4. The backend workflow executes the `StartSandbox` activity, which looks up the registered compute provider and provisions an ephemeral worker (Lambda invocation, ECS task, or AgentCore session). The worker is told to listen on the task queue `sandbox-<uuid>`.
5. The SDK routes subsequent `ExecuteActivity` calls to `sandbox-<uuid>`, where the ephemeral worker picks them up.
6. When the workflow calls `sbx.Stop(ctx)`, a `sandbox-stop` signal is sent to `SandboxWorkflow`, which executes the `StopSandbox` activity to tear down the compute instance.

```
Parent workflow
  │
  ├─ NewSandbox() ──► starts SandboxWorkflow (backend task queue)
  │                        │
  │                        ├─ sandbox-init update
  │                        │    └─ StartSandbox activity
  │                        │         └─ Lambda.Invoke / ECS.RunTask / AgentCore.InvokeAgentRuntime
  │                        │              └─ ephemeral worker on sandbox-<uuid>
  │                        │
  ├─ ExecuteActivity() ──► routed to sandbox-<uuid> task queue
  │
  └─ Stop() ──────────► sandbox-stop signal
                              └─ StopSandbox activity
                                   └─ (no-op / ECS.StopTask / AgentCore.StopRuntimeSession)
```

## Compute providers

Three providers are included. All implement the `compute.ComputeProvider` interface in `backend/compute/`:

| Provider | Type constant | Start | Stop |
|----------|--------------|-------|------|
| AWS Lambda | `aws-lambda` | Async `Invoke` with `{"taskQueue": "..."}` payload | No-op |
| AWS ECS (Fargate) | `aws-ecs` | `RunTask` with `TQ_NAME` env override, waits for RUNNING | `StopTask`, waits for STOPPED |
| AWS AgentCore Runtime | `aws-agentcore` | `InvokeAgentRuntime` with `{"taskQueue": "..."}` payload | `StopRuntimeSession` |

Providers self-register via `init()` and are blank-imported in the backend worker.

### Provider configuration

Each provider is configured via the `Config map[string]string` field of `sdk/compute.ComputeProvider`.

**`aws-lambda`**

| Key | Description |
|-----|-------------|
| `function-arn` | ARN of the Lambda function to invoke |

**`aws-ecs`**

| Key | Description |
|-----|-------------|
| `cluster` | ECS cluster name or ARN |
| `task-definition` | Task definition name or ARN |
| `subnet-ids` | Comma-separated list of subnet IDs |
| `security-group-ids` | Comma-separated list of security group IDs |
| `assign-public-ip` | Set to `"true"` to assign a public IP (default: disabled) |

See [`task-definition.json`](task-definition.json) for an example task definition. Register it with `aws ecs register-task-definition --cli-input-json file://task-definition.json`.

**`aws-agentcore`**

| Key | Description |
|-----|-------------|
| `agent-runtime-arn` | ARN of the AgentCore Runtime to invoke |

### Adding a new provider

1. Create a package under `backend/compute/<name>/`.
2. Implement `compute.ComputeProvider` (`Start` and `Stop`).
3. Call `backendcompute.Register(myType, constructor)` in an `init()` function.
4. Blank-import the package in `backend/cmd/worker/main.go`.
5. Add the new type constant to `sdk/compute/provider.go`.

## Running the example

### Prerequisites

- Go 1.25+
- A running Temporal server (`temporal server start-dev`)
- AWS credentials configured (for Lambda or ECS providers)
- [`ko`](https://ko.build) (for the ECS container image target)

### Build

```sh
make bins                  # build workflow-worker, workflow-starter, backend-worker, sandbox-worker-lambda
make sandbox-worker-ecs    # build ECS container image with ko
```

### Run

Start both workers with [Foreman](https://github.com/ddollar/foreman) (or a compatible tool such as [Hivemind](https://github.com/DarthSim/hivemind) or [Overmind](https://github.com/DarthSim/overmind)):

```sh
foreman start
```

The sandbox worker runs remotely — deploy it before starting workflows:

- **Lambda**: deploy `consumer/sandbox-worker-lambda.zip` to AWS Lambda, configured via function ARN
- **ECS**: deploy the container image built with `make sandbox-worker-ecs` as a Fargate task definition
- **AgentCore**: deploy the container image built with `ko build ./consumer/cmd/sandbox-worker-agentcore` as an AgentCore Runtime

Start the example workflow:

```sh
./consumer/workflow-starter
```

## Repository layout

```
sdk/
  sandbox.go              # Sandbox interface and NewSandbox factory
  sandbox_activity.go     # SendSandboxInit helper activity
  compute/provider.go     # ComputeProvider type and constants
  workflow/interface.go   # Shared constants and types

backend/
  workflow.go             # SandboxWorkflow
  activities.go           # StartSandbox, StopSandbox
  compute/
    provider.go           # ComputeProvider interface
    registry.go           # Register / Lookup
    lambda/provider.go    # Lambda provider
    ecs/provider.go       # ECS Fargate provider
    agentcore/provider.go # AgentCore Runtime provider
  cmd/worker/main.go      # Backend worker entry point

consumer/
  workflow.go             # FileOpsWorkflow example
  activities.go           # CreateFile, ReadFile, ListFiles
  cmd/
    worker/main.go              # Consumer workflow worker
    starter/main.go             # Workflow starter CLI
    sandbox-worker-lambda/      # Lambda sandbox worker (bootstrap binary)
    sandbox-worker-ecs/         # ECS sandbox worker
    sandbox-worker-agentcore/   # AgentCore Runtime sandbox worker
```
