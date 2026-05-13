# CoreWeave Sandboxes provider

This provider runs sandbox workloads on
[CoreWeave Sandboxes](https://docs.coreweave.com/products/sandboxes), the
serverless container platform built on top of CKS. It talks to the public
Sandbox Gateway API (`api.cwsandbox.com`) using the Connect/Go SDK generated
from [`buf.build/coreweave/sandbox`](https://buf.build/coreweave/sandbox).

## Provider type

```go
import _ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/cwsandbox"

// Use compute.ProviderTypeCoreWeaveSandbox ("coreweave-sandbox") when configuring the harness.
```

## Authentication

The provider authenticates with a Bearer token read from the environment:

| Env var                 | Description                                         |
| ----------------------- | --------------------------------------------------- |
| `CWSANDBOX_API_TOKEN`   | API token for `api.cwsandbox.com`. **Required.**    |

Generate tokens from the CoreWeave Cloud Console.

## Config keys

Configuration is passed via `compute.ProviderDetails.Config` (a
`map[string]string`).

| Key                     | Required | Description                                                                                  |
| ----------------------- | -------- | -------------------------------------------------------------------------------------------- |
| `container-image`       | yes      | Image reference for the sandbox container (e.g. `ghcr.io/your-org/worker:latest`).           |
| `command`               | yes      | Entrypoint command. JSON array (`["python", "-m", "worker"]`) or whitespace-split string.    |
| `args`                  | no       | Additional arguments to append to `command`. Same JSON-or-whitespace format.                 |
| `profile-names`         | no       | Comma-separated list of profile names that the sandbox is allowed to schedule against.       |
| `max-lifetime-seconds`  | no       | Optional TTL for the sandbox. Maps to `max_lifetime_seconds` on `StartSandboxRequest`.       |
| `base-url`              | no       | Override the API endpoint. Defaults to `https://api.cwsandbox.com`.                          |

## Operation mapping

| Harness method        | Gateway RPC      | Notes                                                                              |
| --------------------- | ---------------- | ---------------------------------------------------------------------------------- |
| `Start`               | `Start`          | Sets `TEMPORAL_TASK_QUEUE` on the sandbox env so the worker can poll the queue.    |
| `Stop`                | `Delete`         | Idempotent: a `NotFound` response is treated as success.                           |
| `Suspend`             | `Pause`          |                                                                                    |
| `Resume`              | `Resume`         |                                                                                    |
| `ExecuteCommand`      | `Exec`           | Wraps the supplied shell string with `sh -c` to preserve quoting and pipes.        |
| `Snapshot`            | _unsupported_    | Returns `errors.ErrUnsupported`. The public v1beta2 API does not expose snapshots. |
| `StartFromSnapshot`   | _unsupported_    | Same as above.                                                                     |
| `DeleteSnapshot`      | _unsupported_    | Same as above.                                                                     |

Workflows that need pause/resume semantics should rely on `Suspend` and
`Resume` directly. Snapshot-based suspend/resume is opt-in on a per-workflow
basis in the harness and will fall through to suspend when the provider
reports `ErrUnsupported`.

## Example

```go
package main

import (
    "context"
    "log"

    "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
    _ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/cwsandbox"
)

func main() {
    provider, err := compute.Lookup(
        compute.ProviderTypeCoreWeaveSandbox,
        map[string]string{
            "container-image": "ghcr.io/your-org/worker:latest",
            "command":         `["python", "-m", "worker"]`,
            "profile-names":   "default",
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    status, err := provider.Start(context.Background(), "my-task-queue")
    if err != nil {
        log.Fatal(err)
    }
    defer provider.Stop(context.Background(), status)

    log.Printf("sandbox %s running", status.InstanceID)
}
```

## References

- [CoreWeave Sandboxes documentation](https://docs.coreweave.com/products/sandboxes)
- [Public API schema on the Buf Schema Registry](https://buf.build/coreweave/sandbox)
