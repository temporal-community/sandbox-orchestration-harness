# Baseline replay fixtures

`baseline-parent.json` and `baseline-controller.json` are complete histories captured from a local Temporal development server (1.32.0) with Go SDK 1.42.0. The production source was unchanged at harness revision `24f09129335f8057750f09f84a45b9c76c99d9c1`. Only `history_test.go` was added to that checkout to run the capture. The fake provider allocates no compute and uses synthetic client and Worker identities. These fixtures record existing behavior at that revision, not a deprecated execution mode.

The parent calls `NewSandbox`, runs one command, and stops the owned controller. The controller history retains `StartSandbox`, `ExecuteCommand`, and `StopSandbox`. The parent history retains the SDK bridge Activities and SideEffect markers. Sticky task queue names are replaced with `synthetic-dev-server` to remove the local host name. No command, input, event order, or result is changed. `TestRetainedBaselineReplay` replays both histories against the current implementation without a server. The recorded `legacyHistoryWorkflow` type name stays unchanged so replay uses the original Workflow registration.

The `baseline-smolcloud-*.json` fixtures are parent and controller histories for four SmolCloud configurations, captured at the same revision with the same server and SDK versions. Only `history_test.go` and `smolcloud_history_test.go` were added to that checkout. A local fake HTTP endpoint answers provider requests, so no cloud resource is used. The configurations are a plain image, a missing image, an opaque `workload` key and a CPU value above the `int32` range. Each history records a run that started before the `lifecycle` and `branchable` settings existed. `TestRetainedSmolCloudBaselineReplay` replays them against the current implementation, so Workflow code must not reject or reinterpret configuration that the provider constructor owns. Sticky task queue names are replaced with `synthetic-dev-server` in the same way.

`baseline-fresh-controller.json` is captured from unchanged production source at `d8a7d5b8b3782e26ff803678b097eed548c8c8ac`, using `TestLocalProcessWithoutWorkerHistory/fresh-policy`, Go SDK 1.42.0 and Temporal CLI 1.9.1 (Server 1.32.0). It records a fake provider with fresh lifecycle before the initialization cancellation version marker. `TestRetainedFreshControllerReplay` registers that recorded policy and replays the complete controller history. Worker identities and sticky queue names use synthetic values. Commands, inputs, results and event order are unchanged.

`baseline-fresh-canceled-controller.json` records cancellation while fresh initialization is pending, before the controller receives the fake provider's successful allocation result. It was captured on the same local server and SDK versions with the exact controller source from `d8a7d5b8b3782e26ff803678b097eed548c8c8ac`, restored in an isolated candidate checkout. The capture-only test releases the delayed result after the controller processes cancellation. This is a compatibility fixture for the old controller's cancellation behavior, not evidence of cleanup by that controller. Only Worker/client identity fields and sticky task queue names were normalized to synthetic values; inputs, results, commands, event order and ordinary task queue names remain unchanged. `TestRetainedFreshControllerReplay` registers both recorded fake fresh-policy types and replays both complete histories without a server, including on repeated test runs. The other eleven fixtures are retained unchanged.

To capture another baseline, start a local server at `127.0.0.1:17293` and run the following command from that baseline's `sdk` directory with the capture test present. Set `HARNESS_HISTORY_CAPTURE` to an existing output directory. Keep the original fixtures when checking a new implementation.

```sh
GOWORK=off HARNESS_HISTORY_CAPTURE=<output-directory> go test -run '^TestCapture(SmolCloud)?BaselineHistory$' -count=1 -v .
```

Local verification uses the Temporal CLI development server (1.32.0), without Docker or cloud resources. With the Temporal CLI installed, run this command in a separate terminal and wait for the server to report that it has started:

```sh
temporal server start-dev --ip 127.0.0.1 --port 17293 --headless
```

To test fresh configured process execution through the existing start path, run the following command from `sdk/` while that server is running:

```sh
GOWORK=off HARNESS_LOCAL_TEST=1 go test -run '^TestLocalProcessWithoutWorkerHistory$' -count=1 -v .
```

Stop the development server with Ctrl+C after the tests finish.

The SDK tests workflow runs the full SDK suite and vet on a GitHub hosted Ubuntu runner with a pinned Temporal CLI. It starts the same headless local server, enables fresh execution tests and requires the retained histories to replay. Live cloud tests and fixture regeneration stay disabled. No Docker or cloud provider credentials are required.

This test uses a fake provider that runs a bounded local process without a Temporal Worker in the resource. It retries a simulated lost launch reply, checks retained configuration and resource identity, verifies the existing Activity selection, and replays fresh parent and controller histories with their original execution identities. Default and explicit `NoIdleTimeout` cases prove compatibility, not lifecycle policy enforcement. Concrete provider configuration and adoption, lifecycle policy and a complete lifecycle example need separate proof. No cloud resource or model call is used.
