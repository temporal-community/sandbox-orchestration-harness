// Package crafting implements the compute.Provider interface on top of
// Crafting sandboxes.
//
// Crafting sandboxes are production-like environments: a workspace plus the
// dependency workloads it talks to, such as databases and caches. That shape is
// what the provider exposes to agent workflows, and it is why snapshots here are
// composite rather than a single filesystem image. A fork inherits the agent's
// files and its database together, so two candidate changes can run in parallel
// without either seeing the other's rows.
//
// Activate the provider with a blank import and select it by type:
//
//	import _ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/crafting"
//
//	sbx, err := sandbox.NewSandbox(ctx, compute.ProviderDetails{
//	    Type: compute.ProviderTypeCrafting,
//	    Config: map[string]string{
//	        "template":     "agent-sandbox",
//	        "workspace":    "dev",
//	        "dependencies": "db",
//	    },
//	})
//
// Sandboxes are driven through the cs CLI, which must be available on the
// worker host.
package crafting

import (
	"context"
	"fmt"
	"strings"

	crafting "github.com/crafting-demo/lightweight-go-client"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func init() {
	compute.Register(compute.ProviderTypeCrafting, New)
}

type provider struct {
	client *crafting.Client
	cfg    *config
}

var _ compute.Provider = (*provider)(nil)

// New constructs a provider from harness-supplied configuration. It matches
// compute.Constructor so it can be registered directly.
func New(raw map[string]string) (compute.Provider, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	client, err := crafting.NewClient(cfg.client)
	if err != nil {
		return nil, err
	}
	return &provider{client: client, cfg: cfg}, nil
}

// Start provisions a sandbox from the configured Template. taskQueueName is
// injected as TEMPORAL_TASK_QUEUE so the worker inside the sandbox polls the
// queue the harness expects.
func (p *provider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	return p.create(ctx, taskQueueName, nil)
}

// StartFromSnapshot provisions a sandbox restored from a composite snapshot, so
// it comes up with the same home directory and the same dependency data the
// origin had. Each call yields an independent sandbox, which is what makes
// forking possible.
func (p *provider) StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	composite, err := decode(snapshot)
	if err != nil {
		return nil, err
	}
	return p.create(ctx, taskQueueName, composite)
}

func (p *provider) create(ctx context.Context, taskQueueName string, from *crafting.CompositeSnapshot) (*compute.ProviderStatus, error) {
	if taskQueueName == "" {
		return nil, fmt.Errorf("crafting: empty task queue name")
	}

	// The name is derived from the task queue, which is stable for the life of a
	// sandbox workflow, so a retried Start resolves to the same sandbox instead
	// of creating a duplicate. The snapshot joins the seed so that restoring a
	// different snapshot for the same workflow yields a distinct sandbox.
	seed := taskQueueName
	if from != nil {
		seed += "|" + strings.Join(from.Components(), ",")
	}
	name, err := crafting.DeriveSandboxName(p.cfg.namePrefix, seed)
	if err != nil {
		return nil, err
	}

	ref, err := p.client.CreateSandbox(ctx, crafting.CreateSandboxOptions{
		Name:        name,
		Template:    p.cfg.template,
		Env:         append([]string{"TEMPORAL_TASK_QUEUE=" + taskQueueName}, p.cfg.extraEnv...),
		From:        from,
		UsePool:     p.cfg.usePool,
		Region:      p.cfg.region,
		WaitTimeout: p.cfg.createTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &compute.ProviderStatus{InstanceID: ref.String()}, nil
}

// Stop removes the sandbox. A sandbox that is already gone is treated as
// success, so retried teardown does not fail a workflow.
func (p *provider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	ref, err := p.target(status)
	if err != nil {
		return err
	}
	return p.client.DeleteSandbox(ctx, ref)
}

// Suspend releases the sandbox's compute while preserving its state. Crafting
// suspends natively, so the harness never needs its snapshot-based fallback.
func (p *provider) Suspend(ctx context.Context, status *compute.ProviderStatus) error {
	ref, err := p.target(status)
	if err != nil {
		return err
	}
	return p.client.SuspendSandbox(ctx, ref)
}

// Resume brings a suspended sandbox back online and does not return until the
// workspace can actually accept commands, since the caller's next act is to run
// one.
func (p *provider) Resume(ctx context.Context, status *compute.ProviderStatus) error {
	ref, err := p.target(status)
	if err != nil {
		return err
	}
	return p.client.ResumeSandbox(ctx, ref, p.resumeOptions())
}

// Snapshot captures the sandbox as one opaque id. The sandbox keeps running, so
// the workflow gets a checkpoint without losing the environment it just built.
func (p *provider) Snapshot(ctx context.Context, status *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	ref, err := p.target(status)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}
	composite, err := p.client.SnapshotSandbox(ctx, ref, crafting.SnapshotOptions{
		Workspace:    p.cfg.workspace,
		Dependencies: p.cfg.dependencies,
		IncludeBase:  p.cfg.snapshotBase,
		HomeIncludes: p.cfg.homeIncludes,
		HomeExcludes: p.cfg.homeExcludes,
		Folder:       p.cfg.snapshotFolder,
		UID:          p.cfg.execUID,
		Timeout:      p.cfg.snapshotTimeout,
	})
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}
	// The harness carries the snapshot through workflow history as a string, so
	// every component is packed into one id.
	id, err := composite.Encode()
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}
	return compute.SandboxPostSnapshotRunning, &compute.ProviderSnapshot{SnapshotID: id}, nil
}

// DeleteSnapshot removes every component of a composite snapshot.
func (p *provider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) error {
	composite, err := decode(snapshot)
	if err != nil {
		return err
	}
	return p.client.DeleteSnapshot(ctx, composite)
}

// ExecuteCommand runs cmd inside the sandbox's workspace.
//
// A command that runs and fails is a result, not an error: it is returned as a
// CommandResult with a non-zero ExitCode so the agent workflow can react to it.
// An error is reserved for the case where the command could not be run at all,
// which Temporal's activity retries then handle.
func (p *provider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	ref, err := p.target(status)
	if err != nil {
		return nil, err
	}
	res, err := p.client.Exec(ctx, ref, crafting.ExecOptions{
		Workload: p.cfg.workspace,
		UID:      p.cfg.execUID,
		Dir:      p.cfg.execDir,
		Timeout:  p.cfg.commandTimeout,
		// cs would resume a suspended sandbox on its own, but it reports that
		// progress on stderr, which would land in the command's own Stderr.
		AutoResume: true,
	}, cmd)
	if err != nil {
		return nil, err
	}
	return &compute.CommandResult{
		Stdout:   res.Stdout,
		Stderr:   res.Stderr,
		ExitCode: int32(res.ExitCode),
	}, nil
}

func (p *provider) target(status *compute.ProviderStatus) (crafting.SandboxRef, error) {
	if status == nil || status.InstanceID == "" {
		return crafting.SandboxRef{}, fmt.Errorf("crafting: missing instance id")
	}
	return p.client.Ref(status.InstanceID), nil
}

func (p *provider) resumeOptions() crafting.ResumeOptions {
	return crafting.ResumeOptions{Workload: p.cfg.workspace, UID: p.cfg.execUID}
}

func decode(snapshot *compute.ProviderSnapshot) (*crafting.CompositeSnapshot, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("crafting: nil snapshot")
	}
	return crafting.DecodeCompositeSnapshot(snapshot.SnapshotID)
}
