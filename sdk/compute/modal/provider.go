package modal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	modal "github.com/modal-labs/modal-client/go"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func init() {
	compute.Register(compute.ProviderTypeModal, newModalProvider)
}

func newModalProvider(config map[string]string) (compute.Provider, error) {
	image := config["image"]
	if image == "" {
		return nil, fmt.Errorf("modal: image required")
	}
	appName := config["app-name"]
	if appName == "" {
		appName = "temporal-sandbox-example"
	}
	client, err := modal.NewClient()
	if err != nil {
		return nil, fmt.Errorf("modal: create client: %w", err)
	}
	return &modalProvider{client: client, image: image, appName: appName}, nil
}

type modalProvider struct {
	client  *modal.Client
	image   string
	appName string
}

func (p *modalProvider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	app, err := p.client.Apps.FromName(ctx, p.appName, &modal.AppFromNameParams{CreateIfMissing: true})
	if err != nil {
		return nil, fmt.Errorf("modal: get app: %w", err)
	}
	img := p.client.Images.FromRegistry(p.image, nil)
	sb, err := p.client.Sandboxes.Create(ctx, app, img, &modal.SandboxCreateParams{
		Env: map[string]string{"TEMPORAL_TASK_QUEUE": taskQueueName},
	})
	if err != nil {
		return nil, fmt.Errorf("modal: create sandbox: %w", err)
	}
	return &compute.ProviderStatus{InstanceID: sb.SandboxID}, nil
}

func (p *modalProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	sb, err := p.client.Sandboxes.FromID(ctx, status.InstanceID)
	if err != nil {
		var notFound modal.NotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("modal: get sandbox: %w", err)
	}
	if _, err := sb.Terminate(ctx, nil); err != nil {
		var notFound modal.NotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("modal: terminate sandbox: %w", err)
	}
	return nil
}

func (p *modalProvider) Suspend(_ context.Context, _ *compute.ProviderStatus) error {
	return fmt.Errorf("modal: Suspend: %w", errors.ErrUnsupported)
}

func (p *modalProvider) Resume(_ context.Context, _ *compute.ProviderStatus) error {
	return fmt.Errorf("modal: Resume: %w", errors.ErrUnsupported)
}

func (p *modalProvider) Snapshot(ctx context.Context, status *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	sb, err := p.client.Sandboxes.FromID(ctx, status.InstanceID)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("modal: get sandbox: %w", err)
	}
	img, err := sb.SnapshotFilesystem(ctx, 5*time.Minute)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("modal: snapshot filesystem: %w", err)
	}
	return compute.SandboxPostSnapshotRunning, &compute.ProviderSnapshot{SnapshotID: img.ImageID}, nil
}

func (p *modalProvider) StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	app, err := p.client.Apps.FromName(ctx, p.appName, &modal.AppFromNameParams{CreateIfMissing: true})
	if err != nil {
		return nil, fmt.Errorf("modal: get app: %w", err)
	}
	img, err := p.client.Images.FromID(ctx, snapshot.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("modal: get snapshot image: %w", err)
	}
	sb, err := p.client.Sandboxes.Create(ctx, app, img, &modal.SandboxCreateParams{
		Env: map[string]string{"TEMPORAL_TASK_QUEUE": taskQueueName},
	})
	if err != nil {
		return nil, fmt.Errorf("modal: create sandbox from snapshot: %w", err)
	}
	return &compute.ProviderStatus{InstanceID: sb.SandboxID}, nil
}

func (p *modalProvider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) error {
	if err := p.client.Images.Delete(ctx, snapshot.SnapshotID, nil); err != nil {
		var notFound modal.NotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("modal: delete snapshot image: %w", err)
	}
	return nil
}

func (p *modalProvider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	sb, err := p.client.Sandboxes.FromID(ctx, status.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("modal: get sandbox: %w", err)
	}
	proc, err := sb.Exec(ctx, []string{"sh", "-c", cmd}, nil)
	if err != nil {
		return nil, fmt.Errorf("modal: exec command: %w", err)
	}
	stdout, err := io.ReadAll(proc.Stdout)
	if err != nil {
		return nil, fmt.Errorf("modal: read stdout: %w", err)
	}
	stderr, err := io.ReadAll(proc.Stderr)
	if err != nil {
		return nil, fmt.Errorf("modal: read stderr: %w", err)
	}
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("modal: wait for command: %w", err)
	}
	return &compute.CommandResult{
		Stdout:   string(stdout),
		Stderr:   string(stderr),
		ExitCode: int32(exitCode),
	}, nil
}
