package agentcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	agentcoretypes "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

type (
	ProviderInvocationPayload struct {
		TaskQueue string `json:"task_queue"`
	}
)

func init() {
	compute.Register(compute.ProviderTypeAgentCoreRuntime, newAgentCoreRuntimeProvider)
}

func newAgentCoreRuntimeProvider(config map[string]string) (compute.Provider, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("agentcore-runtime: load aws config: %w", err)
	}

	agentRuntimeARN := config["agent-runtime-arn"]
	if err := validateARN(agentRuntimeARN); err != nil {
		return nil, fmt.Errorf("agentcore-runtime: invalid agent-runtime-arn: %w", err)
	}

	return &agentCoreRuntimeProvider{
		agentRuntimeARN: agentRuntimeARN,
		client:          bedrockagentcore.NewFromConfig(cfg),
	}, nil
}

type agentCoreRuntimeProvider struct {
	agentRuntimeARN string
	client          *bedrockagentcore.Client
}

func (p *agentCoreRuntimeProvider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	payload, err := json.Marshal(ProviderInvocationPayload{TaskQueue: taskQueueName})
	if err != nil {
		return nil, fmt.Errorf("agentcore-runtime: marshal payload: %w", err)
	}

	out, err := p.client.InvokeAgentRuntime(ctx, &bedrockagentcore.InvokeAgentRuntimeInput{
		AgentRuntimeArn:  aws.String(p.agentRuntimeARN),
		Payload:          payload,
		ContentType:      aws.String("application/json"),
		RuntimeSessionId: aws.String(taskQueueName),
	})
	if err != nil {
		return nil, fmt.Errorf("agentcore-runtime: invoke agent runtime: %w", err)
	}
	if out.Response != nil {
		defer out.Response.Close()
		_, _ = io.Copy(io.Discard, out.Response)
	}

	return &compute.ProviderStatus{InstanceID: taskQueueName}, nil
}

func (p *agentCoreRuntimeProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	_, err := p.client.StopRuntimeSession(ctx, &bedrockagentcore.StopRuntimeSessionInput{
		AgentRuntimeArn:  aws.String(p.agentRuntimeARN),
		RuntimeSessionId: aws.String(status.InstanceID),
	})
	// if the sandbox already doesn't exist anymore, let's ignore the error
	var notFound *agentcoretypes.ResourceNotFoundException
	if err != nil && !errors.As(err, &notFound) {
		return fmt.Errorf("agentcore-runtime: stop runtime session: %w", err)
	}
	return nil
}

func (p *agentCoreRuntimeProvider) Suspend(ctx context.Context, status *compute.ProviderStatus) error {
	_, err := p.client.StopRuntimeSession(ctx, &bedrockagentcore.StopRuntimeSessionInput{
		AgentRuntimeArn:  aws.String(p.agentRuntimeARN),
		RuntimeSessionId: aws.String(status.InstanceID),
	})

	// if the sandbox already doesn't exist anymore, let's ignore the error
	var notFound *agentcoretypes.ResourceNotFoundException
	if err != nil && !errors.As(err, &notFound) {
		return fmt.Errorf("agentcore-runtime: suspend session: %w", err)
	}
	return nil
}

func (p *agentCoreRuntimeProvider) Resume(ctx context.Context, status *compute.ProviderStatus) error {
	payload, err := json.Marshal(ProviderInvocationPayload{TaskQueue: status.InstanceID})
	if err != nil {
		return fmt.Errorf("agentcore-runtime: marshal payload: %w", err)
	}
	out, err := p.client.InvokeAgentRuntime(ctx, &bedrockagentcore.InvokeAgentRuntimeInput{
		AgentRuntimeArn:  aws.String(p.agentRuntimeARN),
		Payload:          payload,
		ContentType:      aws.String("application/json"),
		RuntimeSessionId: aws.String(status.InstanceID),
	})
	if err != nil {
		return fmt.Errorf("agentcore-runtime: resume session: %w", err)
	}
	if out.Response != nil {
		defer out.Response.Close()
		_, _ = io.Copy(io.Discard, out.Response)
	}
	return nil
}

func (p *agentCoreRuntimeProvider) Snapshot(_ context.Context, _ *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("agentcore-runtime: Snapshot: %w", errors.ErrUnsupported)
}

func (p *agentCoreRuntimeProvider) StartFromSnapshot(_ context.Context, _ string, _ *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	return nil, fmt.Errorf("agentcore-runtime: StartFromSnapshot: %w", errors.ErrUnsupported)
}

func (p *agentCoreRuntimeProvider) DeleteSnapshot(_ context.Context, _ *compute.ProviderSnapshot) error {
	return fmt.Errorf("agentcore-runtime: DeleteSnapshot: %w", errors.ErrUnsupported)
}

func (p *agentCoreRuntimeProvider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	timeout := int32(300)
	out, err := p.client.InvokeAgentRuntimeCommand(ctx, &bedrockagentcore.InvokeAgentRuntimeCommandInput{
		AgentRuntimeArn:  aws.String(p.agentRuntimeARN),
		RuntimeSessionId: aws.String(status.InstanceID),
		Body: &agentcoretypes.InvokeAgentRuntimeCommandRequestBody{
			Command: aws.String(cmd),
			Timeout: &timeout,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("agentcore-runtime: invoke agent runtime command: %w", err)
	}
	defer out.GetStream().Close()

	var stdout, stderr strings.Builder
	var exitCode int32
	for event := range out.GetStream().Events() {
		chunk, ok := event.(*agentcoretypes.InvokeAgentRuntimeCommandStreamOutputMemberChunk)
		if !ok {
			continue
		}
		if d := chunk.Value.ContentDelta; d != nil {
			if d.Stdout != nil {
				stdout.WriteString(*d.Stdout)
			}
			if d.Stderr != nil {
				stderr.WriteString(*d.Stderr)
			}
		}
		if s := chunk.Value.ContentStop; s != nil {
			exitCode = aws.ToInt32(s.ExitCode)
		}
	}
	if err := out.GetStream().Err(); err != nil {
		return nil, fmt.Errorf("agentcore-runtime: command stream error: %w", err)
	}
	return &compute.CommandResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}, nil
}

func validateARN(s string) error {
	if s == "" {
		return errors.New("required")
	}
	if _, err := arn.Parse(s); err != nil {
		return err
	}
	return nil
}
