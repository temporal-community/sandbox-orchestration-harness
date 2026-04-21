package agentcore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	backendcompute "github.com/temporalio/ephemeral-workers-poc/backend/compute"
	sdkcompute "github.com/temporalio/ephemeral-workers-poc/sdk/compute"
)

func init() {
	backendcompute.Register(sdkcompute.ComputeProviderTypeAgentCore, newAgentCoreProvider)
}

func newAgentCoreProvider(config map[string]string) (backendcompute.ComputeProvider, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("agentcore: load aws config: %w", err)
	}
	return &agentCoreProvider{
		agentRuntimeARN: config["agent-runtime-arn"],
		client:          bedrockagentcore.NewFromConfig(cfg),
	}, nil
}

type agentCoreProvider struct {
	agentRuntimeARN string
	client          *bedrockagentcore.Client
}

func (p *agentCoreProvider) Start(ctx context.Context, taskQueueName string) (*backendcompute.ComputeProviderStatus, error) {
	payload, err := json.Marshal(sdkcompute.ComputeProviderInvocationPayload{TaskQueue: taskQueueName})
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	out, err := p.client.InvokeAgentRuntime(ctx, &bedrockagentcore.InvokeAgentRuntimeInput{
		AgentRuntimeArn:  aws.String(p.agentRuntimeARN),
		Payload:          payload,
		ContentType:      aws.String("application/json"),
		RuntimeSessionId: aws.String(taskQueueName),
	})
	if err != nil {
		return nil, fmt.Errorf("invoke agent runtime: %w", err)
	}
	if out.Response != nil {
		out.Response.Close()
	}

	return &backendcompute.ComputeProviderStatus{InstanceID: taskQueueName}, nil
}

func (p *agentCoreProvider) Stop(ctx context.Context, status *backendcompute.ComputeProviderStatus) error {
	if _, err := p.client.StopRuntimeSession(ctx, &bedrockagentcore.StopRuntimeSessionInput{
		AgentRuntimeArn:  aws.String(p.agentRuntimeARN),
		RuntimeSessionId: aws.String(status.InstanceID),
	}); err != nil {
		return fmt.Errorf("stop runtime session: %w", err)
	}
	return nil
}
