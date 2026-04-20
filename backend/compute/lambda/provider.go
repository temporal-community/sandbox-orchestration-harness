package lambda

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	backendcompute "github.com/temporalio/sandbox-backend/compute"
	sdkcompute "github.com/temporalio/sandbox-sdk-go/compute"
)

func init() {
	backendcompute.Register(sdkcompute.ComputeProviderTypeLambda, newLambdaProvider)
}

func newLambdaProvider(config map[string]string) (backendcompute.ComputeProvider, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("lambda: load aws config: %w", err)
	}
	return &lambdaProvider{
		functionARN: config["function-arn"],
		client:      lambda.NewFromConfig(cfg),
	}, nil
}

type lambdaProvider struct {
	functionARN string
	client      *lambda.Client
}

func (p *lambdaProvider) Start(ctx context.Context, taskQueueName string) (*backendcompute.ComputeProviderStatus, error) {
	payload, err := json.Marshal(sdkcompute.ComputeProviderInvocationPayload{TaskQueue: taskQueueName})
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	if _, err := p.client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   aws.String(p.functionARN),
		InvocationType: types.InvocationTypeEvent, // async
		Payload:        payload,
	}); err != nil {
		return nil, fmt.Errorf("invoke lambda: %w", err)
	}

	return &backendcompute.ComputeProviderStatus{InstanceID: p.functionARN}, nil
}

func (p *lambdaProvider) Stop(_ context.Context, _ *backendcompute.ComputeProviderStatus) error {
	// The Lambda worker exits naturally when the sandbox workflow completes
	// and there is no more work on its task queue.
	return nil
}
