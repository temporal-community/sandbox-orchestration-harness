package compute

type (
	ComputeProviderType string

	ComputeProvider struct {
		Type   ComputeProviderType
		Config map[string]string
	}

	ComputeProviderInvocationPayload struct {
		TaskQueue string `json:"taskQueue"`
	}
)

const (
	ComputeProviderTypeLambda    ComputeProviderType = "aws-lambda"
	ComputeProviderTypeECS       ComputeProviderType = "aws-ecs"
	ComputeProviderTypeAgentCore ComputeProviderType = "aws-agentcore"
)
