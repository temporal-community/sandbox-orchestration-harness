package compute

import "context"

type (
	ComputeProviderStatus struct {
		InstanceID string `json:"instance_id"`
	}
	ComputeProvider interface {
		Start(ctx context.Context, taskQueueName string) (*ComputeProviderStatus, error)
		Stop(ctx context.Context, status *ComputeProviderStatus) error
	}
)
