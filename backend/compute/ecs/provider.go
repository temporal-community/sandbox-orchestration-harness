package ecs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	backendcompute "github.com/temporalio/sandbox-backend/compute"
	sdkcompute "github.com/temporalio/sandbox-sdk-go/compute"
)

func init() {
	backendcompute.Register(sdkcompute.ComputeProviderTypeECS, newECSProvider)
}

func newECSProvider(config map[string]string) (backendcompute.ComputeProvider, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("ecs: load aws config: %w", err)
	}
	assignPublicIp := types.AssignPublicIpDisabled
	if config["assign-public-ip"] == "true" {
		assignPublicIp = types.AssignPublicIpEnabled
	}
	return &ecsProvider{
		cluster:        config["cluster"],
		taskDefinition: config["task-definition"],
		subnets:        splitCSV(config["subnet-ids"]),
		securityGroups: splitCSV(config["security-group-ids"]),
		assignPublicIp: assignPublicIp,
		client:         ecs.NewFromConfig(cfg),
	}, nil
}

type ecsProvider struct {
	cluster        string
	taskDefinition string
	subnets        []string
	securityGroups []string
	assignPublicIp types.AssignPublicIp
	client         *ecs.Client
}

func (p *ecsProvider) Start(ctx context.Context, taskQueueName string) (*backendcompute.ComputeProviderStatus, error) {
	out, err := p.client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:              aws.String(p.cluster),
		TaskDefinition:       aws.String(p.taskDefinition),
		LaunchType:           types.LaunchTypeFargate,
		EnableExecuteCommand: true,
		NetworkConfiguration: &types.NetworkConfiguration{
			AwsvpcConfiguration: &types.AwsVpcConfiguration{
				Subnets:        p.subnets,
				SecurityGroups: p.securityGroups,
				AssignPublicIp: p.assignPublicIp,
			},
		},
		Overrides: &types.TaskOverride{
			ContainerOverrides: []types.ContainerOverride{
				{
					Name: aws.String("main"),
					Environment: []types.KeyValuePair{
						{Name: aws.String("TQ_NAME"), Value: aws.String(taskQueueName)},
					},
				},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("run ecs task: %w", err)
	}
	if len(out.Tasks) == 0 {
		return nil, fmt.Errorf("run ecs task: no tasks returned")
	}

	taskARN := aws.ToString(out.Tasks[0].TaskArn)
	waiter := ecs.NewTasksRunningWaiter(p.client)
	if err := waiter.Wait(ctx, &ecs.DescribeTasksInput{
		Cluster: aws.String(p.cluster),
		Tasks:   []string{taskARN},
	}, 5*time.Minute); err != nil {
		return nil, fmt.Errorf("wait for ecs task running: %w", err)
	}

	return &backendcompute.ComputeProviderStatus{InstanceID: taskARN}, nil
}

func (p *ecsProvider) Stop(ctx context.Context, status *backendcompute.ComputeProviderStatus) error {
	if _, err := p.client.StopTask(ctx, &ecs.StopTaskInput{
		Cluster: aws.String(p.cluster),
		Task:    aws.String(status.InstanceID),
	}); err != nil {
		return fmt.Errorf("stop ecs task: %w", err)
	}

	waiter := ecs.NewTasksStoppedWaiter(p.client)
	if err := waiter.Wait(ctx, &ecs.DescribeTasksInput{
		Cluster: aws.String(p.cluster),
		Tasks:   []string{status.InstanceID},
	}, 5*time.Minute); err != nil {
		return fmt.Errorf("wait for ecs task stopped: %w", err)
	}
	return nil
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return parts
}
