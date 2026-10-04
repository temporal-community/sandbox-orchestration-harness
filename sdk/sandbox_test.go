// ABOUTME: Tests for sandbox task-queue routing and the opaque sandbox reference format.
// ABOUTME: Uses a fake SandboxWorkflow and mocked SendSandbox* activities in the Temporal test env.
package sandbox

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	wfIface "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
)

const testParentTaskQueue = "parent-queue"

var testProvider = Provider{Type: compute.ProviderTypeModal, Config: map[string]string{"image": "ubuntu"}}

// fakeSandboxWorkflow stands in for SandboxWorkflow: it runs until it receives the stop signal.
func fakeSandboxWorkflow(ctx workflow.Context, _ wfIface.SandboxLocalState) error {
	workflow.GetSignalChannel(ctx, wfIface.SandboxStopSignal).Receive(ctx, nil)
	return nil
}

// routingRecorder captures the task queue each child workflow and activity was scheduled on.
type routingRecorder struct {
	childQueue     string
	activityQueues map[string]string // activity type → task queue
}

func newTestEnv() (*testsuite.TestWorkflowEnvironment, *routingRecorder) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: testParentTaskQueue})
	env.RegisterWorkflowWithOptions(fakeSandboxWorkflow, workflow.RegisterOptions{Name: wfIface.SandboxWorkflowType})
	env.RegisterActivity(&sandboxActivities{})
	env.OnActivity("SendSandboxInit", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SendSandboxExecuteCommand", mock.Anything, mock.Anything).Return(compute.CommandResult{Stdout: "ok"}, nil)

	rec := &routingRecorder{activityQueues: map[string]string{}}
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, _ converter.EncodedValues) {
		rec.childQueue = info.TaskQueueName
	})
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		rec.activityQueues[info.ActivityType.Name] = info.TaskQueue
	})
	return env, rec
}

// newRunStopWorkflow returns a parent workflow that creates a sandbox with opts,
// runs one command, then stops the sandbox.
func newRunStopWorkflow(opts ...SandboxOption) func(ctx workflow.Context) error {
	return func(ctx workflow.Context) error {
		sbx, err := NewSandbox(ctx, testProvider, opts...)
		if err != nil {
			return err
		}
		if _, err := sbx.ExecuteCommand(ctx, "echo hi"); err != nil {
			return err
		}
		return sbx.Stop(ctx)
	}
}

func TestNewSandbox_DefaultsToParentTaskQueue(t *testing.T) {
	env, rec := newTestEnv()

	env.ExecuteWorkflow(newRunStopWorkflow())

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, testParentTaskQueue, rec.childQueue)
	require.Equal(t, testParentTaskQueue, rec.activityQueues["SendSandboxInit"])
	require.Equal(t, testParentTaskQueue, rec.activityQueues["SendSandboxExecuteCommand"])
}

func TestNewSandbox_WithTaskQueueRoutesChildAndActivities(t *testing.T) {
	env, rec := newTestEnv()

	env.ExecuteWorkflow(newRunStopWorkflow(WithTaskQueue("sandbox-queue")))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "sandbox-queue", rec.childQueue)
	require.Equal(t, "sandbox-queue", rec.activityQueues["SendSandboxInit"])
	require.Equal(t, "sandbox-queue", rec.activityQueues["SendSandboxExecuteCommand"])
}

func TestAttachToSandbox_RoutesToTaskQueueFromRef(t *testing.T) {
	env, rec := newTestEnv()
	ref, err := encodeRef("sandbox-id", "sandbox-queue")
	require.NoError(t, err)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		sbx, err := AttachToSandbox(ref)
		if err != nil {
			return err
		}
		_, err = sbx.ExecuteCommand(ctx, "echo hi")
		return err
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "sandbox-queue", rec.activityQueues["SendSandboxExecuteCommand"])
}

func TestRef_RoundTripsSandboxIDAndTaskQueue(t *testing.T) {
	ref, err := encodeRef("sandbox-id", "sandbox-queue")
	require.NoError(t, err)

	data, err := decodeRef(ref)

	require.NoError(t, err)
	require.Equal(t, "sandbox-id", data.SandboxID)
	require.Equal(t, "sandbox-queue", data.TaskQueue)
}

func TestRef_DecodesVersion1AsParentTaskQueue(t *testing.T) {
	v1 := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"sandbox_id":"sandbox-id"}`))

	data, err := decodeRef(v1)

	require.NoError(t, err)
	require.Equal(t, "sandbox-id", data.SandboxID)
	require.Empty(t, data.TaskQueue)
}

func TestRef_RejectsUnknownVersion(t *testing.T) {
	v3 := base64.RawURLEncoding.EncodeToString([]byte(`{"v":3,"sandbox_id":"sandbox-id"}`))

	_, err := decodeRef(v3)

	require.ErrorContains(t, err, "unsupported ref version 3")
}
