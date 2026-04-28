package gkeagentsandbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	sandboxclient "sigs.k8s.io/agent-sandbox/clients/go/sandbox"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func init() {
	compute.Register(compute.ProviderTypeGKEAgentSandbox, newGKEAgentSandboxProvider)
}

var (
	sandboxGVR = schema.GroupVersionResource{
		Group:    "agents.x-k8s.io",
		Version:  "v1alpha1",
		Resource: "sandboxes",
	}
	podSnapshotGVR = schema.GroupVersionResource{
		Group:    "podsnapshot.gke.io",
		Version:  "v1alpha1",
		Resource: "podsnapshots",
	}
	podSnapshotTriggerGVR = schema.GroupVersionResource{
		Group:    "podsnapshot.gke.io",
		Version:  "v1alpha1",
		Resource: "podsnapshotmanualtriggers",
	}
)

func newGKEAgentSandboxProvider(config map[string]string) (compute.Provider, error) {
	template := config["template"]
	if template == "" {
		return nil, fmt.Errorf("gke-agent-sandbox: template required")
	}
	namespace := config["namespace"]
	if namespace == "" {
		namespace = "default"
	}
	client, err := sandboxclient.NewClient(context.Background(), sandboxclient.Options{
		TemplateName: template,
		Namespace:    namespace,
		Quiet:        false,
	})
	if err != nil {
		return nil, fmt.Errorf("gke-agent-sandbox: create client: %w", err)
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		restConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(),
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("gke-agent-sandbox: build k8s config: %w", err)
		}
	}
	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("gke-agent-sandbox: create dynamic client: %w", err)
	}

	return &gkeAgentSandboxProvider{
		client:        client,
		dynamicClient: dynClient,
		template:      template,
		namespace:     namespace,
	}, nil
}

type gkeAgentSandboxProvider struct {
	client        *sandboxclient.Client
	dynamicClient dynamic.Interface
	template      string
	namespace     string
}

func (p *gkeAgentSandboxProvider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	sb, err := p.client.CreateSandbox(ctx, p.template, p.namespace)
	if err != nil {
		return nil, fmt.Errorf("gke-agent-sandbox: create sandbox: %w", err)
	}
	// TODO: the GKE Agent Sandbox SDK does not support customizing environment variables (though the underlying project does seem to support it). So taking this work around for now.
	if _, err := sb.Run(ctx, fmt.Sprintf("echo TEMPORAL_TASK_QUEUE=%s >> /etc/environment", taskQueueName)); err != nil {
		return nil, fmt.Errorf("gke-agent-sandbox: set task queue env: %w", err)
	}
	return &compute.ProviderStatus{InstanceID: sb.ClaimName()}, nil
}

func (p *gkeAgentSandboxProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	if err := p.client.DeleteSandbox(ctx, status.InstanceID, p.namespace); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("gke-agent-sandbox: delete sandbox: %w", err)
	}
	return nil
}

func (p *gkeAgentSandboxProvider) Suspend(_ context.Context, _ *compute.ProviderStatus) error {
	return fmt.Errorf("gke-agent-sandbox: Suspend: %w", errors.ErrUnsupported)
}

func (p *gkeAgentSandboxProvider) Resume(_ context.Context, _ *compute.ProviderStatus) error {
	return fmt.Errorf("gke-agent-sandbox: Resume: %w", errors.ErrUnsupported)
}

// Snapshot creates a GKE PodSnapshot checkpoint of the running sandbox pod, then scales
// the Sandbox to 0 replicas (suspended without deleting the SandboxClaim). The returned
// SnapshotID encodes "claimName;sandboxName;snapshotName" for use by StartFromSnapshot.
func (p *gkeAgentSandboxProvider) Snapshot(ctx context.Context, status *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	sb, err := p.client.GetSandbox(ctx, status.InstanceID, p.namespace)
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("gke-agent-sandbox: get sandbox: %w", err)
	}
	podName := sb.PodName()
	sandboxName := sb.SandboxName()

	triggerName := "snap-trigger-" + status.InstanceID
	trigger := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "podsnapshot.gke.io/v1alpha1",
		"kind":       "PodSnapshotManualTrigger",
		"metadata":   map[string]interface{}{"name": triggerName, "namespace": p.namespace},
		"spec":       map[string]interface{}{"targetPod": podName},
	}}
	if _, err := p.dynamicClient.Resource(podSnapshotTriggerGVR).Namespace(p.namespace).Create(ctx, trigger, metav1.CreateOptions{}); err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("gke-agent-sandbox: create snapshot trigger: %w", err)
	}

	snapshotName, err := p.waitForSnapshotTrigger(ctx, triggerName)
	// Always clean up the one-shot trigger resource.
	_ = p.dynamicClient.Resource(podSnapshotTriggerGVR).Namespace(p.namespace).Delete(ctx, triggerName, metav1.DeleteOptions{})
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}

	// Suspend: scale to 0 without deleting the claim so StartFromSnapshot can resume it.
	patch := []byte(`{"spec":{"replicas":0}}`)
	if _, err := p.dynamicClient.Resource(sandboxGVR).Namespace(p.namespace).Patch(ctx, sandboxName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("gke-agent-sandbox: scale sandbox to 0: %w", err)
	}

	snapshotID := status.InstanceID + ";" + sandboxName + ";" + snapshotName
	return compute.SandboxPostSnapshotSuspended, &compute.ProviderSnapshot{SnapshotID: snapshotID}, nil
}

// waitForSnapshotTrigger polls the PodSnapshotManualTrigger status until the GKE controller
// marks the Triggered condition Complete (returning the PodSnapshot UID) or Failed.
func (p *gkeAgentSandboxProvider) waitForSnapshotTrigger(ctx context.Context, triggerName string) (string, error) {
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		obj, err := p.dynamicClient.Resource(podSnapshotTriggerGVR).Namespace(p.namespace).Get(ctx, triggerName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("gke-agent-sandbox: get snapshot trigger: %w", err)
		}

		conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
		for _, c := range conditions {
			cond, _ := c.(map[string]interface{})
			if cond["type"] != "Triggered" {
				continue
			}
			switch cond["reason"] {
			case "Complete":
				uid, _, _ := unstructured.NestedString(obj.Object, "status", "snapshotCreated", "name")
				if uid == "" {
					return "", fmt.Errorf("gke-agent-sandbox: trigger completed but snapshotCreated.name is empty")
				}
				return uid, nil
			case "Failed", "Error":
				msg, _ := cond["message"].(string)
				return "", fmt.Errorf("gke-agent-sandbox: snapshot trigger failed: %s", msg)
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// StartFromSnapshot scales the suspended Sandbox back to 1 replica. The GKE Pod Snapshot
// controller automatically restores the checkpoint into the restarted pod. The sandbox is
// identified by the sandboxName encoded in the SnapshotID; the returned InstanceID is the
// original claimName so subsequent operations (ExecuteCommand, Stop) continue to work.
//
// Note: Only one resume is possible per snapshot (GKE checkpoints are pod-specific, not
// template-level). True forking (multiple independent sandboxes from one snapshot) is
// not supported by the podsnapshot.gke.io CRD API.
func (p *gkeAgentSandboxProvider) StartFromSnapshot(ctx context.Context, _ string, snapshot *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	parts := strings.SplitN(snapshot.SnapshotID, ";", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("gke-agent-sandbox: malformed snapshot ID %q", snapshot.SnapshotID)
	}
	claimName, sandboxName := parts[0], parts[1]

	patch := []byte(`{"spec":{"replicas":1}}`)
	if _, err := p.dynamicClient.Resource(sandboxGVR).Namespace(p.namespace).Patch(ctx, sandboxName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return nil, fmt.Errorf("gke-agent-sandbox: scale sandbox to 1: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		obj, err := p.dynamicClient.Resource(sandboxGVR).Namespace(p.namespace).Get(ctx, sandboxName, metav1.GetOptions{})
		if err == nil {
			replicas, _, _ := unstructured.NestedInt64(obj.Object, "status", "replicas")
			if replicas > 0 {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}

	return &compute.ProviderStatus{InstanceID: claimName}, nil
}

// DeleteSnapshot deletes the GKE PodSnapshot resource identified by the snapshotName in
// the SnapshotID. The SandboxClaim and Sandbox are not touched here; Stop() handles those.
func (p *gkeAgentSandboxProvider) DeleteSnapshot(ctx context.Context, snapshot *compute.ProviderSnapshot) error {
	parts := strings.SplitN(snapshot.SnapshotID, ";", 3)
	snapshotName := parts[len(parts)-1]
	err := p.dynamicClient.Resource(podSnapshotGVR).Namespace(p.namespace).Delete(ctx, snapshotName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("gke-agent-sandbox: delete snapshot %s: %w", snapshotName, err)
	}
	return nil
}

func (p *gkeAgentSandboxProvider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	sb, err := p.client.GetSandbox(ctx, status.InstanceID, p.namespace)
	if err != nil {
		return nil, fmt.Errorf("gke-agent-sandbox: get sandbox: %w", err)
	}
	result, err := sb.Run(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("gke-agent-sandbox: run command: %w", err)
	}
	return &compute.CommandResult{
		Stdout:   result.Stdout,
		Stderr:   result.Stderr,
		ExitCode: int32(result.ExitCode),
	}, nil
}
