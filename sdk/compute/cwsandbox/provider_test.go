package cwsandbox

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"buf.build/gen/go/coreweave/sandbox/connectrpc/go/coreweave/sandbox/v1/sandboxv1connect"
	v1 "buf.build/gen/go/coreweave/sandbox/protocolbuffers/go/coreweave/sandbox/v1"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

type testService struct {
	sandboxv1connect.UnimplementedSandboxServiceHandler
	create         func(*v1.CreateSandboxRequest) (*v1.Sandbox, error)
	get            func(*v1.GetSandboxRequest) (*v1.Sandbox, error)
	delete         func(*v1.DeleteSandboxRequest) error
	exec           func(*v1.ExecRequest) (*v1.ExecResponse, error)
	snapshot       func(*v1.CreateFileSystemSnapshotRequest) (*v1.FileSystemSnapshot, error)
	getSnapshot    func(*v1.GetFileSystemSnapshotRequest) (*v1.FileSystemSnapshot, error)
	deleteSnapshot func(*v1.DeleteFileSystemSnapshotRequest) error
}

func (s *testService) CreateSandbox(_ context.Context, r *connect.Request[v1.CreateSandboxRequest]) (*connect.Response[v1.Sandbox], error) {
	m, err := s.create(r.Msg)
	return connect.NewResponse(m), err
}
func (s *testService) GetSandbox(_ context.Context, r *connect.Request[v1.GetSandboxRequest]) (*connect.Response[v1.Sandbox], error) {
	m, err := s.get(r.Msg)
	return connect.NewResponse(m), err
}
func (s *testService) DeleteSandbox(_ context.Context, r *connect.Request[v1.DeleteSandboxRequest]) (*connect.Response[v1.DeleteSandboxResponse], error) {
	return connect.NewResponse(&v1.DeleteSandboxResponse{}), s.delete(r.Msg)
}
func (s *testService) Exec(_ context.Context, r *connect.Request[v1.ExecRequest]) (*connect.Response[v1.ExecResponse], error) {
	m, err := s.exec(r.Msg)
	return connect.NewResponse(m), err
}
func (s *testService) CreateFileSystemSnapshot(_ context.Context, r *connect.Request[v1.CreateFileSystemSnapshotRequest]) (*connect.Response[v1.FileSystemSnapshot], error) {
	m, err := s.snapshot(r.Msg)
	return connect.NewResponse(m), err
}
func (s *testService) GetFileSystemSnapshot(_ context.Context, r *connect.Request[v1.GetFileSystemSnapshotRequest]) (*connect.Response[v1.FileSystemSnapshot], error) {
	m, err := s.getSnapshot(r.Msg)
	return connect.NewResponse(m), err
}
func (s *testService) DeleteFileSystemSnapshot(_ context.Context, r *connect.Request[v1.DeleteFileSystemSnapshotRequest]) (*connect.Response[v1.FileSystemSnapshot], error) {
	return connect.NewResponse(&v1.FileSystemSnapshot{}), s.deleteSnapshot(r.Msg)
}

func testConfig(t *testing.T) map[string]string {
	t.Helper()
	t.Setenv("CWSANDBOX_API_KEY", "test-key")
	t.Setenv("CWSANDBOX_API_TOKEN", "")
	return map[string]string{"container-image": "ubuntu:24.04", "command": `["sleep", "infinity"]`}
}

func testProvider(t *testing.T, service *testService, config map[string]string) *cwSandboxProvider {
	t.Helper()
	route, handler := sandboxv1connect.NewSandboxServiceHandler(service)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" || !strings.HasPrefix(r.URL.Path, route) {
			t.Errorf("unexpected authentication or v1 route: %s", r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	config["base-url"] = server.URL
	provider, err := compute.Lookup(compute.ProviderTypeCoreWeaveSandbox, config)
	require.NoError(t, err)
	p := provider.(*cwSandboxProvider)
	p.pollInterval = time.Millisecond
	return p
}

func sandboxState(state v1.State) *v1.Sandbox {
	return &v1.Sandbox{SandboxId: "sb-test", Status: &v1.SandboxStatus{State: state, StateReason: "test reason"}}
}

func TestConfig(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"missing image":          {"container-image": ""},
		"missing command":        {"command": ""},
		"whitespace command":     {"command": "   "},
		"empty command":          {"command": "[]"},
		"blank executable":       {"command": `[""]`},
		"malformed command":      {"command": `["unterminated`},
		"malformed args":         {"args": "[1]"},
		"negative ttl":           {"max-lifetime-seconds": "-1"},
		"overflow ttl":           {"max-lifetime-seconds": "2147483648"},
		"invalid ttl":            {"max-lifetime-seconds": "later"},
		"profiles":               {"profile-names": "default"},
		"invalid mode":           {"mode": "other"},
		"serverless runners":     {"runner-ids": "runner-1"},
		"duplicate runners":      {"mode": "cks", "runner-ids": "a,a"},
		"blank runners":          {"mode": "cks", "runner-ids": "a,"},
		"relative workspace":     {"workspace-path": "data"},
		"unclean workspace":      {"workspace-path": "/a/../b"},
		"root workspace":         {"workspace-path": "/"},
		"size without workspace": {"workspace-size": "10Gi"},
		"bad URL":                {"base-url": "://invalid"},
		"URL credentials":        {"base-url": "https://user:password@example.com"},
		"URL query":              {"base-url": "https://example.com?foo=bar"},
	} {
		t.Run(name, func(t *testing.T) {
			config := testConfig(t)
			for key, value := range values {
				config[key] = value
			}
			_, err := newCoreWeaveSandboxProvider(config)
			require.Error(t, err)
		})
	}
	t.Run("credentials", func(t *testing.T) {
		config := testConfig(t)
		t.Setenv("CWSANDBOX_API_KEY", "")
		_, err := newCoreWeaveSandboxProvider(config)
		require.Error(t, err)
		t.Setenv("CWSANDBOX_API_TOKEN", "legacy-key")
		spec, token, endpoint, err := parseConfig(config)
		require.NoError(t, err)
		require.Equal(t, "legacy-key", token)
		require.Equal(t, defaultBaseURL, endpoint)
		require.Equal(t, v1.SandboxMode_SANDBOX_MODE_SERVERLESS, spec.Mode)
		t.Setenv("CWSANDBOX_API_KEY", "current-key")
		_, token, _, err = parseConfig(config)
		require.NoError(t, err)
		require.Equal(t, "current-key", token)
	})
}

func TestSplitArgs(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want []string
	}{
		{"", nil}, {"  \t", nil}, {"python worker.py", []string{"python", "worker.py"}},
		{`["sh", "-c", "echo hello world"]`, []string{"sh", "-c", "echo hello world"}},
	} {
		got, err := splitArgs(tc.raw)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual(tc.want, got), "%q: %v", tc.raw, got)
	}
}

func TestStartAndRestore(t *testing.T) {
	config := testConfig(t)
	config["mode"] = "cks"
	config["runner-ids"] = "runner-a, runner-b"
	config["cpu"], config["memory"] = "2", "4Gi"
	config["workspace-path"], config["workspace-size"] = "/workspace", "10Gi"
	config["max-lifetime-seconds"] = "600"
	config["args"] = `["extra argument"]`
	var creates []*v1.CreateSandboxRequest
	gets := 0
	service := &testService{
		create: func(r *v1.CreateSandboxRequest) (*v1.Sandbox, error) {
			creates = append(creates, r)
			return sandboxState(v1.State_STATE_CREATING), nil
		},
		get: func(r *v1.GetSandboxRequest) (*v1.Sandbox, error) {
			gets++
			require.Equal(t, "sb-test", r.SandboxId)
			return sandboxState(v1.State_STATE_RUNNING), nil
		},
	}
	p := testProvider(t, service, config)
	status, err := p.Start(context.Background(), "original-queue")
	require.NoError(t, err)
	require.Equal(t, "sb-test", status.InstanceID)
	_, err = p.StartFromSnapshot(context.Background(), "fork-queue", &compute.ProviderSnapshot{SnapshotID: "fss-test"})
	require.NoError(t, err)
	require.Equal(t, 2, gets)
	first, restored := creates[0], creates[1]
	require.NotEmpty(t, first.RequestId)
	require.NotEqual(t, first.RequestId, restored.RequestId)
	spec := first.Sandbox.Spec
	require.Equal(t, v1.SandboxMode_SANDBOX_MODE_CKS, spec.Mode)
	require.Equal(t, []string{"runner-a", "runner-b"}, spec.RunnerIds)
	require.EqualValues(t, 600, spec.MaxLifetimeSeconds)
	c := spec.Containers[0]
	require.Equal(t, "ubuntu:24.04", c.Image)
	require.Equal(t, "sleep", c.Command)
	require.Equal(t, []string{"infinity", "extra argument"}, c.Args)
	require.Equal(t, "original-queue", c.EnvironmentVariables[taskQueueEnvKey])
	require.Equal(t, "2", c.ResourceRequirements.Requests.Cpu)
	require.Equal(t, "4Gi", c.ResourceRequirements.Requests.Memory)
	require.Equal(t, "/workspace", c.WorkingDir)
	require.Equal(t, workspaceVolume, c.VolumeMounts[0].Volume)
	require.Equal(t, "/workspace", c.VolumeMounts[0].MountPath)
	require.Equal(t, "10Gi", spec.Volumes[0].GetScratch().Size)
	require.Empty(t, spec.Volumes[0].GetScratch().RestoreFromSnapshotId)
	require.Equal(t, "fork-queue", restored.Sandbox.Spec.Containers[0].EnvironmentVariables[taskQueueEnvKey])
	require.Equal(t, "fss-test", restored.Sandbox.Spec.Volumes[0].GetScratch().RestoreFromSnapshotId)
	require.Empty(t, p.spec.Volumes[0].GetScratch().RestoreFromSnapshotId)
	require.Empty(t, p.spec.Containers[0].EnvironmentVariables)
}

func TestStartFailures(t *testing.T) {
	for _, state := range []v1.State{v1.State_STATE_FAILED, v1.State_STATE_COMPLETED, v1.State_STATE_TERMINATING, v1.State_STATE_TERMINATED, v1.State_STATE_UNSPECIFIED} {
		t.Run(state.String(), func(t *testing.T) {
			p := testProvider(t, &testService{create: func(*v1.CreateSandboxRequest) (*v1.Sandbox, error) { return sandboxState(state), nil }}, testConfig(t))
			_, err := p.Start(context.Background(), "queue")
			require.ErrorContains(t, err, state.String())
			require.ErrorContains(t, err, "test reason")
		})
	}
	t.Run("missing ID", func(t *testing.T) {
		p := testProvider(t, &testService{create: func(*v1.CreateSandboxRequest) (*v1.Sandbox, error) { return &v1.Sandbox{}, nil }}, testConfig(t))
		_, err := p.Start(context.Background(), "queue")
		require.ErrorContains(t, err, "no sandbox ID")
	})
	t.Run("RPC error", func(t *testing.T) {
		p := testProvider(t, &testService{create: func(*v1.CreateSandboxRequest) (*v1.Sandbox, error) {
			return nil, connect.NewError(connect.CodePermissionDenied, errors.New("denied"))
		}}, testConfig(t))
		_, err := p.Start(context.Background(), "queue")
		require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})
	t.Run("cancellation while polling", func(t *testing.T) {
		p := testProvider(t, &testService{create: func(*v1.CreateSandboxRequest) (*v1.Sandbox, error) { return sandboxState(v1.State_STATE_CREATING), nil }}, testConfig(t))
		p.pollInterval = time.Hour
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := p.Start(ctx, "queue")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestSnapshot(t *testing.T) {
	for _, state := range []v1.SnapshotState{v1.SnapshotState_SNAPSHOT_STATE_READY, v1.SnapshotState_SNAPSHOT_STATE_FAILED, v1.SnapshotState_SNAPSHOT_STATE_DELETING} {
		t.Run(state.String(), func(t *testing.T) {
			config := testConfig(t)
			config["workspace-path"] = "/workspace"
			p := testProvider(t, &testService{
				snapshot: func(r *v1.CreateFileSystemSnapshotRequest) (*v1.FileSystemSnapshot, error) {
					require.Equal(t, "sb-test", r.SandboxId)
					require.Equal(t, workspaceVolume, r.ScratchVolumeName)
					require.NotEmpty(t, r.RequestId)
					return &v1.FileSystemSnapshot{FileSystemSnapshotId: "fss-test", State: v1.SnapshotState_SNAPSHOT_STATE_CREATING}, nil
				},
				getSnapshot: func(r *v1.GetFileSystemSnapshotRequest) (*v1.FileSystemSnapshot, error) {
					require.Equal(t, "fss-test", r.FileSystemSnapshotId)
					return &v1.FileSystemSnapshot{FileSystemSnapshotId: "fss-test", State: state, StateReason: "snapshot reason"}, nil
				},
			}, config)
			post, snapshot, err := p.Snapshot(context.Background(), &compute.ProviderStatus{InstanceID: "sb-test"})
			require.Equal(t, compute.SandboxPostSnapshotRunning, post)
			if state == v1.SnapshotState_SNAPSHOT_STATE_READY {
				require.NoError(t, err)
				require.Equal(t, "fss-test", snapshot.SnapshotID)
			} else {
				require.Nil(t, snapshot)
				require.ErrorContains(t, err, "snapshot reason")
			}
		})
	}
	t.Run("cancellation", func(t *testing.T) {
		config := testConfig(t)
		config["workspace-path"] = "/workspace"
		p := testProvider(t, &testService{snapshot: func(*v1.CreateFileSystemSnapshotRequest) (*v1.FileSystemSnapshot, error) {
			return &v1.FileSystemSnapshot{FileSystemSnapshotId: "fss-test", State: v1.SnapshotState_SNAPSHOT_STATE_CREATING}, nil
		}}, config)
		p.pollInterval = time.Hour
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _, err := p.Snapshot(ctx, &compute.ProviderStatus{InstanceID: "sb-test"})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestUnsupportedOperations(t *testing.T) {
	p := testProvider(t, &testService{}, testConfig(t))
	ctx := context.Background()
	require.ErrorIs(t, p.Suspend(ctx, nil), errors.ErrUnsupported)
	require.ErrorIs(t, p.Resume(ctx, nil), errors.ErrUnsupported)
	_, _, err := p.Snapshot(ctx, nil)
	require.ErrorIs(t, err, errors.ErrUnsupported)
	_, err = p.StartFromSnapshot(ctx, "queue", nil)
	require.ErrorIs(t, err, errors.ErrUnsupported)
}

func TestDelete(t *testing.T) {
	for _, code := range []connect.Code{0, connect.CodeNotFound, connect.CodePermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			var rpcErr error
			if code != 0 {
				rpcErr = connect.NewError(code, errors.New("test error"))
			}
			p := testProvider(t, &testService{
				delete: func(r *v1.DeleteSandboxRequest) error { require.Equal(t, "sb-test", r.SandboxId); return rpcErr },
				deleteSnapshot: func(r *v1.DeleteFileSystemSnapshotRequest) error {
					require.Equal(t, "fss-test", r.FileSystemSnapshotId)
					require.True(t, r.AllowMissing)
					return rpcErr
				},
			}, testConfig(t))
			for _, err := range []error{p.Stop(context.Background(), &compute.ProviderStatus{InstanceID: "sb-test"}), p.DeleteSnapshot(context.Background(), &compute.ProviderSnapshot{SnapshotID: "fss-test"})} {
				if code == 0 || code == connect.CodeNotFound {
					require.NoError(t, err)
				} else {
					require.Equal(t, code, connect.CodeOf(err))
				}
			}
		})
	}
}

func TestExec(t *testing.T) {
	for _, truncated := range []string{"", "stdout", "stderr"} {
		t.Run("truncated="+truncated, func(t *testing.T) {
			cmd := `printf '%s' 'hello world' | cat; exit 7`
			p := testProvider(t, &testService{exec: func(r *v1.ExecRequest) (*v1.ExecResponse, error) {
				require.Equal(t, "sb-test", r.SandboxId)
				require.Equal(t, []string{"sh", "-c", cmd}, r.Command)
				return &v1.ExecResponse{Stdout: []byte("hello world"), Stderr: []byte("error"), ExitCode: 7, StdoutTruncated: truncated == "stdout", StderrTruncated: truncated == "stderr"}, nil
			}}, testConfig(t))
			result, err := p.ExecuteCommand(context.Background(), &compute.ProviderStatus{InstanceID: "sb-test"}, cmd)
			if truncated != "" {
				require.ErrorContains(t, err, "truncated")
				return
			}
			require.NoError(t, err)
			require.Equal(t, &compute.CommandResult{Stdout: "hello world", Stderr: "error", ExitCode: 7}, result)
		})
	}
}

func TestRequestIDAcrossActivityAttempts(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var ids []string
	fn := func(ctx context.Context) error {
		ids = append(ids, requestID(ctx))
		if activity.GetInfo(ctx).Attempt == 1 {
			return errors.New("retry")
		}
		return nil
	}
	env.RegisterActivity(fn)
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		for _, id := range []string{"create", "restore"} {
			activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				ActivityID: id, StartToCloseTimeout: time.Minute,
				RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Millisecond, MaximumAttempts: 2},
			})
			if err := workflow.ExecuteActivity(activityCtx, fn).Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, ids, 4)
	require.Equal(t, ids[0], ids[1])
	require.Equal(t, ids[2], ids[3])
	require.NotEqual(t, ids[0], ids[2])
}
