package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

func TestProviderErrorClassification(t *testing.T) {
	unrelated := fmt.Errorf("outer: %w", errors.New("transient provider failure"))
	for _, test := range []struct {
		name string
		err  error
		typ  string
	}{
		{name: "nil"},
		{name: "unrelated", err: unrelated},
		{name: "unsupported", err: fmt.Errorf("outer: %w", errors.ErrUnsupported), typ: errUnsupportedType},
		{name: "binding", err: fmt.Errorf("outer: %w", compute.ErrInvalidResourceBinding), typ: "ProviderConfigError"},
		{name: "unsupported-priority", err: fmt.Errorf("%w: %w", compute.ErrInvalidResourceBinding, errors.ErrUnsupported), typ: errUnsupportedType},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := wrapProviderError(test.err)
			if test.typ == "" {
				if got != test.err {
					t.Fatalf("pass-through changed error: %v", got)
				}
				return
			}
			var app *temporal.ApplicationError
			if !errors.As(got, &app) || app.Type() != test.typ || !app.NonRetryable() || app.Unwrap() != test.err {
				t.Fatalf("classification lost type, retry flag or cause: %v", got)
			}
		})
	}
}

type retryClassificationProvider struct {
	compute.Provider
	attempts  *atomic.Int32
	permanent bool
}

func (p *retryClassificationProvider) Stop(context.Context, *compute.ProviderStatus) error {
	attempt := p.attempts.Add(1)
	if p.permanent {
		return fmt.Errorf("local resource refusal: %w", compute.ErrInvalidResourceBinding)
	}
	if attempt == 1 {
		return errors.New("transient provider failure")
	}
	return nil
}

func providerRetryWorkflow(ctx temporalworkflow.Context, input StopSandboxInput) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout:    5 * time.Second,
		ScheduleToCloseTimeout: 15 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Millisecond,
			MaximumAttempts: 3,
		},
	})
	return temporalworkflow.ExecuteActivity(ctx, StopSandbox, input).Get(ctx, nil)
}

// TestLocalProviderErrorRetries checks serialized Activity failures on a local server.
func TestLocalProviderErrorRetries(t *testing.T) {
	if os.Getenv("HARNESS_LOCAL_TEST") != "1" {
		t.Skip("set HARNESS_LOCAL_TEST=1 for local server conformance")
	}
	for _, permanent := range []bool{false, true} {
		t.Run(fmt.Sprintf("permanent=%t", permanent), func(t *testing.T) {
			attempts := new(atomic.Int32)
			typ := compute.ProviderType(fmt.Sprintf("retry-classification-%d", workflowPolicySequence.Add(1)))
			compute.Register(typ, func(map[string]string) (compute.Provider, error) {
				return &retryClassificationProvider{attempts: attempts, permanent: permanent}, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := client.Dial(client.Options{HostPort: "127.0.0.1:17293"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			queue := fmt.Sprintf("retry-classification-%d", time.Now().UnixNano())
			w := worker.New(c, queue, worker.Options{})
			w.RegisterWorkflow(providerRetryWorkflow)
			w.RegisterActivity(StopSandbox)
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			defer w.Stop()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, providerRetryWorkflow, StopSandboxInput{
				Provider: compute.ProviderDetails{Type: typ},
				Status:   &compute.ProviderStatus{InstanceID: "resource"},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer c.CancelWorkflow(context.Background(), run.GetID(), run.GetRunID())
			err = run.Get(ctx, nil)
			if permanent {
				var app *temporal.ApplicationError
				if attempts.Load() != 1 || !errors.As(err, &app) || app.Type() != "ProviderConfigError" || !app.NonRetryable() || app.Unwrap() == nil || !strings.Contains(app.Unwrap().Error(), "local resource refusal") {
					t.Fatalf("permanent wire failure: attempts=%d err=%v", attempts.Load(), err)
				}
			} else if err != nil || attempts.Load() != 2 {
				t.Fatalf("transient retry: attempts=%d err=%v", attempts.Load(), err)
			}
		})
	}
}
