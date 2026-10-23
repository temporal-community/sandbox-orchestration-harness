// ABOUTME: Verifies the Go payload types match the shared golden fixtures in contract/fixtures.
// ABOUTME: Clients in other languages test against the same files, so the two cannot drift apart.
package sandbox

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	wfIface "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
)

const fixturesDir = "../contract/fixtures"

// contractTypes maps each fixture file to a constructor for the Go type it encodes.
var contractTypes = map[string]func() any{
	"sandbox_workflow_input.json":                func() any { return &wfIface.SandboxLocalState{} },
	"send_sandbox_init_input.json":               func() any { return &SendSandboxInitInput{} },
	"send_sandbox_init_input_from_snapshot.json": func() any { return &SendSandboxInitInput{} },
	"send_sandbox_execute_command_input.json":    func() any { return &SendSandboxExecuteCommandInput{} },
	"send_sandbox_suspend_input.json":            func() any { return &SendSandboxSuspendInput{} },
	"send_sandbox_resume_input.json":             func() any { return &SendSandboxResumeInput{} },
	"send_sandbox_snapshot_input.json":           func() any { return &SendSandboxSnapshotInput{} },
	"send_sandbox_delete_snapshot_input.json":    func() any { return &SendSandboxDeleteSnapshotInput{} },
	"command_result.json":                        func() any { return &compute.CommandResult{} },
	"provider_snapshot.json":                     func() any { return &compute.ProviderSnapshot{} },
	"sandbox_ref_payload.json":                   func() any { return &sandboxRefData{} },
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixturesDir, name))
	require.NoError(t, err)
	return raw
}

func TestContractFixtures_RoundTripThroughGoTypes(t *testing.T) {
	for name, newValue := range contractTypes {
		t.Run(name, func(t *testing.T) {
			raw := readFixture(t, name)
			value := newValue()

			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			require.NoError(t, decoder.Decode(value), "fixture has fields the Go type does not")

			encoded, err := json.Marshal(value)
			require.NoError(t, err)
			require.JSONEq(t, string(raw), string(encoded), "Go encoding differs from fixture")
		})
	}
}

func TestContractFixtures_EveryFixtureIsCovered(t *testing.T) {
	entries, err := os.ReadDir(fixturesDir)
	require.NoError(t, err)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			require.Contains(t, contractTypes, e.Name(), "fixture has no Go type mapping")
		}
	}
}

func TestContractFixtures_SandboxRefEncoding(t *testing.T) {
	var payload sandboxRefData
	require.NoError(t, json.Unmarshal(readFixture(t, "sandbox_ref_payload.json"), &payload))
	wantRef := strings.TrimSpace(string(readFixture(t, "sandbox_ref.txt")))

	ref, err := encodeRef(payload.SandboxID, payload.TaskQueue)

	require.NoError(t, err)
	require.Equal(t, wantRef, ref)
}
