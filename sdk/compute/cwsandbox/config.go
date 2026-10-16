package cwsandbox

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	sandboxv1 "buf.build/gen/go/coreweave/sandbox/protocolbuffers/go/coreweave/sandbox/v1"
)

func parseConfig(config map[string]string) (*sandboxv1.SandboxSpec, string, string, error) {
	token := os.Getenv("CWSANDBOX_API_KEY")
	if token == "" {
		token = os.Getenv("CWSANDBOX_API_TOKEN")
	}
	if token == "" {
		return nil, "", "", fmt.Errorf("cwsandbox: CWSANDBOX_API_KEY env var required (CWSANDBOX_API_TOKEN also accepted)")
	}
	if config["profile-names"] != "" {
		return nil, "", "", fmt.Errorf("cwsandbox: profile-names is not supported by v1; use mode=cks and runner-ids for own-cluster placement")
	}
	if strings.TrimSpace(config["container-image"]) == "" {
		return nil, "", "", fmt.Errorf("cwsandbox: container-image required")
	}
	command, err := splitArgs(config["command"])
	if err != nil {
		return nil, "", "", fmt.Errorf("cwsandbox: invalid command: %w", err)
	}
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		return nil, "", "", fmt.Errorf("cwsandbox: command must contain a non-empty executable")
	}
	args, err := splitArgs(config["args"])
	if err != nil {
		return nil, "", "", fmt.Errorf("cwsandbox: invalid args: %w", err)
	}
	container := &sandboxv1.Container{
		Name: "worker", Image: config["container-image"], Command: command[0], Args: append(command[1:], args...),
		EnvironmentVariables: map[string]string{},
	}
	spec := &sandboxv1.SandboxSpec{Containers: []*sandboxv1.Container{container}, Mode: sandboxv1.SandboxMode_SANDBOX_MODE_SERVERLESS}
	if raw := config["max-lifetime-seconds"]; raw != "" {
		ttl, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || ttl < 0 {
			return nil, "", "", fmt.Errorf("cwsandbox: max-lifetime-seconds must be a non-negative int32")
		}
		spec.MaxLifetimeSeconds = int32(ttl)
	}
	switch config["mode"] {
	case "", "serverless":
	case "cks":
		spec.Mode = sandboxv1.SandboxMode_SANDBOX_MODE_CKS
	default:
		return nil, "", "", fmt.Errorf("cwsandbox: mode must be serverless or cks")
	}
	if raw := config["runner-ids"]; raw != "" {
		if spec.Mode != sandboxv1.SandboxMode_SANDBOX_MODE_CKS {
			return nil, "", "", fmt.Errorf("cwsandbox: runner-ids requires mode=cks")
		}
		seen := map[string]bool{}
		for _, id := range strings.Split(raw, ",") {
			id = strings.TrimSpace(id)
			if id == "" || seen[id] {
				return nil, "", "", fmt.Errorf("cwsandbox: runner-ids must be non-empty and unique")
			}
			seen[id] = true
			spec.RunnerIds = append(spec.RunnerIds, id)
		}
	}
	if config["cpu"] != "" || config["memory"] != "" {
		container.ResourceRequirements = &sandboxv1.ResourceRequirements{Requests: &sandboxv1.Resources{Cpu: config["cpu"], Memory: config["memory"]}}
	}
	if workspace := config["workspace-path"]; workspace != "" {
		if !path.IsAbs(workspace) || path.Clean(workspace) != workspace || workspace == "/" {
			return nil, "", "", fmt.Errorf("cwsandbox: workspace-path must be a clean absolute directory other than /")
		}
		spec.Volumes = []*sandboxv1.SandboxVolume{{Name: workspaceVolume, Source: &sandboxv1.SandboxVolume_Scratch{Scratch: &sandboxv1.ScratchVolume{Size: config["workspace-size"]}}}}
		container.VolumeMounts = []*sandboxv1.VolumeMount{{Volume: workspaceVolume, MountPath: workspace}}
		container.WorkingDir = workspace
	} else if config["workspace-size"] != "" {
		return nil, "", "", fmt.Errorf("cwsandbox: workspace-size requires workspace-path")
	}
	baseURL := strings.TrimRight(config["base-url"], "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, "", "", fmt.Errorf("cwsandbox: base-url must be an HTTP(S) endpoint without credentials, query, or fragment")
	}
	return spec, token, baseURL, nil
}

func splitArgs(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if strings.HasPrefix(raw, "[") {
		var parsed []string
		if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
			return nil, err
		}
		return parsed, nil
	}
	return strings.Fields(raw), nil
}
