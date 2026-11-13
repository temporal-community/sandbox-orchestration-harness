package smolcloud

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

const (
	// ConfigKeyBootstrap selects whether the guest receives Worker bootstrap: worker (default) or none.
	ConfigKeyBootstrap = "bootstrap"
	// ConfigKeyLifecycle selects the execution lifecycle policy: standard (default) or fresh.
	ConfigKeyLifecycle = "lifecycle"
	// ConfigKeyBranchable controls checkpoint support: true (default) or false. Fresh lifecycle requires false.
	ConfigKeyBranchable = "branchable"
)

type bootstrapMode string

const (
	bootstrapWorker bootstrapMode = "worker"
	bootstrapNone   bootstrapMode = "none"
)

// resolvedConfig holds the validated provider configuration.
type resolvedConfig struct {
	bootstrap bootstrapMode
	policy    compute.ExecutionPolicy
	// bindConfiguration is true when the configuration sets bootstrap, lifecycle or branchable.
	// Such machines carry a configuration label, and retries adopt only a single machine whose
	// label matches. Configurations without these settings keep the original request and adoption.
	bindConfiguration      bool
	image, network         string
	cidrs, hosts           []string
	cpus, memoryMB, diskGB int
}

// resolvePolicy reads only the lifecycle and branchable settings. It runs in Workflow code,
// so other configuration errors surface from the constructor inside the start Activity.
func resolvePolicy(config map[string]string) (compute.ExecutionPolicy, error) {
	var policy compute.ExecutionPolicy
	if raw, ok := config[ConfigKeyLifecycle]; ok {
		switch raw {
		case "standard":
			policy.Lifecycle = compute.LifecycleStandard
		case "fresh":
			policy.Lifecycle = compute.LifecycleFresh
		default:
			return policy, fmt.Errorf("smol-cloud: lifecycle must be standard or fresh")
		}
	}
	policy.NoBranching = policy.Lifecycle == compute.LifecycleFresh
	if raw, ok := config[ConfigKeyBranchable]; ok {
		if raw != "true" && raw != "false" {
			return policy, fmt.Errorf("smol-cloud: branchable must be true or false")
		}
		policy.NoBranching = raw == "false"
		if !policy.NoBranching && policy.Lifecycle == compute.LifecycleFresh {
			return policy, fmt.Errorf("smol-cloud: fresh lifecycle requires branchable=false")
		}
	}
	return policy, nil
}

// resolveConfig validates the complete configuration for the constructor. It reuses
// resolvePolicy so Workflow admission and the provider apply the same policy.
func resolveConfig(config map[string]string) (resolvedConfig, error) {
	c := resolvedConfig{bootstrap: bootstrapWorker, cpus: 1, memoryMB: 512}
	policy, err := resolvePolicy(config)
	if err != nil {
		return c, err
	}
	c.policy = policy
	for _, key := range []string{ConfigKeyBootstrap, ConfigKeyLifecycle, ConfigKeyBranchable} {
		if _, ok := config[key]; ok {
			c.bindConfiguration = true
		}
	}
	if raw, ok := config[ConfigKeyBootstrap]; ok {
		if raw != string(bootstrapWorker) && raw != string(bootstrapNone) {
			return c, fmt.Errorf("smol-cloud: bootstrap must be worker or none")
		}
		c.bootstrap = bootstrapMode(raw)
	}
	c.image = strings.TrimSpace(config["image"])
	if c.image == "" {
		return c, fmt.Errorf("smol-cloud: image is required")
	}
	c.network = config["network"]
	if c.network == "" {
		c.network = "blocked"
	}
	if c.network != "open" && c.network != "blocked" && c.network != "allowCidrs" {
		return c, fmt.Errorf("smol-cloud: network must be open, blocked, or allowCidrs")
	}
	c.cidrs, c.hosts = splitList(config["allow-cidrs"]), splitList(config["allow-hosts"])
	if c.network == "allowCidrs" && len(c.cidrs)+len(c.hosts) == 0 {
		return c, fmt.Errorf("smol-cloud: allowCidrs requires allow-cidrs or allow-hosts")
	}
	if c.network != "allowCidrs" && len(c.cidrs)+len(c.hosts) != 0 {
		return c, fmt.Errorf("smol-cloud: allow-cidrs and allow-hosts require network=allowCidrs")
	}
	for _, prefix := range c.cidrs {
		if _, err := netip.ParsePrefix(prefix); err != nil {
			return c, fmt.Errorf("smol-cloud: invalid CIDR %q: %w", prefix, err)
		}
	}
	for _, host := range c.hosts {
		if strings.ContainsAny(host, " /*:") || !strings.Contains(host, ".") {
			return c, fmt.Errorf("smol-cloud: invalid host %q", host)
		}
	}
	for _, resource := range []struct {
		key string
		dst *int
	}{{"cpus", &c.cpus}, {"memory-mb", &c.memoryMB}, {"disk-gb", &c.diskGB}} {
		key, dst := resource.key, resource.dst
		if config[key] == "" {
			continue
		}
		bits := 32
		if key == "disk-gb" {
			bits = strconv.IntSize
		}
		v, err := strconv.ParseInt(config[key], 10, bits)
		if err != nil || v <= 0 {
			return c, fmt.Errorf("smol-cloud: %s must be a positive integer", key)
		}
		*dst = int(v)
	}
	return c, nil
}
