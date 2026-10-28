package smolcloud

import (
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

const (
	// ConfigKeyWorkload selects a workload preset.
	ConfigKeyWorkload = "workload"
	// ConfigKeyBootstrap selects Worker task queue injection into the guest environment.
	ConfigKeyBootstrap = "bootstrap"
	// ConfigKeyLifecycle selects the execution lifecycle policy.
	ConfigKeyLifecycle = "lifecycle"
	// ConfigKeyBranchable controls checkpoint creation and restore support.
	ConfigKeyBranchable = "branchable"
)

type bootstrapMode string

const (
	bootstrapWorker bootstrapMode = "worker"
	bootstrapNone   bootstrapMode = "none"
)

// resolvedConfig keeps bootstrap, lifecycle, and API capabilities independent.
type resolvedConfig struct {
	bootstrap              bootstrapMode
	policy                 compute.ExecutionPolicy
	branchable             bool
	bound                  bool
	preset                 bool
	image, network         string
	cidrs, hosts           []string
	cpus, memoryMB, diskGB int
}

// resolveConfig is pure: Workflow admission and provider construction share validation.
func resolveConfig(config map[string]string) (resolvedConfig, error) {
	c := resolvedConfig{bootstrap: bootstrapWorker, branchable: true, cpus: 1, memoryMB: 512}
	switch config[ConfigKeyWorkload] {
	case "":
	case "native-process":
		c.preset, c.bound = true, true
		c.bootstrap, c.policy.Lifecycle, c.branchable = bootstrapNone, compute.LifecycleFresh, false
	default:
		return c, fmt.Errorf("smol-cloud: unsupported workload %q", config[ConfigKeyWorkload])
	}
	if raw, ok := config[ConfigKeyBootstrap]; ok {
		c.bound = true
		if raw != string(bootstrapWorker) && raw != string(bootstrapNone) {
			return c, fmt.Errorf("smol-cloud: bootstrap must be worker or none")
		}
		if c.preset && raw != string(bootstrapNone) {
			return c, fmt.Errorf("smol-cloud: bootstrap contradicts native-process preset")
		}
		c.bootstrap = bootstrapMode(raw)
	}
	if raw, ok := config[ConfigKeyLifecycle]; ok {
		c.bound = true
		switch raw {
		case "standard":
			if c.preset {
				return c, fmt.Errorf("smol-cloud: lifecycle contradicts native-process preset")
			}
			c.policy.Lifecycle = compute.LifecycleStandard
		case "fresh":
			c.policy.Lifecycle = compute.LifecycleFresh
		default:
			return c, fmt.Errorf("smol-cloud: lifecycle must be standard or fresh")
		}
	}
	if c.policy.Lifecycle == compute.LifecycleFresh {
		c.branchable = false
	}
	if raw, ok := config[ConfigKeyBranchable]; ok {
		c.bound = true
		if raw != "true" && raw != "false" {
			return c, fmt.Errorf("smol-cloud: branchable must be true or false")
		}
		c.branchable = raw == "true"
		if c.branchable && c.policy.Lifecycle == compute.LifecycleFresh {
			return c, fmt.Errorf("smol-cloud: fresh lifecycle requires branchable=false")
		}
	}
	if c.preset || c.policy.Lifecycle == compute.LifecycleFresh {
		keys := make([]string, 0, len(config))
		for key := range config {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			switch key {
			case ConfigKeyWorkload, ConfigKeyBootstrap, ConfigKeyLifecycle, ConfigKeyBranchable, "image", "network", "allow-cidrs", "allow-hosts", "cpus", "memory-mb", "disk-gb":
			default:
				if c.preset {
					return c, fmt.Errorf("smol-cloud: unsupported native-process configuration key %q", key)
				}
				return c, fmt.Errorf("smol-cloud: unsupported fresh lifecycle configuration key %q", key)
			}
		}
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

func resolvePolicy(config map[string]string) (compute.ExecutionPolicy, error) {
	c, err := resolveConfig(config)
	return c.policy, err
}
