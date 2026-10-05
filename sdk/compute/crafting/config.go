package crafting

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	crafting "github.com/crafting-demo/lightweight-go-client"
)

// defaultNamePrefix leads every derived sandbox name, so sandboxes this
// provider created are recognisable in a shared organization.
const defaultNamePrefix = "tsp"

// config is the provider's view of compute.ProviderDetails.Config. It holds the
// per-call choices the client takes as arguments, plus the client options
// themselves. Only template and workspace are required; everything else has a
// default supplied by the client.
type config struct {
	client crafting.Options

	template     string
	workspace    string
	dependencies []string

	namePrefix string
	usePool    string
	region     string

	snapshotFolder string
	snapshotBase   bool
	homeIncludes   []string
	homeExcludes   []string

	execUID  *int
	execDir  string
	extraEnv []string

	createTimeout   time.Duration
	commandTimeout  time.Duration
	snapshotTimeout time.Duration
}

func parseConfig(raw map[string]string) (*config, error) {
	get := func(k string) string { return strings.TrimSpace(raw[k]) }

	c := &config{
		client: crafting.Options{
			Org:       get("org"),
			Folder:    get("folder"),
			Token:     get("token"),
			ConfigDir: get("config-dir"),
			Binary:    get("cs-binary"),
		},
		template:       get("template"),
		workspace:      get("workspace"),
		namePrefix:     defaultNamePrefix,
		usePool:        get("use-pool"),
		region:         get("region"),
		snapshotFolder: get("snapshot-folder"),
		execDir:        get("exec-dir"),
	}

	if c.template == "" {
		return nil, fmt.Errorf("crafting: template is required")
	}
	if c.workspace == "" {
		return nil, fmt.Errorf("crafting: workspace is required")
	}
	if v := get("name-prefix"); v != "" {
		c.namePrefix = v
	}
	c.dependencies = splitList(get("dependencies"))
	c.homeIncludes = splitList(get("home-includes"))
	c.homeExcludes = splitList(get("home-excludes"))
	c.extraEnv = splitList(get("extra-env"))
	for _, e := range c.extraEnv {
		if !strings.Contains(e, "=") {
			return nil, fmt.Errorf("crafting: extra-env entry %q is not KEY=VALUE", e)
		}
	}

	if v := get("snapshot-base"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("crafting: invalid snapshot-base %q: %w", v, err)
		}
		c.snapshotBase = b
	}
	// Commands default to the workspace user, whose home directory is what home
	// snapshots capture; running as root would write into /root and be lost on a
	// fork. An explicit uid, including 0, overrides that.
	if v := get("exec-uid"); v != "" {
		uid, err := strconv.Atoi(v)
		if err != nil || uid < 0 {
			return nil, fmt.Errorf("crafting: invalid exec-uid %q", v)
		}
		c.execUID = crafting.UID(uid)
	}

	for key, target := range map[string]*time.Duration{
		"create-timeout":    &c.createTimeout,
		"command-timeout":   &c.commandTimeout,
		"snapshot-timeout":  &c.snapshotTimeout,
		"lifecycle-timeout": &c.client.LifecycleTimeout,
	} {
		v := get(key)
		if v == "" {
			continue
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("crafting: invalid %s %q: %w", key, v, err)
		}
		*target = d
	}
	return c, nil
}

func splitList(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
