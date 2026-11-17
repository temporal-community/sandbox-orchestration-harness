package smolcloud

type machineResources struct {
	CPUs     int `json:"cpus,omitempty"`
	MemoryMB int `json:"memoryMb,omitempty"`
	DiskGB   int `json:"diskGb,omitempty"`
}

type machineSource struct {
	Type      string `json:"type"`
	Reference string `json:"reference"`
}

type createMachineRequest struct {
	Name       string            `json:"name"`
	Source     machineSource     `json:"source"`
	Resources  machineResources  `json:"resources"`
	Network    networkPolicy     `json:"network"`
	Branchable bool              `json:"branchable"`
	Env        map[string]string `json:"env"`
	Labels     map[string]string `json:"labels"`
}

type createdMachine struct {
	ID      string        `json:"id"`
	Network networkPolicy `json:"network"`
}

type networkPolicy struct {
	Mode  string   `json:"mode"`
	CIDRs []string `json:"cidrs,omitempty"`
	Hosts []string `json:"hosts,omitempty"`
}
