############################# Main targets #############################
# Rebuild binaries (used by Dockerfile).
bins: examples-auto-suspend-starter examples-auto-suspend-worker examples-detached-sandbox-starter examples-detached-sandbox-worker examples-explicit-suspend-resume-starter examples-explicit-suspend-resume-worker examples-file-management-starter examples-file-management-worker examples-shared-sandbox-starter examples-shared-sandbox-worker examples-snapshot-fork-starter examples-snapshot-fork-worker sdk-sandbox-worker

# Install all tools, run all possible checks and tests (long but comprehensive).
all: clean bins

# Run the SDK test suite.
test:
	cd sdk && go test ./...

# Delete all build artifacts
clean: clean-bins
########################################################################

.PHONY: bins test clean sandbox-worker-ecs

##### Variables ######

COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

ALL_SRC         := $(shell find . -name "*.go")
ALL_SRC         += sdk/go.mod examples/auto-suspend/go.mod examples/detached-sandbox/go.mod examples/explicit-suspend-resume/go.mod examples/file-management/go.mod examples/shared-sandbox/go.mod examples/snapshot-fork/go.mod

##### Binaries #####
clean-bins:
	@printf $(COLOR) "Delete old binaries..."
	@rm -f examples/auto-suspend/starter
	@rm -f examples/auto-suspend/worker
	@rm -f examples/detached-sandbox/starter
	@rm -f examples/detached-sandbox/worker
	@rm -f examples/explicit-suspend-resume/starter
	@rm -f examples/explicit-suspend-resume/worker
	@rm -f examples/file-management/starter
	@rm -f examples/file-management/worker
	@rm -f examples/shared-sandbox/starter
	@rm -f examples/shared-sandbox/worker
	@rm -f examples/snapshot-fork/starter
	@rm -f examples/snapshot-fork/worker
	@rm -f sdk/sandbox-worker

examples-auto-suspend-starter: $(ALL_SRC)
	@printf $(COLOR) "Build examples-auto-suspend-starter with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/auto-suspend && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o starter ./cmd/starter

examples-auto-suspend-worker: $(ALL_SRC)
	@printf $(COLOR) "Build examples-auto-suspend-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/auto-suspend && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o worker ./cmd/worker

examples-detached-sandbox-starter: $(ALL_SRC)
	@printf $(COLOR) "Build examples-detached-sandbox-starter with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/detached-sandbox && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o starter ./cmd/starter

examples-detached-sandbox-worker: $(ALL_SRC)
	@printf $(COLOR) "Build examples-detached-sandbox-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/detached-sandbox && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o worker ./cmd/worker

examples-explicit-suspend-resume-starter: $(ALL_SRC)
	@printf $(COLOR) "Build examples-explicit-suspend-resume-starter with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/explicit-suspend-resume && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o starter ./cmd/starter

examples-explicit-suspend-resume-worker: $(ALL_SRC)
	@printf $(COLOR) "Build examples-explicit-suspend-resume-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/explicit-suspend-resume && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o worker ./cmd/worker

examples-shared-sandbox-starter: $(ALL_SRC)
	@printf $(COLOR) "Build examples-shared-sandbox-starter with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/shared-sandbox && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o starter ./cmd/starter

examples-shared-sandbox-worker: $(ALL_SRC)
	@printf $(COLOR) "Build examples-shared-sandbox-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/shared-sandbox && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o worker ./cmd/worker

examples-file-management-starter: $(ALL_SRC)
	@printf $(COLOR) "Build examples-file-management-starter with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/file-management && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o starter ./cmd/starter

examples-file-management-worker: $(ALL_SRC)
	@printf $(COLOR) "Build examples-file-management-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/file-management && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o worker ./cmd/worker

examples-snapshot-fork-starter: $(ALL_SRC)
	@printf $(COLOR) "Build examples-snapshot-fork-starter with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/snapshot-fork && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o starter ./cmd/starter

examples-snapshot-fork-worker: $(ALL_SRC)
	@printf $(COLOR) "Build examples-snapshot-fork-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd examples/snapshot-fork && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o worker ./cmd/worker

sdk-sandbox-worker: $(ALL_SRC)
	@printf $(COLOR) "Build sdk-sandbox-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd sdk && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o sandbox-worker ./cmd/sandbox-worker
