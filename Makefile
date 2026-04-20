############################# Main targets #############################
# Rebuild binaries (used by Dockerfile).
bins: sandbox-worker-lambda workflow-starter workflow-worker backend-worker

# Install all tools, run all possible checks and tests (long but comprehensive).
all: clean bins

# Delete all build artifacts
clean: clean-bins
########################################################################

.PHONY: bins clean sandbox-worker-ecs

##### Variables ######

COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

ALL_SRC         := $(shell find . -name "*.go")
ALL_SRC         += sdk/go.mod consumer/go.mod

##### Binaries #####
clean-bins:
	@printf $(COLOR) "Delete old binaries..."
	@rm -f backend/worker
	@rm -f consumer/sandbox-worker-lambda.zip
	@rm -f consumer/workflow-starter
	@rm -f consumer/workflow-worker

sandbox-worker-agentcore: $(ALL_SRC)
	@printf $(COLOR) "Build sandbox-worker-agentcore container image with ko..."
	cd consumer && ko build ./cmd/sandbox-worker-agentcore

sandbox-worker-ecs: $(ALL_SRC)
	@printf $(COLOR) "Build sandbox-worker-ecs container image with ko..."
	cd consumer && ko build ./cmd/sandbox-worker-ecs

sandbox-worker-lambda: $(ALL_SRC)
	@printf $(COLOR) "Build sandbox-worker-lambda with CGO_ENABLED=$(CGO_ENABLED)..."
	cd consumer && CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o bootstrap ./cmd/sandbox-worker-lambda
	cd consumer && zip sandbox-worker-lambda.zip bootstrap && rm bootstrap

workflow-starter: $(ALL_SRC)
	@printf $(COLOR) "Build workflow-starter with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd consumer && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o workflow-starter ./cmd/starter

workflow-worker: $(ALL_SRC)
	@printf $(COLOR) "Build workflow-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd consumer && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o workflow-worker ./cmd/worker

backend-worker: $(ALL_SRC)
	@printf $(COLOR) "Build backend-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	cd backend && CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o worker ./cmd/worker
