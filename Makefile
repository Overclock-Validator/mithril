VERSION ?= dev
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -X github.com/Overclock-Validator/mithril/pkg/version.Version=$(VERSION) \
           -X github.com/Overclock-Validator/mithril/pkg/version.GitCommit=$(GIT_COMMIT) \
           -X github.com/Overclock-Validator/mithril/pkg/version.GitBranch=$(GIT_BRANCH) \
           -X github.com/Overclock-Validator/mithril/pkg/version.BuildDate=$(BUILD_DATE)

.PHONY: build release clean server-setup disk-setup tune

build:
	go build -ldflags "$(LDFLAGS)" -o mithril ./cmd/mithril

release:
	go build -ldflags "$(LDFLAGS) -s -w" -o mithril ./cmd/mithril

clean:
	rm -f mithril

# Server setup scripts (require sudo - run as: sudo make server-setup ...)
server-setup:
	./scripts/server-setup.sh $(ARGS)

disk-setup:
	./scripts/disk-setup.sh $(ARGS)

tune:
	./scripts/performance-tune.sh $(ARGS)
