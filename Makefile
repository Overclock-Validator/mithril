VERSION ?= dev
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -X github.com/Overclock-Validator/mithril/pkg/version.Version=$(VERSION) \
           -X github.com/Overclock-Validator/mithril/pkg/version.GitCommit=$(GIT_COMMIT) \
           -X github.com/Overclock-Validator/mithril/pkg/version.GitBranch=$(GIT_BRANCH) \
           -X github.com/Overclock-Validator/mithril/pkg/version.BuildDate=$(BUILD_DATE)

.PHONY: build release clean server-setup disk-setup tune conformance-vectors test-conformance-elf test-conformance-vm-programs test-conformance-sbpf test-conformance-precompiles

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

# Firedancer's fixture corpus is ~7 GB and gitignored, so it is fetched rather
# than vendored. The revision is pinned: the corpus moves, and both its schema
# and its per-suite result counts move with it, so tracking main would make a
# passing run unreproducible and a regression indistinguishable from an upstream
# edit. Bump this deliberately and re-record the counts when you do.
CONFORMANCE_VECTORS_REV ?= a87fc430
conformance-vectors:
	@if [ ! -d conformance/test-vectors/.git ]; then \
		git clone --filter=blob:none --no-checkout \
			https://github.com/firedancer-io/test-vectors.git conformance/test-vectors; \
	fi
	@git -C conformance/test-vectors fetch --depth 1 origin $(CONFORMANCE_VECTORS_REV)
	@git -C conformance/test-vectors checkout --force --detach $(CONFORMANCE_VECTORS_REV)
	@echo "conformance corpus pinned at $(CONFORMANCE_VECTORS_REV)"

test-conformance-precompiles:
	go test ./conformance/ -run 'TestConformance_Precompile_' -timeout 90m -v

test-conformance-elf:
	go test ./conformance/ -run TestConformance_ElfLoader_Firedancer -v

test-conformance-vm-programs:
	go test ./conformance/ -run TestConformance_VMPrograms_Firedancer -v

test-conformance-sbpf:
	go test ./conformance/ -run '^(TestConformance_ElfLoader_Firedancer|TestConformance_VMPrograms_Firedancer)$$' -v
