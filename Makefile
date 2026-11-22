VERSION ?= dev
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -X github.com/Overclock-Validator/mithril/pkg/version.Version=$(VERSION) \
           -X github.com/Overclock-Validator/mithril/pkg/version.GitCommit=$(GIT_COMMIT) \
           -X github.com/Overclock-Validator/mithril/pkg/version.GitBranch=$(GIT_BRANCH) \
           -X github.com/Overclock-Validator/mithril/pkg/version.BuildDate=$(BUILD_DATE)

DASHBOARD_BUNDLE ?= mithril-dashboard-bundle.tar.gz
DASHBOARD_BUNDLE_TARGET ?= native
export DASHBOARD_VIEWER_DIST DASHBOARD_VIEWER_MANIFEST DASHBOARD_BUNDLE DASHBOARD_BUNDLE_TARGET

.PHONY: build release dashboard-bundle clean server-setup disk-setup tune conformance-vectors test-conformance-elf test-conformance-vm-programs test-conformance-sbpf test-conformance-precompiles

build:
	go build -ldflags "$(LDFLAGS)" -o mithril ./cmd/mithril

release:
	go build -ldflags "$(LDFLAGS) -s -w" -o mithril ./cmd/mithril

dashboard-bundle:
	@set -eu; \
		if [ -z "$$DASHBOARD_VIEWER_DIST" ] || [ -z "$$DASHBOARD_VIEWER_MANIFEST" ]; then \
			echo "Set DASHBOARD_VIEWER_DIST and DASHBOARD_VIEWER_MANIFEST to the approved built viewer and its JSON asset map" >&2; \
			exit 2; \
		fi; \
		case "$$DASHBOARD_BUNDLE_TARGET" in \
			native|linux-amd64) ;; \
			*) echo "DASHBOARD_BUNDLE_TARGET must be native or linux-amd64" >&2; exit 2 ;; \
		esac; \
		target="$$DASHBOARD_BUNDLE_TARGET"; \
		case "$${DASHBOARD_BUNDLE##*/}" in *linux-amd64*) target=linux-amd64 ;; esac; \
		if [ "$$target" = linux-amd64 ] && [ "$$(go env GOOS GOARCH CGO_ENABLED)" != "$$(printf '%s\n' linux amd64 1)" ]; then \
			echo "Linux AMD64 bundles require GOOS=linux, GOARCH=amd64 and CGO_ENABLED=1" >&2; \
			exit 2; \
		fi; \
		stage=$$(mktemp -d); \
		trap 'rm -rf -- "$$stage"' EXIT; \
		go build -trimpath -ldflags "$(LDFLAGS) -s -w" -o "$$stage/" \
			./cmd/mithril ./cmd/mithril-dashboard ./cmd/mithril-dashboard-collect; \
		python3 scripts/dashboard-bundle.py --target "$$target" "$$stage" "$$DASHBOARD_VIEWER_DIST" \
			"$$DASHBOARD_VIEWER_MANIFEST" "$$DASHBOARD_BUNDLE"

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
