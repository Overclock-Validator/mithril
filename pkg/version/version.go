package version

// These variables are set at build time via ldflags:
//   go build -ldflags "-X github.com/Overclock-Validator/mithril/pkg/version.Version=v1.0.0
//                      -X github.com/Overclock-Validator/mithril/pkg/version.GitCommit=abc123
//                      -X github.com/Overclock-Validator/mithril/pkg/version.BuildDate=2025-01-01"
var (
	// Version is the semantic version (e.g., "v1.0.0" or "dev")
	Version = "dev"

	// GitCommit is the git commit hash
	GitCommit = "unknown"

	// GitBranch is the git branch name
	GitBranch = "unknown"

	// BuildDate is the build timestamp
	BuildDate = "unknown"
)
