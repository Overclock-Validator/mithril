package version

import (
	"runtime/debug"
	"strings"
)

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

// Commit returns the git commit the binary was built from: GitCommit when set
// at build time, otherwise the module's VCS revision shortened to 8
// characters, otherwise "unknown".
func Commit() string {
	if GitCommit != "" && GitCommit != "unknown" {
		return GitCommit
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				if len(setting.Value) > 8 {
					return setting.Value[:8]
				}
				return setting.Value
			}
		}
	}
	return "unknown"
}

// Semver returns Version as a bare semantic version, without a leading "v".
// A build whose Version is not a release, such as "dev", reports "0.0.0-dev".
func Semver() string {
	v := strings.TrimPrefix(Version, "v")
	if v == "" || v[0] < '0' || v[0] > '9' {
		return "0.0.0-dev"
	}
	return v
}
