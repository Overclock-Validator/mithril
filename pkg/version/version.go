package version

import "runtime/debug"

// These variables are set at build time via ldflags:
//
//	go build -ldflags "-X github.com/Overclock-Validator/mithril/pkg/version.Version=v1.0.0
//	                   -X github.com/Overclock-Validator/mithril/pkg/version.GitCommit=abc123
//	                   -X github.com/Overclock-Validator/mithril/pkg/version.BuildDate=2025-01-01"
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

// Identity is the resolved build identity of the running binary.
type Identity struct {
	Version  string
	Commit   string
	Branch   string
	Modified bool
}

// Resolve reports what this binary actually is, preferring ldflags and falling
// back to the VCS stamp Go records automatically.
//
// The fallback is what makes this reliable. `make build` sets the vars above,
// but a plain `go build` does not -- and since Go 1.18 it embeds vcs.revision
// and vcs.modified into the binary regardless, so a build made either way can
// still identify itself. Modified comes only from the stamp: `git rev-parse` in
// the Makefile reports a clean hash even with uncommitted changes, so ldflags
// alone cannot tell you whether what ran matches what is committed.
func Resolve() Identity {
	var settings []debug.BuildSetting
	if info, ok := debug.ReadBuildInfo(); ok {
		settings = info.Settings
	}
	return resolveFrom(Version, GitCommit, GitBranch, settings)
}

// resolveFrom holds the whole decision so it can be tested. Resolve cannot be:
// debug.ReadBuildInfo has no seam, and a `go test` binary carries no VCS stamp
// at all -- only `go build` does -- so a test calling Resolve directly would
// fail on a correct implementation and prove nothing about an incorrect one.
func resolveFrom(version, commit, branch string, settings []debug.BuildSetting) Identity {
	id := Identity{Version: version, Commit: commit, Branch: branch}

	var revision string
	for _, setting := range settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			id.Modified = setting.Value == "true"
		}
	}

	if (id.Commit == "" || id.Commit == "unknown") && revision != "" {
		if len(revision) > 8 {
			revision = revision[:8]
		}
		id.Commit = revision
	}
	return id
}
