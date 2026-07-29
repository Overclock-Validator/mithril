package version

import (
	"runtime/debug"
	"testing"
)

func stamp(revision, modified string) []debug.BuildSetting {
	var out []debug.BuildSetting
	if revision != "" {
		out = append(out, debug.BuildSetting{Key: "vcs.revision", Value: revision})
	}
	if modified != "" {
		out = append(out, debug.BuildSetting{Key: "vcs.modified", Value: modified})
	}
	return out
}

func TestResolveFrom(t *testing.T) {
	const longSHA = "c69e8f3c9ff16432d1c04a1a2a68be1094faf23e"

	cases := []struct {
		name         string
		version      string
		commit       string
		branch       string
		settings     []debug.BuildSetting
		wantCommit   string
		wantModified bool
	}{
		{
			// `make build`: ldflags win, and they are already short.
			name: "ldflags set", version: "v1.0.0", commit: "abc1234", branch: "main",
			settings: stamp(longSHA, "false"), wantCommit: "abc1234",
		},
		{
			// plain `go build`: the whole point of the fallback.
			name: "ldflags absent", version: "dev", commit: "unknown", branch: "unknown",
			settings: stamp(longSHA, "false"), wantCommit: "c69e8f3c",
		},
		{
			// A dirty tree must be reported even when ldflags supplied a clean
			// hash, because `git rev-parse` cannot see uncommitted changes.
			name: "ldflags set but tree dirty", version: "v1.0.0", commit: "abc1234", branch: "main",
			settings: stamp(longSHA, "true"), wantCommit: "abc1234", wantModified: true,
		},
		{
			// -buildvcs=false, or built outside a checkout: no worse than before.
			name: "no stamp at all", version: "dev", commit: "unknown", branch: "unknown",
			settings: nil, wantCommit: "unknown",
		},
		{
			name: "empty commit treated as unset", version: "dev", commit: "", branch: "",
			settings: stamp(longSHA, "true"), wantCommit: "c69e8f3c", wantModified: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveFrom(tc.version, tc.commit, tc.branch, tc.settings)
			if got.Commit != tc.wantCommit {
				t.Errorf("Commit = %q, want %q", got.Commit, tc.wantCommit)
			}
			if got.Modified != tc.wantModified {
				t.Errorf("Modified = %v, want %v", got.Modified, tc.wantModified)
			}
			if got.Version != tc.version {
				t.Errorf("Version = %q, want %q", got.Version, tc.version)
			}
		})
	}
}
