package version

import "testing"

func TestCommitPrefersBuildTimeValue(t *testing.T) {
	saved := GitCommit
	t.Cleanup(func() { GitCommit = saved })

	GitCommit = "abc1234"
	if got := Commit(); got != "abc1234" {
		t.Fatalf("Commit() = %q, want the build-time value", got)
	}
	for _, unset := range []string{"", "unknown"} {
		GitCommit = unset
		got := Commit()
		if got == "" || (len(got) > 8 && got != "unknown") {
			t.Fatalf("Commit() with GitCommit=%q = %q, want a VCS revision of at most 8 characters or \"unknown\"", unset, got)
		}
	}
}

func TestSemver(t *testing.T) {
	saved := Version
	t.Cleanup(func() { Version = saved })

	for version, want := range map[string]string{
		"v1.2.3":       "1.2.3",
		"1.2.3":        "1.2.3",
		"v2.0.0-rc.1":  "2.0.0-rc.1",
		"dev":          "0.0.0-dev",
		"":             "0.0.0-dev",
		"unknown":      "0.0.0-dev",
		"v":            "0.0.0-dev",
		"v10.20.30+ab": "10.20.30+ab",
	} {
		Version = version
		if got := Semver(); got != want {
			t.Errorf("Semver() with Version=%q = %q, want %q", version, got, want)
		}
	}
}
