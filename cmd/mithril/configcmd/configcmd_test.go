package configcmd

import (
	"regexp"
	"strings"
	"testing"
)

func TestGenerateStarterConfigInheritsAdaptiveAccountIndexConcurrency(t *testing.T) {
	generated := generateStarterConfig(false)
	for _, key := range []string{
		"account_index_checkpoint_workers",
		"account_index_max_concurrent_seals",
	} {
		assignment := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(key) + `\s*=`)
		if assignment.MatchString(generated) {
			t.Fatalf("starter config pins host-adaptive setting %s", key)
		}
	}
	if !strings.Contains(generated, "GOMAXPROCS workers, at most 8 seals") {
		t.Fatal("starter config does not explain inherited account-index concurrency defaults")
	}
	if !regexp.MustCompile(`(?m)^working_set_max_mb\s*=\s*1024\s`).MatchString(generated) {
		t.Fatal("starter config does not install the production WorkingSet memory threshold")
	}
	for _, assignment := range []string{
		"enabled = true",
		"enforce_disk_reserve = true",
		"min_free_mb = 0",
		"target_free_mb = 0",
		"max_source_mb = 64",
	} {
		if !strings.Contains(generated, assignment) {
			t.Fatalf("starter config does not install appendvec disk policy %q", assignment)
		}
	}
}
