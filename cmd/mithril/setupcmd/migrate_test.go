package setupcmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrateConfigAddsRealSectionsAndIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(`
[network]
cluster = "mainnet-beta"
`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if !MigrateConfig(path) {
		t.Fatal("expected first migration to modify config")
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migrated config: %v", err)
	}
	body := string(first)
	if !hasTomlSection(body, "consensus") {
		t.Fatal("migration should add a real [consensus] section")
	}
	if !hasTomlSection(body, "lightbringer") {
		t.Fatal("migration should add a real [lightbringer] section")
	}
	if countExactLine(body, "[lightbringer]") != 1 {
		t.Fatalf("expected one [lightbringer] section, got:\n%s", body)
	}

	if MigrateConfig(path) {
		t.Fatal("second migration should be a no-op")
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config after second migration: %v", err)
	}
	if string(second) != body {
		t.Fatal("second migration changed config content")
	}
}

func TestMigrateConfigRecognizesInlineCommentSections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	original := `
[consensus] # vote-anchored consensus
skip_path_max_depth = 64

[lightbringer] # sidecar config
enabled = false
`
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if MigrateConfig(path) {
		t.Fatal("migration should be a no-op when sections exist with inline comments")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(data) != original {
		t.Fatal("migration changed an already migrated config")
	}
}

func countExactLine(content, want string) int {
	count := 0
	for _, line := range strings.Split(content, "\n") {
		if strings.TrimSpace(line) == want {
			count++
		}
	}
	return count
}
