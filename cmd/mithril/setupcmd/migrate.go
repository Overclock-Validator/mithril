package setupcmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/tui"
)

// MigrateConfig checks if a config file is missing [lightbringer] or [consensus]
// sections and adds them. Returns true if any migration was performed.
func MigrateConfig(configPath string) bool {
	data, err := os.ReadFile(configPath)
	if err != nil {
		// don't treat a read failure as up-to-date
		fmt.Printf("  %s Failed to read config: %v\n", errorStyle.Render("✗"), err)
		return false
	}

	content := string(data)
	hasLB := hasTomlSection(content, "lightbringer")
	hasConsensus := hasTomlSection(content, "consensus")

	if hasLB && hasConsensus {
		return false // already up to date
	}

	var additions string

	if !hasConsensus {
		additions += `
# ============================================================================
# [consensus] - Vote-Anchored Consensus (added by mithril setup --migrate)
# ============================================================================
[consensus]
skip_path_max_depth = 64
unresolved_policy = "halt"
enforce_on_source = "stream"
`
	}

	if !hasLB {
		additions += `
# ============================================================================
# [lightbringer] - Lightbringer Sidecar (added by mithril setup --migrate)
# ============================================================================
[lightbringer]
enabled = false
binary_path = "./lightbringer"
gossip_entrypoint = ""
gossip_port = 65400
port_range_start = 65401
port_range_end = 65500
# shredstore path is in [storage] section (storage.shredstore)
rpc_addr = "127.0.0.1:3000"
grpc_addr = "127.0.0.1:3001"
`
	}

	newContent := strings.TrimRight(content, "\n") + "\n" + additions

	if err := tui.AtomicWriteFile(configPath, []byte(newContent), 0600); err != nil {
		fmt.Printf("  %s Failed to update config: %v\n", errorStyle.Render("✗"), err)
		return false
	}

	return true
}
