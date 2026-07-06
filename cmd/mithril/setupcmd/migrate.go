package setupcmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/tui"
)

// MigrateConfig checks if a config file is missing [lightbringer], [consensus],
// or [validator]
// sections and adds them. Returns true if any migration was performed.
func MigrateConfig(configPath string) bool {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return false
	}

	content := string(data)
	hasLB := strings.Contains(content, "[lightbringer]")
	hasConsensus := strings.Contains(content, "[consensus]")
	hasValidator := strings.Contains(content, "[validator]")

	if hasLB && hasConsensus && hasValidator {
		return false // already up to date
	}

	var additions string

	if !hasConsensus {
		additions += `
# ============================================================================
# [consensus] - Alpenglow Observer (added by mithril setup --migrate)
# ============================================================================
# [consensus]
# alpenglow_observer_bind_addr = ""  # Optional Votor QUIC listener (raw-vote cert feed)
# alpenglow_max_message_bytes = 0
# alpenglow_bls_dst = ""
`
	}

	if !hasValidator {
		additions += `
# ============================================================================
# [validator] - Optional Validator Identity (added by mithril setup --migrate)
# ============================================================================
# identity_keypair is used by native turbine gossip. Set it to a staked
# validator identity for better turbine tree positioning.
#
# [validator]
# identity_keypair = ""
# vote_account_keypair = ""
# authorized_withdrawer_keypair = ""
`
	}

	if !hasLB {
		additions += `
# ============================================================================
# [lightbringer] - Lightbringer Sidecar (added by mithril setup --migrate)
# ============================================================================
# Enable to manage Lightbringer from Mithril. See config.example.toml for details.
#
# [lightbringer]
# enabled = false
# binary_path = "./lightbringer"
# gossip_entrypoint = ""
# shredstore path is in [storage] section (storage.shredstore)
# rpc_addr = "127.0.0.1:3000"
# grpc_addr = "127.0.0.1:3001"
`
	}

	newContent := strings.TrimRight(content, "\n") + "\n" + additions

	if err := tui.AtomicWriteFile(configPath, []byte(newContent), 0600); err != nil {
		fmt.Printf("  %s Failed to update config: %v\n", errorStyle.Render("✗"), err)
		return false
	}

	return true
}
