package setupcmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestHandleSelect_ScrLightbringerQuiet verifies that picking an option on
// the new quiet-mode screen sets m.lbQuiet correctly.
func TestHandleSelect_ScrLightbringerQuiet_True(t *testing.T) {
	m := setupModel{screen: scrLightbringerQuiet, mode: "full"}
	out, _ := m.handleSelect("true")
	model := out.(setupModel)
	assert.True(t, model.lbQuiet, "picking 'true' should set lbQuiet=true")
}

func TestHandleSelect_ScrLightbringerQuiet_False(t *testing.T) {
	m := setupModel{screen: scrLightbringerQuiet, mode: "full", lbQuiet: true}
	out, _ := m.handleSelect("false")
	model := out.(setupModel)
	assert.False(t, model.lbQuiet, "picking 'false' should set lbQuiet=false")
}

// TestHandleSelect_DisableLB_ResetsQuiet verifies that picking "disable" on
// Lightbringer resets m.lbQuiet to the default so a later re-enable starts clean.
func TestHandleSelect_DisableLB_ResetsQuietDefault(t *testing.T) {
	m := setupModel{
		screen:   scrLightbringer,
		mode:     "full",
		enableLB: true,
		lbQuiet:  true, // user previously picked Quiet
	}
	out, _ := m.handleSelect("disable")
	model := out.(setupModel)
	assert.False(t, model.enableLB, "disable should set enableLB=false")
	assert.True(t, model.lbQuiet, "disable should reset lbQuiet to the default to avoid stale state on re-enable")
}

// TestHandleSelect_EnableLB_DoesNotChangeQuiet ensures picking "enable"
// preserves whatever quiet value the user had (so toggling enable->enable
// doesn't reset it).
func TestHandleSelect_EnableLB_PreservesQuiet(t *testing.T) {
	m := setupModel{
		screen:   scrLightbringer,
		mode:     "full",
		enableLB: false,
		lbQuiet:  true, // shouldn't be true if enableLB was false, but test the invariant
	}
	out, _ := m.handleSelect("enable")
	model := out.(setupModel)
	assert.True(t, model.enableLB)
	assert.True(t, model.lbQuiet, "enable should not modify lbQuiet")
}

// TestNewSetupModel_StoragePathsPopulated verifies the model picks up storage
// defaults from config.DefaultStoragePaths() at construction.
func TestNewSetupModel_StoragePathsPopulated(t *testing.T) {
	m := newSetupModel()
	assert.NotEmpty(t, m.accountsPath)
	assert.NotEmpty(t, m.snapshotsPath)
	assert.NotEmpty(t, m.logsPath)
	assert.NotEmpty(t, m.shredstorePath)
	// Specific paths depend on the host's /mnt/mithril-accounts writability;
	// the all-or-nothing invariant is covered by pkg/config tests.
}

func TestClusterMenuOffersBuiltinAlpenglowClusters(t *testing.T) {
	for _, nodeType := range []string{"verifying", "validator"} {
		m := setupModel{screen: scrCluster, nodeType: nodeType}
		var values []string
		for _, item := range m.currentItems() {
			if item.value != "" && item.value != "_back" {
				values = append(values, item.value)
			}
		}
		assert.Contains(t, values, "mainnet", nodeType)
		assert.Contains(t, values, "testnet", nodeType)
		assert.Contains(t, values, "devnet", nodeType)
		assert.NotContains(t, values, "alpenglow", nodeType)
		assert.NotContains(t, values, "mainnet-beta", nodeType)
	}
}

func TestQuickSetupAsksForTurbineGossip(t *testing.T) {
	m := setupModel{screen: scrRPC, mode: "quick", nodeType: "verifying", cluster: "mainnet"}
	m.advanceFromInput()
	assert.Equal(t, scrGossip, m.screen)
}

func TestClusterChoicePrefillsNetworkDefaults(t *testing.T) {
	m := newSetupModel()
	assert.Equal(t, "mainnet", m.cluster)
	assert.Equal(t, "entrypoint.mainnet-beta.solana.com:8001", m.gossipEntry)

	m.screen = scrCluster
	next, _ := m.handleSelect("testnet")
	m = next.(setupModel)
	assert.Equal(t, "testnet", m.cluster)
	assert.Equal(t, "https://api.testnet.solana.com", m.rpcEndpoint)
	assert.Equal(t, "entrypoint.testnet.solana.com:8001", m.gossipEntry)
}

func TestSetupGeneratesTurbineSourceForEveryCluster(t *testing.T) {
	for _, cluster := range []string{"mainnet", "testnet", "devnet"} {
		m := newSetupModel()
		m.mode = "quick"
		m.nodeType = "verifying"
		m.cluster = cluster
		m.configPath = filepath.Join(t.TempDir(), "config.toml")

		m.generateConfig()
		raw, err := os.ReadFile(m.configPath)
		assert.NoError(t, err)
		content := string(raw)
		assert.Contains(t, content, "cluster = \""+cluster+"\"")
		assert.Contains(t, content, "source = \"turbine\"")
		assert.Contains(t, content, "[turbine]")
		assert.False(t, strings.Contains(content, "source = \"rpc\""))
	}
}
