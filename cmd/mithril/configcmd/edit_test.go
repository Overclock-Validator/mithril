package configcmd

import (
	"os"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestEditor_ScrLightbringerQuiet_True(t *testing.T) {
	m := &editModel{screen: edScrLightbringerQuiet, lbEnabled: true, lbQuiet: false}
	m.handleSelect("true")
	assert.True(t, m.lbQuiet, "picking 'true' should set lbQuiet=true")
}

func TestEditor_ScrLightbringerQuiet_False(t *testing.T) {
	m := &editModel{screen: edScrLightbringerQuiet, lbEnabled: true, lbQuiet: true}
	m.handleSelect("false")
	assert.False(t, m.lbQuiet, "picking 'false' should set lbQuiet=false")
}

// TestEditor_DisableLB_ResetsQuiet verifies that "disable" resets m.lbQuiet to
// the default so a later re-enable starts clean (no stale quiet state carried over).
func TestEditor_DisableLB_ResetsQuietDefault(t *testing.T) {
	// Seed lbQuiet opposite the default so the reset is observable.
	m := &editModel{
		screen:    edScrLightbringer,
		lbEnabled: true,
		lbQuiet:   !config.LightbringerQuietDefault,
	}
	m.handleSelect("disable")
	assert.False(t, m.lbEnabled, "disable should set lbEnabled=false")
	assert.Equal(t, config.LightbringerQuietDefault, m.lbQuiet, "disable should reset lbQuiet to the default to avoid stale state on re-enable")
}

// TestEditor_EnableLB_PreservesQuiet ensures enable does not clobber quiet.
func TestEditor_EnableLB_PreservesQuiet(t *testing.T) {
	m := &editModel{
		screen:    edScrLightbringer,
		lbEnabled: false,
		lbQuiet:   true,
	}
	m.handleSelect("enable")
	assert.True(t, m.lbEnabled)
	assert.True(t, m.lbQuiet, "enable should not modify lbQuiet")
}

// Quiet logs entry must not appear when LB is off.
func TestEditor_QuietMenuItem_HiddenWhenLBDisabled(t *testing.T) {
	m := editModel{screen: edScrLightbringer, lbEnabled: false}
	items := m.currentItems()
	for _, it := range items {
		assert.NotEqual(t, "quiet", it.value, "Quiet logs entry must not appear when lbEnabled=false")
	}
}

func TestEditor_QuietMenuItem_ShownWhenLBEnabled(t *testing.T) {
	m := editModel{screen: edScrLightbringer, lbEnabled: true}
	items := m.currentItems()
	found := false
	for _, it := range items {
		if it.value == "quiet" {
			found = true
			break
		}
	}
	assert.True(t, found, "Quiet logs entry should appear when lbEnabled=true")
}

func TestHasTomlSectionIgnoresCommentedHeaders(t *testing.T) {
	content := `
# [lightbringer]
[network] # active network section
cluster = "mainnet-beta"
`
	if hasTomlSection(content, "lightbringer") {
		t.Fatal("commented section header must not count as an existing section")
	}
	if !hasTomlSection(content, "network") {
		t.Fatal("real section header should be detected")
	}
}

func TestSetTomlValueUpdatesInlineCommentSection(t *testing.T) {
	content := `
[lightbringer] # managed sidecar
enabled = false
`
	updated := setTomlValue(content, "lightbringer", "enabled", "true")

	assert.Contains(t, updated, `[lightbringer] # managed sidecar`)
	assert.Contains(t, updated, `enabled = true`)
	assert.Equal(t, 1, strings.Count(updated, "[lightbringer]"))
}

func TestNewEditModelFallsBackToLegacyRPCList(t *testing.T) {
	v := viper.New()
	config.ApplyDefaults(v)
	v.Set("rpc.rpc", []string{
		"https://legacy-primary.example.invalid",
		"https://legacy-backup.example.invalid",
	})

	m := newEditModel("config.toml", v)
	if m.rpcEndpoint != "https://legacy-primary.example.invalid" {
		t.Fatalf("unexpected primary endpoint: %q", m.rpcEndpoint)
	}
	if len(m.rpcFull) != 2 || m.rpcFull[1] != "https://legacy-backup.example.invalid" {
		t.Fatalf("legacy failover endpoints were not preserved: %#v", m.rpcFull)
	}
}

func TestNewEditModelFallsBackToRuntimeStoragePaths(t *testing.T) {
	v := viper.New()
	config.ApplyDefaults(v)
	v.Set("ledger.accounts_path", "/legacy/accounts")
	v.Set("snapshot.download_path", "/legacy/snapshots")

	m := newEditModel("config.toml", v)

	assert.Equal(t, "/legacy/accounts", m.accountsPath)
	assert.Equal(t, "/legacy/snapshots", m.snapshotsPath)
}

func TestEditor_TxparAllowsEmptyToClearPinnedValue(t *testing.T) {
	m := &editModel{screen: edScrTuning, inputVal: "", txpar: "8", txparWasSet: true}

	if !m.validateAndApplyInput() {
		t.Fatalf("empty txpar should be accepted, got error: %s", m.inputErr)
	}
	assert.Equal(t, "", m.txpar)
	assert.True(t, m.txparWasSet)
}

func TestSaveConfig_EmptyTxparRemovesCanonicalAndLegacyKeys(t *testing.T) {
	path := t.TempDir() + "/config.toml"
	content := `
[network]
cluster = "mainnet-beta"
rpc = ["https://rpc.example.invalid"]

[block]
max_rps = 5
max_inflight = 2

[rpc]
port = 8899

[log]
level = "info"

[bootstrap]
mode = "auto"

[tuning]
txpar = 8

[replay]
txpar = 9
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	v := viper.New()
	config.ApplyDefaults(v)
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		t.Fatal(err)
	}

	m := &editModel{
		configFile:    path,
		v:             v,
		cluster:       "mainnet-beta",
		rpcEndpoint:   "https://rpc.example.invalid",
		rpcFull:       []string{"https://rpc.example.invalid"},
		blockMaxRPS:   "5",
		blockInflight: "2",
		rpcPort:       "8899",
		logLevel:      "info",
		bootstrapMode: "auto",
		txpar:         "",
		txparWasSet:   true,
	}
	m.saveConfig()
	if m.err != nil {
		t.Fatal(m.err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	updated := string(data)
	if strings.Contains(updated, "\ntxpar = 8") || strings.Contains(updated, "\ntxpar = 9") {
		t.Fatalf("txpar keys should be commented out, got:\n%s", updated)
	}
	assert.Contains(t, updated, "# txpar = 8")
	assert.Contains(t, updated, "# txpar = 9")
}

func TestSaveConfig_SnapshotsPathClearsShadowingDownloadPath(t *testing.T) {
	path := t.TempDir() + "/config.toml"
	content := `
[network]
cluster = "mainnet-beta"
rpc = ["https://rpc.example.invalid"]

[storage]
snapshots = "/old/storage-snapshots"

[snapshot]
download_path = "/old/download-path"

[block]
max_rps = 5
max_inflight = 2

[rpc]
port = 8899

[log]
level = "info"

[bootstrap]
mode = "auto"
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	v := viper.New()
	config.ApplyDefaults(v)
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		t.Fatal(err)
	}

	m := newEditModel(path, v)
	m.snapshotsPath = "/new/snapshots"
	m.saveConfig()
	if m.err != nil {
		t.Fatal(m.err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	updated := string(data)
	assert.Contains(t, updated, `snapshots = "/new/snapshots"`)
	assert.Contains(t, updated, `# download_path = "/old/download-path"`)
}
