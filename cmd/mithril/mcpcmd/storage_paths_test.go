package mcpcmd

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestResolvedConfigAccountsDirectories(t *testing.T) {
	for _, key := range []string{"[storage]\naccounts", "[ledger]\naccounts_path"} {
		t.Run(key, func(t *testing.T) {
			clearMCPNodeSettingEnv(t)
			setConfigFileForTest(t, writeConfigFile(t, key+" = ['/disk-a/accounts', '/disk-b/accounts']\n"))
			cfg, err := resolvedConfig()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.AccountsDir != "/disk-a/accounts" || !reflect.DeepEqual(cfg.AccountsDirs, []string{"/disk-a/accounts", "/disk-b/accounts"}) {
				t.Fatalf("accounts paths = %q, %v", cfg.AccountsDir, cfg.AccountsDirs)
			}
			if cfg.StatePath != filepath.Join(cfg.AccountsDir, "mithril_state.json") {
				t.Fatalf("state path = %q", cfg.StatePath)
			}
			t.Setenv("MITHRIL_ACCOUNTS_PATH", "/override/accounts")
			cfg, err = resolvedConfig()
			if err != nil || !reflect.DeepEqual(cfg.AccountsDirs, []string{"/override/accounts"}) {
				t.Fatalf("environment override: paths %v, error %v", cfg.AccountsDirs, err)
			}
			t.Setenv("MITHRIL_ACCOUNTS_PATH", "")
			cfg, err = resolvedConfig()
			if err != nil || cfg.AccountsDir != "" || len(cfg.AccountsDirs) != 0 {
				t.Fatalf("disabled accounts: paths %v, error %v", cfg.AccountsDirs, err)
			}
		})
	}
}

func TestResolvedConfigRejectsInvalidAccountsDirectories(t *testing.T) {
	for _, paths := range []string{"['/disk-a/accounts', 'relative/accounts']", "['', '/disk-b/accounts']"} {
		t.Run(paths, func(t *testing.T) {
			clearMCPNodeSettingEnv(t)
			setConfigFileForTest(t, writeConfigFile(t, "[storage]\naccounts = "+paths+"\n"))
			if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "must be absolute") {
				t.Fatalf("invalid accounts paths error = %v", err)
			}
		})
	}
}
