package node

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func TestDashboardFeedConfig(t *testing.T) {
	tests := []struct {
		name, socket, group, goos string
		enabled                   bool
		wantSocket, wantGroup     string
		wantError                 bool
	}{
		{name: "disabled", goos: "linux"},
		{name: "manual", goos: "darwin", socket: "/private/feed.sock", group: "custom", wantSocket: "/private/feed.sock", wantGroup: "custom"},
		{name: "enabled", goos: "linux", enabled: true, wantSocket: "/run/mithril-dashboard-feed/dashboard.sock", wantGroup: "mithril-dashboard-feed"},
		{name: "unsupported platform", goos: "darwin", enabled: true, wantError: true},
		{name: "socket conflict", goos: "linux", enabled: true, socket: "/private/feed.sock", wantError: true},
		{name: "group conflict", goos: "linux", enabled: true, group: "custom", wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			socket, group, err := dashboardFeedConfig(tc.enabled, tc.socket, tc.group, tc.goos)
			if (err != nil) != tc.wantError || socket != tc.wantSocket || group != tc.wantGroup {
				t.Fatalf("got socket %q, group %q, error %v; want %q, %q, error=%t", socket, group, err, tc.wantSocket, tc.wantGroup, tc.wantError)
			}
		})
	}
}

func TestDashboardEnabledConfigPrecedence(t *testing.T) {
	flags := pflag.NewFlagSet("dashboard", pflag.ContinueOnError)
	flags.Bool("dashboard-enabled", false, "")
	v := viper.New()
	v.Set("dashboard.enabled", true)
	flag := flags.Lookup("dashboard-enabled")
	if !resolveBoolOption(flag, v.IsSet("dashboard.enabled"), v.GetBool("dashboard.enabled")) {
		t.Fatal("TOML setting did not enable dashboard")
	}
	if err := flags.Set("dashboard-enabled", "false"); err != nil {
		t.Fatal(err)
	}
	if resolveBoolOption(flag, v.IsSet("dashboard.enabled"), v.GetBool("dashboard.enabled")) {
		t.Fatal("explicit flag did not override TOML")
	}
}

func TestDashboardEnabledTOML(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the provisioned service profile is Linux-only")
	}
	const marker = "MITHRIL_TEST_DASHBOARD_ENABLED_TOML"
	if os.Getenv(marker) == "1" {
		viper.Reset()
		config.ConfigFile = filepath.Join(t.TempDir(), "config.toml")
		body := "[network]\ncluster = 'devnet'\n[dashboard]\nenabled = true\n"
		if err := os.WriteFile(config.ConfigFile, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		viper.Set("storage.accounts", []string{t.TempDir()})
		if err := initConfigAndBindFlags(&Run); err != nil {
			t.Fatal(err)
		}
		if dashboardSocket != "/run/mithril-dashboard-feed/dashboard.sock" || dashboardSocketGroup != "mithril-dashboard-feed" {
			t.Fatalf("unexpected feed configuration: socket %q, group %q", dashboardSocket, dashboardSocketGroup)
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestDashboardEnabledTOML$", "-test.count=1")
	cmd.Env = append(os.Environ(), marker+"=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("config check: %v\n%s", err, out)
	}
}
