package node

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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

func TestDashboardPublicOriginConfig(t *testing.T) {
	const origin = "https://dashboard.example.com"
	for _, tc := range []struct {
		name, origin, profile, socket, group, goos string
		enabled                                    bool
		wantError                                  string
	}{
		{name: "unset"},
		{name: "provisioned", origin: origin, profile: "standard", goos: "linux", enabled: true},
		{name: "profile missing", origin: origin, goos: "linux", enabled: true, wantError: "requires dashboard.profile=standard"},
		{name: "feed disabled", origin: origin, profile: "standard", goos: "linux", wantError: "requires the dashboard feed to be enabled"},
		{name: "custom socket", origin: origin, profile: "standard", socket: "/private/node.sock", goos: "linux", wantError: "cannot be combined"},
		{name: "custom group", origin: origin, profile: "standard", group: "custom", goos: "linux", enabled: true, wantError: "cannot be combined"},
		{name: "non Linux", origin: origin, profile: "standard", goos: "darwin", enabled: true, wantError: "requires the provisioned Linux"},
		{name: "plaintext", origin: "http://dashboard.example.com", profile: "standard", goos: "linux", enabled: true, wantError: "must be an HTTPS origin"},
		{name: "URL path", origin: origin + "/path", profile: "standard", goos: "linux", enabled: true, wantError: "must be an HTTPS origin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDashboardPublicOrigin(tc.origin, tc.profile, tc.socket, tc.group, tc.goos, tc.enabled)
			if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("origin validation error = %v, want %q", err, tc.wantError)
			}
		})
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

func TestDashboardStandardProfile(t *testing.T) {
	const marker = "MITHRIL_TEST_DASHBOARD_STANDARD_PROFILE"
	switch mode := os.Getenv(marker); mode {
	case "default", "overrides", "group-only", "managed-origin", "managed-disabled":
		viper.Reset()
		config.ConfigFile = filepath.Join(t.TempDir(), "config.toml")
		socket := filepath.Join(t.TempDir(), "dashboard.sock")
		body := "[network]\ncluster = 'devnet'\n[dashboard]\nprofile = 'standard'\n"
		if mode == "managed-origin" || mode == "managed-disabled" {
			body += "public_origin = 'https://toml.example.com'\nenabled = true\n"
			if mode == "managed-origin" {
				if err := Run.Flags().Set("dashboard-public-origin", "https://flag.example.com"); err != nil {
					t.Fatal(err)
				}
			} else if err := Run.Flags().Set("dashboard-enabled", "false"); err != nil {
				t.Fatal(err)
			}
		} else if mode == "group-only" {
			body += "socket_group = 'dashboard-reader'\n"
		} else {
			body += "socket = '" + socket + "'\n"
		}
		if mode == "overrides" {
			body += "replay_details = true\nstorage_details = false\n"
			if err := Run.Flags().Set("dashboard-replay-details", "false"); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(config.ConfigFile, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		viper.Set("storage.accounts", []string{t.TempDir()})
		err := initConfigAndBindFlags(&Run)
		if mode == "managed-disabled" {
			if dashboardPublicOrigin != "https://toml.example.com" || err == nil || !strings.Contains(err.Error(), "requires the dashboard feed to be enabled") {
				t.Fatalf("disabled managed origin = %q, error = %v", dashboardPublicOrigin, err)
			}
			return
		}
		if mode == "managed-origin" {
			if dashboardPublicOrigin != "https://flag.example.com" {
				t.Fatalf("managed origin = %q, want explicit flag", dashboardPublicOrigin)
			}
			if runtime.GOOS == "linux" {
				if err != nil || dashboardSocket != "/run/mithril-dashboard-feed/dashboard.sock" || dashboardSocketGroup != "mithril-dashboard-feed" {
					t.Fatalf("managed feed = %q, group = %q, error = %v", dashboardSocket, dashboardSocketGroup, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "requires the provisioned Linux") {
				t.Fatalf("unexpected non-Linux origin validation: %v", err)
			}
			return
		}
		if mode == "group-only" {
			if err == nil || err.Error() != "dashboard.socket_group requires dashboard.socket with dashboard.profile=standard" {
				t.Fatalf("unexpected error with group and no socket: %v", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if dashboardSocket != socket || !dashboardHostResources || !dashboardNetworkDetails || !dashboardLocalPeers || !dashboardCacheDetails {
			t.Fatal("standard profile did not select the configured feed and standard observations")
		}
		if dashboardCacheActivity || dashboardMachineDetails || dashboardCgroupResources || dashboardShredHistory || dashboardProductionDetails {
			t.Fatal("standard profile enabled an independently opted-in observation")
		}
		enabled := mode == "default"
		if dashboardReplayDetails != enabled || dashboardFastExecuted != enabled ||
			dashboardStorageDetails != enabled || dashboardIODetails != enabled {
			t.Fatal("explicit settings did not take precedence over paired preset defaults")
		}
	default:
		for _, mode := range []string{"default", "overrides", "group-only", "managed-origin", "managed-disabled"} {
			t.Run(mode, func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestDashboardStandardProfile$", "-test.count=1")
				cmd.Env = append(os.Environ(), marker+"="+mode)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("config check: %v\n%s", err, out)
				}
			})
		}
	}
}
