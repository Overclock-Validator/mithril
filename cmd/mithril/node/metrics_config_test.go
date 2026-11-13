package node

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// Configuration initialization includes once-per-process signature verification.
// Isolate cases just as the cluster startup configuration tests do.
func TestMetricsStartupConfig(t *testing.T) {
	const caseEnv = "MITHRIL_TEST_METRICS_CONFIG_CASE"
	cases := []struct{ name, body, cli, want, wantErr string }{
		{name: "default", want: ":9090"},
		{name: "toml", body: "[metrics]\nbind_addr = '127.0.0.1:19090'\n", want: "127.0.0.1:19090"},
		{name: "cli precedence", body: "[metrics]\nbind_addr = '127.0.0.1:19090'\n", cli: "127.0.0.1:29090", want: "127.0.0.1:29090"},
		{name: "ipv6", cli: "[::1]:9090", want: "[::1]:9090"},
		{name: "missing port", cli: "127.0.0.1", wantErr: "invalid metrics.bind_addr"},
		{name: "invalid port", cli: "127.0.0.1:65536", wantErr: "invalid metrics.bind_addr"},
		{name: "empty address", body: "[metrics]\nbind_addr = ''\n", wantErr: "invalid metrics.bind_addr"},
	}
	if name := os.Getenv(caseEnv); name != "" {
		for _, tc := range cases {
			if tc.name != name {
				continue
			}
			viper.Reset()
			config.ConfigFile = filepath.Join(t.TempDir(), "mithril.toml")
			require.NoError(t, os.WriteFile(config.ConfigFile, []byte(tc.body), 0o600))
			viper.Set("storage.accounts", []string{t.TempDir()})
			if tc.cli != "" {
				require.NoError(t, Run.Flags().Set("metrics-bind-addr", tc.cli))
			}
			err := initConfigAndBindFlags(&Run)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, metricsBindAddr)
			return
		}
		t.Fatalf("unknown case %q", name)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestMetricsStartupConfig$", "-test.count=1")
			cmd.Env = append(os.Environ(), caseEnv+"="+tc.name)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
		})
	}
}
