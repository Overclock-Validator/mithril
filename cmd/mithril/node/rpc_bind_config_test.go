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

func TestRPCBindStartupConfig(t *testing.T) {
	const env = "MITHRIL_TEST_RPC_BIND_CONFIG"
	cases := []struct{ name, body, flag, want, wantErr string }{
		{name: "default", want: "0.0.0.0"},
		{name: "private", body: "bind_address = '127.0.0.1'", want: "127.0.0.1"},
		{name: "ipv6", body: "bind_address = '::1'", want: "::1"},
		{name: "flag overrides file", body: "bind_address = '0.0.0.0'", flag: "127.0.0.1", want: "127.0.0.1"},
		{name: "hostname rejected", body: "bind_address = 'localhost'", wantErr: "must be a literal IP address"},
		{name: "empty rejected", body: "bind_address = ''", wantErr: "must be a literal IP address"},
		{name: "port rejected", body: "bind_address = '127.0.0.1:8899'", wantErr: "must be a literal IP address"},
	}
	if name := os.Getenv(env); name != "" {
		for _, tc := range cases {
			if tc.name != name {
				continue
			}
			viper.Reset()
			config.ConfigFile = filepath.Join(t.TempDir(), "config.toml")
			require.NoError(t, os.WriteFile(config.ConfigFile, []byte("[rpc]\nport = 8899\n"+tc.body+"\n"), 0o600))
			viper.Set("storage.accounts", []string{t.TempDir()})
			if tc.flag != "" {
				require.NoError(t, Run.Flags().Set("rpc-bind-address", tc.flag))
			}
			err := initConfigAndBindFlags(&Run)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, 8899, rpcPort)
				require.Equal(t, tc.want, rpcBindAddress)
			}
			return
		}
		t.Fatalf("unknown case %q", name)
	}
	// Signature-verification initialization occurs once per process; isolate
	// config loads just as the existing cluster startup tests do.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestRPCBindStartupConfig$", "-test.count=1")
			cmd.Env = append(os.Environ(), env+"="+tc.name)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
		})
	}
}
