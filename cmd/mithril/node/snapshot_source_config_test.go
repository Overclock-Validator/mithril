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

func TestSnapshotSourceStartupConfig(t *testing.T) {
	const env = "MITHRIL_TEST_SNAPSHOT_SOURCE_CONFIG"
	const identity = "7Np41oeYqPefeNQEHSv1UDhYrehxin3NStELsSKCT4K2"
	cases := []struct{ name, body, wantErr string }{
		{"known validator", "known_validators = ['" + identity + "']", ""},
		{"empty allowlist", "known_validators = []", "requires nonempty snapshot.known_validators"},
		{"invalid identity", "known_validators = ['not-a-pubkey']", "invalid snapshot.known_validators identity"},
	}
	if name := os.Getenv(env); name != "" {
		for _, tc := range cases {
			if tc.name != name {
				continue
			}
			viper.Reset()
			config.ConfigFile = filepath.Join(t.TempDir(), "config.toml")
			require.NoError(t, os.WriteFile(config.ConfigFile, []byte("[snapshot]\nonly_known_rpc = true\n"+tc.body+"\n"), 0o600))
			viper.Set("storage.accounts", []string{t.TempDir()})
			err := initConfigAndBindFlags(&Run)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				cfg := buildSnapshotConfig(rpcEndpoints)
				require.True(t, cfg.OnlyKnownRPC)
				require.Equal(t, []string{identity}, cfg.KnownValidators)
			}
			return
		}
		t.Fatalf("unknown case %q", name)
	}
	// Startup initializes signature verification once per process.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestSnapshotSourceStartupConfig$", "-test.count=1")
			cmd.Env = append(os.Environ(), env+"="+tc.name)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
		})
	}
}
