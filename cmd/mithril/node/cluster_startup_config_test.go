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

const clusterConfigCaseEnv = "MITHRIL_TEST_CLUSTER_CONFIG_CASE"

const (
	customCluster       = "[network]\ncluster = 'private'\ngenesis_hash = '11111111111111111111111111111111'\n"
	customClusterRPC    = "rpc = ['http://127.0.0.1:8899']\n"
	managedLightbringer = "[lightbringer]\nenabled = true\ngossip_entrypoint = '127.0.0.1:8001'\n"
)

// Each case runs the node's config loading on a TOML body, as `mithril run
// --config` would. A successful load configures signature verification, which
// is once per process, so every case runs in its own test process.
var clusterConfigCases = []struct {
	name, body, wantErr string
	check               func(t *testing.T)
}{
	{name: "builtin cluster defaults its RPC before source validation", body: "[network]\ncluster = 'devnet'\n", check: func(t *testing.T) {
		devnet, err := config.ResolveCluster("devnet", "")
		require.NoError(t, err)
		require.Equal(t, []string{devnet.RPC}, rpcEndpoints)
		require.Equal(t, "turbine", blockSource)
		require.Equal(t, devnet.GossipEntrypoint, turbineGossipEntrypoint)
	}},
	{name: "builtin cluster keeps a configured RPC", body: "[network]\ncluster = 'mainnet'\nrpc = ['http://127.0.0.1:8899']\n", check: func(t *testing.T) {
		require.Equal(t, []string{"http://127.0.0.1:8899"}, rpcEndpoints)
	}},
	{name: "custom cluster requires an RPC", body: customCluster, wantErr: "requires network.rpc"},
	{name: "custom cluster on turbine requires an entrypoint", body: customCluster + customClusterRPC, wantErr: "requires turbine.gossip_entrypoint"},
	{name: "custom cluster with lightbringer selected by default", body: customCluster + customClusterRPC + managedLightbringer, check: func(t *testing.T) {
		require.Equal(t, "lightbringer", blockSource)
		require.Empty(t, turbineGossipEntrypoint)
	}},
	{name: "custom cluster with lightbringer selected explicitly", body: customCluster + customClusterRPC + "[block]\nsource = 'lightbringer'\n" + managedLightbringer, check: func(t *testing.T) {
		require.Equal(t, "lightbringer", blockSource)
	}},
}

func TestClusterStartupConfig(t *testing.T) {
	if name := os.Getenv(clusterConfigCaseEnv); name != "" {
		for _, tc := range clusterConfigCases {
			if tc.name != name {
				continue
			}
			viper.Reset()
			config.ConfigFile = filepath.Join(t.TempDir(), "mithril.toml")
			require.NoError(t, os.WriteFile(config.ConfigFile, []byte(tc.body), 0o600))
			viper.Set("storage.accounts", []string{t.TempDir()})
			err := initConfigAndBindFlags(&Run)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			tc.check(t)
			return
		}
		t.Fatalf("unknown case %q", name)
	}
	for _, tc := range clusterConfigCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestClusterStartupConfig$", "-test.count=1")
			cmd.Env = append(os.Environ(), clusterConfigCaseEnv+"="+tc.name)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
		})
	}
}
