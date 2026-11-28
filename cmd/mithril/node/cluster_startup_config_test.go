package node

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/replay"
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
	{name: "offline replay turns off every network component", body: offlineReplayConfig, check: func(t *testing.T) {
		require.True(t, offlineReplay)
		require.Equal(t, "turbine", blockSource)
		require.Empty(t, turbineGossipEntrypoint)
		require.Empty(t, turbineAdvertisedIP)
		require.Empty(t, alpenglowObserverBindAddr)
		require.Equal(t, "127.0.0.1:0", turbineBindAddr)
		require.Equal(t, 50093, turbineShredVersion)
		require.False(t, replay.DelegatedFinalityEnabled, "offline rooting follows footer certificates only")
		require.Empty(t, rpcEndpoints, "offline replay ignores network.rpc")
	}},
	{name: "offline replay needs a shred version", body: strings.Replace(offlineReplayConfig, "shred_version = 50093", "shred_version = 0", 1), wantErr: "turbine.shred_version"},
	{name: "offline replay needs a finite range", body: strings.Replace(offlineReplayConfig, "end_slot = 100", "end_slot = -1", 1), wantErr: "replay.end_slot or replay.num_slots"},
	{name: "offline replay needs an existing AccountsDB", body: strings.Replace(offlineReplayConfig, "mode = 'accountsdb'", "mode = 'auto'", 1), wantErr: "bootstrap.mode=accountsdb"},
	{name: "offline replay on a built-in cluster does not default to its public RPC", body: strings.Replace(offlineReplayConfig, "rpc = ['http://127.0.0.1:8899']\n", "", 1), check: func(t *testing.T) {
		require.Empty(t, rpcEndpoints)
		require.Equal(t, "turbine", blockSource)
	}},
	{name: "offline replay on a custom cluster needs no RPC", body: strings.Replace(strings.Replace(offlineReplayConfig, "cluster = 'mainnet'", "cluster = 'private'\ngenesis_hash = '11111111111111111111111111111111'", 1), "rpc = ['http://127.0.0.1:8899']\n", "", 1) + "[block]\nsource = 'turbine'\n", check: func(t *testing.T) {
		require.Empty(t, rpcEndpoints)
		require.Equal(t, "private", cluster)
	}},
	{name: "replay phase timers default on", body: "[network]\ncluster = 'devnet'\n", check: func(t *testing.T) {
		require.False(t, replay.SkipPhaseTimers)
	}},
	{name: "replay phase timers can be turned off", body: "[network]\ncluster = 'devnet'\n[tuning]\nreplay_phase_timers = false\n", check: func(t *testing.T) {
		require.True(t, replay.SkipPhaseTimers)
	}},
	{name: "offline replay builds from local snapshot files", body: strings.Replace(offlineReplayConfig, "mode = 'accountsdb'", "mode = 'snapshot'", 1) + "[ledger]\nsnapshot_archive_path = '/data/snapshot-100-11111111111111111111111111111111.tar.zst'\n", check: func(t *testing.T) {
		require.True(t, offlineReplay)
		require.NotEmpty(t, snapshotArchivePath)
	}},
	{name: "offline replay refuses validator mode", body: offlineReplayConfig + "mode = 'validator'\n", wantErr: "consensus.mode=verifying"},
}

// offlineReplayConfig is a corpus replay: a configured gossip entrypoint,
// advertised IP and Votor listener that offline mode must all ignore.
const offlineReplayConfig = "[network]\ncluster = 'mainnet'\nrpc = ['http://127.0.0.1:8899']\noffline = true\n" +
	"[bootstrap]\nmode = 'accountsdb'\n[replay]\nend_slot = 100\n" +
	"[turbine]\ngossip_entrypoint = '127.0.0.1:8001'\nbind_addr = '0.0.0.0:65431'\nadvertised_ip = '192.0.2.1'\nshred_version = 50093\n" +
	"[consensus]\nalpenglow_observer_bind_addr = '0.0.0.0:65432'\n"

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
