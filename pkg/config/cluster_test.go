package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	customGenesis  = "9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT"
	mainnetGenesis = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	testnetGenesis = "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY"
)

func TestResolveBuiltinClusters(t *testing.T) {
	for _, tc := range []struct{ in, want, genesis string }{
		{"", "mainnet", mainnetGenesis},
		{"mainnet", "mainnet", mainnetGenesis},
		{"mainnet-beta", "mainnet", mainnetGenesis},
		{" Mainnet-Beta ", "mainnet", mainnetGenesis},
		{"testnet", "testnet", testnetGenesis},
		{"devnet", "devnet", "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG"},
	} {
		got, err := ResolveCluster(tc.in, "")
		require.NoError(t, err, tc.in)
		require.Equal(t, tc.want, got.Name, tc.in)
		require.Equal(t, tc.genesis, got.GenesisHash, tc.in)
		require.False(t, got.Custom, tc.in)
		require.NotEmpty(t, got.RPC, tc.in)
		require.NotEmpty(t, got.GossipEntrypoint, tc.in)
	}

	// Naming the built-in genesis is redundant but allowed.
	got, err := ResolveCluster("testnet", testnetGenesis)
	require.NoError(t, err)
	require.Equal(t, "testnet", got.Name)

	// A built-in name can never be pointed at another chain.
	_, err = ResolveCluster("mainnet-beta", testnetGenesis)
	require.ErrorContains(t, err, "does not match mainnet genesis")
}

func TestResolveCustomCluster(t *testing.T) {
	got, err := ResolveCluster("my-private", customGenesis)
	require.NoError(t, err)
	require.Equal(t, Cluster{Name: "my-private", GenesisHash: customGenesis, Custom: true}, got)

	_, err = ResolveCluster("my-private", "")
	require.ErrorContains(t, err, "requires network.genesis_hash")

	_, err = ResolveCluster("alpenglow", "")
	require.ErrorContains(t, err, "no longer names a cluster")

	_, err = ResolveCluster("my-private", "not-a-hash")
	require.ErrorContains(t, err, "base58")

	_, err = ResolveCluster("../escape", customGenesis)
	require.ErrorContains(t, err, "invalid network.cluster")
}

func TestClusterGenesisRefusesOtherChains(t *testing.T) {
	mainnet, err := ResolveCluster("mainnet-beta", "")
	require.NoError(t, err)
	require.NoError(t, mainnet.CheckGenesis(mainnetGenesis))
	require.ErrorContains(t, mainnet.CheckGenesis(testnetGenesis), "belongs to another cluster")

	custom, err := ResolveCluster("my-private", customGenesis)
	require.NoError(t, err)
	require.NoError(t, custom.CheckGenesis(customGenesis))
	require.Error(t, custom.CheckGenesis(mainnetGenesis))
}

func TestBuiltinClustersOrder(t *testing.T) {
	var names []string
	for _, c := range BuiltinClusters() {
		names = append(names, c.Name)
	}
	require.Equal(t, []string{"mainnet", "testnet", "devnet"}, names)
}
