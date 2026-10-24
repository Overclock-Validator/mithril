package node

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestResolveProtocolMode(t *testing.T) {
	for _, cluster := range []string{"mainnet-beta", "testnet", "devnet", "alpenglow"} {
		t.Run(cluster, func(t *testing.T) {
			got, err := resolveProtocolMode(cluster, "")
			require.NoError(t, err)
			require.Equal(t, cluster == "alpenglow", got)
			got, err = resolveProtocolMode(cluster, "alpenglow")
			require.NoError(t, err)
			require.True(t, got)
			got, err = resolveProtocolMode(cluster, "classic")
			require.NoError(t, err)
			require.False(t, got)
		})
	}
	_, err := resolveProtocolMode("mainnet-beta", "invalid")
	require.ErrorContains(t, err, "consensus.protocol")
	_, err = resolveProtocolMode("localnet", "alpenglow")
	require.ErrorContains(t, err, "network.cluster")
}
