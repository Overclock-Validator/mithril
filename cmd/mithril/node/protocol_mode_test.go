package node

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestAlpenglowModeForCluster(t *testing.T) {
	alpenglow, err := alpenglowModeForCluster("alpenglow")
	require.NoError(t, err)
	require.True(t, alpenglow)

	for _, cluster := range []string{"mainnet-beta", "testnet", "devnet"} {
		classic, err := alpenglowModeForCluster(cluster)
		require.NoError(t, err)
		require.False(t, classic, cluster)
	}

	_, err = alpenglowModeForCluster("localnet")
	require.Error(t, err)
}

func TestV2RuntimeClusterPreflightStopsClassicBeforeRun(t *testing.T) {
	for _, cluster := range []string{"mainnet-beta", "testnet", "devnet"} {
		t.Run(cluster, func(t *testing.T) {
			runCalled := false
			cmd := cobra.Command{
				PreRunE: func(*cobra.Command, []string) error {
					return validateV2RuntimeCluster(cluster)
				},
				Run: func(*cobra.Command, []string) {
					runCalled = true
				},
			}
			cmd.SetArgs(nil)
			err := cmd.Execute()
			require.ErrorContains(t, err, "currently supports only network.cluster=alpenglow")
			require.False(t, runCalled, "classic preflight must stop runLive and all of its side effects")
		})
	}

	require.NoError(t, validateV2RuntimeCluster("alpenglow"))
}

func TestResolveTurbineShredVersion(t *testing.T) {
	called := false
	query := func(addr *net.UDPAddr, timeout time.Duration) (gossip.EchoResponse, error) {
		called = true
		require.Equal(t, "127.0.0.1:9000", addr.String())
		require.Equal(t, 5*time.Second, timeout)
		return gossip.EchoResponse{ShredVersion: 10638}, nil
	}

	got, err := resolveTurbineShredVersion(0, "127.0.0.1:9000", query)
	require.NoError(t, err)
	require.Equal(t, 10638, got)
	require.True(t, called)

	called = false
	got, err = resolveTurbineShredVersion(4242, "", query)
	require.NoError(t, err)
	require.Equal(t, 4242, got)
	require.False(t, called, "an explicitly configured version must not query gossip")
}

func TestResolveTurbineShredVersionReportsDiscoveryErrors(t *testing.T) {
	_, err := resolveTurbineShredVersion(0, "", nil)
	require.ErrorContains(t, err, "entrypoint is required")

	want := errors.New("echo unavailable")
	_, err = resolveTurbineShredVersion(0, "127.0.0.1:9000", func(*net.UDPAddr, time.Duration) (gossip.EchoResponse, error) {
		return gossip.EchoResponse{}, want
	})
	require.ErrorIs(t, err, want)
}

func TestDefaultBlockSourceForProtocolMode(t *testing.T) {
	require.Equal(t, "turbine", defaultBlockSourceForMode(true))
	require.Equal(t, "rpc", defaultBlockSourceForMode(false))
}

func TestValidatorModeUsesLocalFooterBankHashInsteadOfRPCVerifier(t *testing.T) {
	cfg := replay.TrailingVerifierDefaults()
	got := verifierConfigForConsensusMode("validator", cfg)
	require.False(t, got.Enabled)
	require.False(t, got.Required)
	require.True(t, got.ValidatorFooterHash)

	require.Equal(t, cfg, verifierConfigForConsensusMode("verifying", cfg), "verifying mode must remain unchanged")
}

func TestTxParallelismForMode(t *testing.T) {
	require.Equal(t, int64(32), txParallelismForMode("validator", 0, false, 16))
	require.Equal(t, int64(0), txParallelismForMode("validator", 0, true, 16), "explicit sequential validator mode must be preserved")
	require.Equal(t, int64(7), txParallelismForMode("validator", 7, true, 16))
	require.Equal(t, int64(0), txParallelismForMode("verifying", 0, false, 16), "non-Alpenglow verifying flow must remain unchanged")
	require.Equal(t, int64(2), txParallelismForMode("validator", 0, false, 0), "invalid CPU discovery gets a safe minimum")
}

func TestResolveInitialAlpenglowBlockID(t *testing.T) {
	snapshotID := solana.Hash{1, 2, 3}
	mithrilState := &state.MithrilState{
		ManifestParentSlot:             100,
		ManifestParentAlpenglowBlockID: snapshotID.String(),
	}

	got, err := resolveInitialAlpenglowBlockID(mithrilState, nil)
	require.NoError(t, err)
	require.Equal(t, snapshotID, got)

	checkpointID := solana.Hash{9, 8, 7}
	got, err = resolveInitialAlpenglowBlockID(mithrilState, &replay.ResumeState{
		ParentSlot:                120,
		ParentAlpenglowBlockID:    checkpointID,
		HasParentAlpenglowBlockID: true,
	})
	require.NoError(t, err)
	require.Equal(t, checkpointID, got, "a rooted checkpoint supersedes the snapshot anchor")

	for name, stateValue := range map[string]*state.MithrilState{
		"missing state": nil,
		"missing id":    {ManifestParentSlot: 100},
		"malformed id":  {ManifestParentSlot: 100, ManifestParentAlpenglowBlockID: "not-base58!"},
		"zero id":       {ManifestParentSlot: 100, ManifestParentAlpenglowBlockID: (solana.Hash{}).String()},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := resolveInitialAlpenglowBlockID(stateValue, nil)
			require.Error(t, err)
		})
	}

	_, err = resolveInitialAlpenglowBlockID(mithrilState, &replay.ResumeState{ParentSlot: 120})
	require.ErrorContains(t, err, "has no Alpenglow block ID")
}
