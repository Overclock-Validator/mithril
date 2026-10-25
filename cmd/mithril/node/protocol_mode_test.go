package node

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, int64(0), txParallelismForMode("verifying", 0, false, 16), "verifying flow must remain unchanged")
	require.Equal(t, int64(2), txParallelismForMode("validator", 0, false, 0), "invalid CPU discovery gets a safe minimum")
}
