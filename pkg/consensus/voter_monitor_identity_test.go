package consensus

import (
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestMonitoringLandedVoteDoesNotCrossIdentityRebind(t *testing.T) {
	engine, voter := newStoppedFloorTestVoter(t, alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}})
	engine.monitoringEnabled = true
	block := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	require.NoError(t, voter.history.AddVote(alpenglow.NewNotarizationVote(block.Slot, block.Hash)))
	require.NoError(t, voter.saveHistory())
	bitmap, err := alpenglow.EncodeSignerStoreBitmap(alpenglow.SignerBitmap{Encoding: alpenglow.SignerBitmapBase2, Length: 3, Base: []bool{true, false, false}})
	require.NoError(t, err)
	voter.recordNetworkCertificate(alpenglow.Certificate{Type: alpenglow.CertificateNotarize, Slot: block.Slot, BlockHash: block.Hash, Bitmap: bitmap})
	voter.publishMonitoring(voter.snapshot())
	before := *voter.monitoring.Load()
	require.Equal(t, uint64(40), before.LastNetworkLandedSlot)
	require.False(t, before.LastNetworkLandedAt.IsZero())
	require.NoError(t, voter.rebindIdentity(voter.identity))
	voter.publishMonitoring(voter.snapshot())
	require.Equal(t, uint64(40), voter.monitoring.Load().LastNetworkLandedSlot)
	next, err := identity.NewKeypair(voterTestKey(84))
	require.NoError(t, err)
	require.NoError(t, voter.rebindIdentity(next))
	after := voter.monitoring.Load()
	require.Equal(t, next.PublicKey(), after.Identity)
	require.True(t, after.LastNetworkLandedAt.IsZero())
	require.Zero(t, after.LastNetworkLandedSlot)
	require.Equal(t, before.NetworkLanded, after.NetworkLanded)
	require.False(t, before.LastNetworkLandedAt.IsZero())
	voter.publishMonitoring(voter.snapshot())
	require.True(t, voter.monitoring.Load().LastNetworkLandedAt.IsZero())
	old, err := identity.NewKeypair(voterTestKey(81))
	require.NoError(t, err)
	require.NoError(t, voter.rebindIdentity(old))
	require.True(t, voter.monitoring.Load().LastNetworkLandedAt.IsZero())
	require.NoError(t, voter.rebindIdentity(next))

	set, ok := voter.validatorSet(41)
	require.True(t, ok)
	for i := range set.Validators {
		if set.Validators[i].NodePubkey == before.Identity {
			set.Validators[i].NodePubkey = next.PublicKey()
		}
	}
	voter.sets[set.Epoch] = set
	block = alpenglow.BlockID{Slot: 41, Hash: solana.Hash{0x41}}
	require.NoError(t, voter.history.AddVote(alpenglow.NewNotarizationVote(block.Slot, block.Hash)))
	voter.recordNetworkCertificate(alpenglow.Certificate{Type: alpenglow.CertificateNotarize, Slot: block.Slot, BlockHash: block.Hash, Bitmap: bitmap})
	voter.publishMonitoring(voter.snapshot())
	require.Equal(t, uint64(41), voter.monitoring.Load().LastNetworkLandedSlot)
	require.False(t, voter.monitoring.Load().LastNetworkLandedAt.IsZero())
	bad, err := identity.NewKeypair(voterTestKey(85))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(alpenglow.VoteHistoryFilename(voter.historyDir, bad.PublicKey()), []byte("invalid"), 0o600))
	require.Error(t, voter.rebindIdentity(bad))
	voter.publishMonitoring(voter.snapshot())
	require.Equal(t, next.PublicKey(), voter.monitoring.Load().Identity)
	require.Equal(t, uint64(41), voter.monitoring.Load().LastNetworkLandedSlot)
}
