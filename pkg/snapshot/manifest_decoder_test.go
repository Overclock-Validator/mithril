package snapshot

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestManifestVoteAccountDecoderPreservesV4BLSPubkey(t *testing.T) {
	var node solana.PublicKey
	node[0] = 7
	var owner solana.PublicKey
	owner[0] = 9
	var bls [48]byte
	for i := range bls {
		bls[i] = byte(i + 1)
	}

	voteState := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4: sealevel.VoteState4{
			NodePubkey:            node,
			BlockRevenueCollector: solana.PublicKey{17},
			BlsPubkeyCompressed:   &bls,
			LastTimestamp: sealevel.BlockTimestamp{
				Timestamp: 123,
				Slot:      456,
			},
		},
	}

	var voteStateBuf bytes.Buffer
	require.NoError(t, voteState.MarshalWithEncoder(bin.NewBinEncoder(&voteStateBuf)))

	var accountBuf bytes.Buffer
	encoder := bin.NewBinEncoder(&accountBuf)
	require.NoError(t, encoder.WriteUint64(42, bin.LE))
	require.NoError(t, encoder.WriteUint64(uint64(voteStateBuf.Len()), bin.LE))
	require.NoError(t, encoder.WriteBytes(voteStateBuf.Bytes(), false))
	require.NoError(t, encoder.WriteBytes(owner[:], false))
	require.NoError(t, encoder.WriteByte(0))
	require.NoError(t, encoder.WriteUint64(18446744073709551615, bin.LE))

	var decoded VoteAccount
	require.NoError(t, decoded.UnmarshalWithDecoder(bin.NewBinDecoder(accountBuf.Bytes())))
	require.Equal(t, node, decoded.NodePubkey)
	require.NotNil(t, decoded.BlockRevenueCollector)
	require.Equal(t, solana.PublicKey{17}, *decoded.BlockRevenueCollector)
	require.NotNil(t, decoded.BlsPubkeyCompressed)
	require.Equal(t, bls, *decoded.BlsPubkeyCompressed)
	require.Equal(t, int64(123), decoded.LastTimestampTs)
	require.Equal(t, uint64(456), decoded.LastTimestampSlot)
}

func TestManifestSeedPreservesFeeCollectors(t *testing.T) {
	vote, node, collector := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	for _, destination := range []solana.PublicKey{node, collector, {}} {
		decoded := VoteAccount{NodePubkey: node, BlockRevenueCollector: &destination}
		seeds := convertVersionedEpochStakesToPersisted([]VersionedEpochStakesPair{{Epoch: 1054, Val: VersionedEpochStakes{TotalStake: 1, Stakes: Stake{VoteAccounts: []VoteAccountsPair{{Key: vote, Stake: 1, Value: decoded}}}}}})
		cache := epochstakes.NewEpochStakesCache()
		_, err := cache.DeserializeAndLoadEpoch([]byte(seeds[1054]))
		require.NoError(t, err)
		got, ok := cache.Snapshot(1054)
		require.True(t, ok)
		require.NotNil(t, got.VoteAccounts[vote].BlockRevenueCollector)
		require.Equal(t, destination, *got.VoteAccounts[vote].BlockRevenueCollector)
	}
}
