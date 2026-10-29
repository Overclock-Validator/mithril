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

func TestManifestCommissionSurvivesDecodeAndSeed(t *testing.T) {
	for _, version := range []uint32{sealevel.VoteStateVersionV0_23_5, sealevel.VoteStateVersionV1_14_11, sealevel.VoteStateVersionCurrent, sealevel.VoteStateVersionV4} {
		for _, bps := range []uint16{0, 1200, 10000, 1234} {
			want := bps
			vote := &sealevel.VoteStateVersions{Type: version}
			if version == sealevel.VoteStateVersionV4 {
				vote.V4.InflationRewardsCommissionBps = bps
			} else {
				want = (bps / 100) * 100
				vote.V0_23_5.Commission = byte(bps / 100)
				vote.V1_14_11.Commission = byte(bps / 100)
				vote.Current.Commission = byte(bps / 100)
			}
			var wire, account bytes.Buffer
			require.NoError(t, vote.MarshalWithEncoder(bin.NewBinEncoder(&wire)))
			enc := bin.NewBinEncoder(&account)
			require.NoError(t, enc.WriteUint64(42, bin.LE))
			require.NoError(t, enc.WriteUint64(uint64(wire.Len()), bin.LE))
			require.NoError(t, enc.WriteBytes(wire.Bytes(), false))
			require.NoError(t, enc.WriteBytes(make([]byte, 32), false))
			require.NoError(t, enc.WriteByte(0))
			require.NoError(t, enc.WriteUint64(0, bin.LE))
			var decoded VoteAccount
			require.NoError(t, decoded.UnmarshalWithDecoder(bin.NewBinDecoder(account.Bytes())))
			require.NotNil(t, decoded.InflationRewardsCommissionBps)
			require.Equal(t, want, *decoded.InflationRewardsCommissionBps)
			key := solana.PublicKey{31}
			seeds := convertVersionedEpochStakesToPersisted([]VersionedEpochStakesPair{{Epoch: 1058, Val: VersionedEpochStakes{TotalStake: 42, Stakes: Stake{VoteAccounts: []VoteAccountsPair{{Key: key, Stake: 42, Value: decoded}}}}}})
			cache := epochstakes.NewEpochStakesCache()
			_, err := cache.DeserializeAndLoadEpoch([]byte(seeds[1058]))
			require.NoError(t, err)
			got, _ := cache.Snapshot(1058)
			require.Equal(t, want, *got.VoteAccounts[key].InflationRewardsCommissionBps)
		}
	}
}
