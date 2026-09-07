package replay

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type rewardAccountReader struct {
	acct *accounts.Account
}

func (r rewardAccountReader) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if r.acct != nil && r.acct.Key == pubkey {
		return r.acct.Clone(), nil
	}
	return nil, accountsdb.ErrNoAccount
}

func TestRewardEpochDelegatedStakesAddress(t *testing.T) {
	require.Equal(
		t,
		"CHfmHwNcskfxg1ZnLatbHbmVvYbA6rLDz3Xok5a3Ku8i",
		RewardEpochDelegatedStakesAccountAddr().String(),
	)
}

func TestEncodeRewardEpochDelegatedStakesSorted(t *testing.T) {
	low := solana.PublicKey{31: 1}
	high := solana.PublicKey{0: 1}
	data, err := encodeRewardEpochDelegatedStakes(
		70,
		map[solana.PublicKey]uint64{high: 1, low: 1},
		map[solana.PublicKey]uint64{high: 20, low: 10},
	)
	require.NoError(t, err)
	require.Len(t, data, 16+2*rewardEpochDelegatedStakeRecordLen)
	require.Equal(t, uint64(70), binary.LittleEndian.Uint64(data[:8]))
	require.Equal(t, uint64(2), binary.LittleEndian.Uint64(data[8:16]))
	require.Equal(t, low[:], data[16:48])
	require.Equal(t, uint64(10), binary.LittleEndian.Uint64(data[48:56]))
	require.Equal(t, high[:], data[56:88])
	require.Equal(t, uint64(20), binary.LittleEndian.Uint64(data[88:96]))
	require.Equal(t, uint64(80_016), rewardEpochDelegatedStakesMaxDataLen())
}

func TestCoalesceEpochAccountUpdatesKeepsOriginalParentAndFinalValue(t *testing.T) {
	key := solana.NewWallet().PublicKey()
	other := solana.NewWallet().PublicKey()
	updated, parents := coalesceEpochAccountUpdates(
		[]*accounts.Account{
			{Key: key, Lamports: 90},
			{Key: other, Lamports: 5},
			{Key: key, Lamports: 120},
		},
		[]*accounts.Account{
			{Key: key, Lamports: 100},
			{Key: other, Lamports: 4},
			{Key: key, Lamports: 90},
		},
	)
	require.Len(t, updated, 2)
	require.Len(t, parents, 2)
	require.Equal(t, key, updated[0].Key)
	require.Equal(t, uint64(120), updated[0].Lamports)
	require.Equal(t, uint64(100), parents[0].Lamports)
	require.Equal(t, other, updated[1].Key)
}

func TestEpochRewardAccountLoaderPrefersStagedThenSpeculativeParent(t *testing.T) {
	stagedKey := solana.NewWallet().PublicKey()
	parentKey := solana.NewWallet().PublicKey()
	staged := &accounts.Account{Key: stagedKey, Lamports: 90}
	parent := &accounts.Account{Key: parentKey, Lamports: 70}
	parentCtx := &sealevel.SlotCtx{
		Slot:         41,
		UnrootedRead: rewardAccountReader{acct: parent},
	}

	loader := epochRewardAccountLoader(nil, 42, parentCtx, []*accounts.Account{
		{Key: stagedKey, Lamports: 95},
		staged,
	})

	loadedStaged, err := loader(stagedKey)
	require.NoError(t, err)
	require.Equal(t, staged.Key, loadedStaged.Key)
	require.Equal(t, staged.Lamports, loadedStaged.Lamports, "latest same-bank staged write must win")
	loadedStaged.Lamports++
	require.Equal(t, uint64(90), staged.Lamports, "loader must not expose retained staged state")

	loadedParent, err := loader(parentKey)
	require.NoError(t, err)
	require.Equal(t, parent.Key, loadedParent.Key)
	require.Equal(t, parent.Lamports, loadedParent.Lamports, "next reward partition must read the speculative parent bank")
}
