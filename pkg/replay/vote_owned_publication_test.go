package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Publication must distinguish an exact rewarded version, an account replaced
// since its reward, and ordinary execution bytes that may still alias an arena.
func TestVoteOwnedPublicationMatchesExactVersion(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		bank, stored := modifiedAccountsBank(t, 320)
		ownedKey, replacedKey := solana.PublicKey{3, 0}, solana.PublicKey{4, 0}
		ownedKey[31], replacedKey[31] = 0x5c, 0x5c
		owned, old := stored[ownedKey], stored[replacedKey]
		var versions votePublicationVersions
		if enabled {
			versions = votePublicationVersions{ownedKey: owned, replacedKey: old}
		}
		replacement := old.Clone()
		replacement.Data[0]++
		require.NoError(t, bank.SetAccount(replacedKey, replacement))
		stored[replacedKey] = replacement

		beforeAccounts := metrics.GlobalBlockReplay.ReusedVotePublicationAccounts
		beforeBytes := metrics.GlobalBlockReplay.ReusedVotePublicationBytes
		t.Cleanup(func() {
			metrics.GlobalBlockReplay.ReusedVotePublicationAccounts = beforeAccounts
			metrics.GlobalBlockReplay.ReusedVotePublicationBytes = beforeBytes
		})
		published := appendModifiedAccountCopiesWithVotes(nil, bank, versions)
		require.Len(t, published, len(stored))
		for _, acct := range published {
			source := stored[acct.Key]
			require.Equal(t, source, acct)
			require.NotSame(t, source, acct)
			if len(acct.Data) == 0 {
				continue
			}
			if enabled && acct.Key == ownedKey {
				require.Same(t, &source.Data[0], &acct.Data[0])
				require.Equal(t, len(acct.Data), cap(acct.Data))
			} else {
				require.NotSame(t, &source.Data[0], &acct.Data[0])
			}
		}
		if enabled {
			require.Equal(t, beforeAccounts+1, metrics.GlobalBlockReplay.ReusedVotePublicationAccounts)
			require.Equal(t, beforeBytes+uint64(len(owned.Data)), metrics.GlobalBlockReplay.ReusedVotePublicationBytes)
		} else {
			require.Equal(t, beforeAccounts, metrics.GlobalBlockReplay.ReusedVotePublicationAccounts)
			require.Equal(t, beforeBytes, metrics.GlobalBlockReplay.ReusedVotePublicationBytes)
		}
	}
}

func TestVoteOwnedPublicationRewardPipeline(t *testing.T) {
	fx := newVoteRewardFixture(t, 300, 34, false)
	bank, _ := fx.bank(t)
	expected, _ := fx.bank(t)
	update := fx.update(bank)
	update.versions = make(votePublicationVersions)
	got, err := runVoteAccountUpdates(bank, update)
	require.NoError(t, err)
	want, err := runVoteAccountUpdates(expected, fx.update(expected))
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NotEmpty(t, update.versions)
	accountsBefore := metrics.GlobalBlockReplay.ReusedVotePublicationAccounts
	bytesBefore := metrics.GlobalBlockReplay.ReusedVotePublicationBytes
	t.Cleanup(func() {
		metrics.GlobalBlockReplay.ReusedVotePublicationAccounts = accountsBefore
		metrics.GlobalBlockReplay.ReusedVotePublicationBytes = bytesBefore
	})

	out := appendModifiedAccountCopiesWithVotes(nil, bank, update.versions)
	for _, acct := range out {
		golden, err := expected.GetAccount(acct.Key)
		require.NoError(t, err)
		require.Equal(t, golden, acct)
		mutable, err := bank.GetAccount(acct.Key)
		require.NoError(t, err)
		if len(mutable.Data) > 0 {
			mutable.Data[0] ^= 255
		}
		require.Equal(t, golden, acct)
	}
	require.Greater(t, metrics.GlobalBlockReplay.ReusedVotePublicationBytes, bytesBefore)
}
