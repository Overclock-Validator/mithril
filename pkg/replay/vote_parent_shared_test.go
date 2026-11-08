package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"testing"
)

type readonlyVoteParentSource struct{ *countingAccountSource }

func (src *readonlyVoteParentSource) GetAccountShared(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
	src.mu.Lock()
	defer src.mu.Unlock()
	src.reads++
	if acct, ok := src.accts[key]; ok {
		return acct, nil
	}
	return nil, accountsdb.ErrNoAccount
}

func TestVoteParentSharedRewardParityAndIsolation(t *testing.T) {
	old := voteParentSharedEnabled
	defer func() { voteParentSharedEnabled = old }()
	for _, n := range []int{20, 700} {
		fx := newVoteRewardFixture(t, n, int64(n+551), true)
		wantCtx, _ := fx.bank(t)
		voteParentSharedEnabled = false
		want, err := runVoteAccountUpdates(wantCtx, fx.update(wantCtx))
		require.NoError(t, err)
		gotCtx, source := fx.bank(t)
		gotCtx.UnrootedRead = &readonlyVoteParentSource{source}
		voteParentSharedEnabled = true
		before := map[solana.PublicKey]*accounts.Account{}
		for k, v := range fx.parents {
			before[k] = v.Clone()
		}
		got, err := runVoteAccountUpdates(gotCtx, fx.update(gotCtx))
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, wantCtx.ModifiedAccts, gotCtx.ModifiedAccts)
		wantModified, gotModified := modifiedAccountsOf(t, wantCtx), modifiedAccountsOf(t, gotCtx)
		require.NoError(t, ensureParentAccountsForModified(wantCtx, wantModified))
		require.NoError(t, ensureParentAccountsForModified(gotCtx, gotModified))
		wantHash := bankhash.CalculateBankHash(wantCtx, nil, wantModified, [32]byte{1}, 3, [32]byte{2})
		gotHash := bankhash.CalculateBankHash(gotCtx, nil, gotModified, [32]byte{1}, 3, [32]byte{2})
		require.Equal(t, wantHash, gotHash)

		for k := range gotCtx.ModifiedAccts {
			a, err := gotCtx.GetAccount(k)
			require.NoError(t, err)
			b, err := wantCtx.GetAccount(k)
			require.NoError(t, err)
			requireSameAccount(t, b, a, k)
			parent, err := gotCtx.GetParentAccount(k)
			require.NoError(t, err)
			requireSameAccount(t, before[k], parent, k)
			// Mutating the rewarded live value must never affect parent/source data.
			live, err := gotCtx.GetAccountShared(k)
			require.NoError(t, err)
			if len(live.Data) > 0 {
				live.Data[0] ^= 0xff
			}
			requireSameAccount(t, before[k], parent, k)
			requireSameAccount(t, before[k], fx.parents[k], k)
		}
		for k, v := range before {
			requireSameAccount(t, v, fx.parents[k], k)
		}
	}
}

func TestVoteParentSharedPrivateHeaderAndFallback(t *testing.T) {
	old := voteParentSharedEnabled
	voteParentSharedEnabled = true
	defer func() { voteParentSharedEnabled = old }()
	fx := newVoteRewardFixture(t, 20, 9951, false)
	ctx, src := fx.bank(t)
	ctx.UnrootedRead = &readonlyVoteParentSource{src}
	key := fx.accounts[0]
	original := fx.parents[key].Clone()
	mutable, parent, err := loadAccountLiveOrParentForReplay(ctx, key)
	require.NoError(t, err)
	require.NotSame(t, fx.parents[key], parent)
	parent.Lamports++
	require.Equal(t, original.Lamports, fx.parents[key].Lamports)
	mutable.Data[0] ^= 0xff
	require.Equal(t, original.Data, fx.parents[key].Data)
	require.Equal(t, original.Data, parent.Data)
	// Replacing the source version does not alter a retained parent image.
	src.accts[key] = original.Clone()
	src.accts[key].Data[0] ^= 0x33
	require.Equal(t, original.Data, parent.Data)
	// A source without shared-read capability keeps the old owning path.
	ctx.UnrootedRead = src
	_, p, err := loadAccountLiveOrParentForReplay(ctx, key)
	require.NoError(t, err)
	require.NotSame(t, src.accts[key], p)
	_, _, err = loadAccountLiveOrParentForReplay(ctx, solana.PublicKey{0xfe})
	require.Error(t, err)
}
