package replay

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestIsLiveVoteAccount(t *testing.T) {
	require.False(t, isLiveVoteAccount(nil))
	require.True(t, isLiveVoteAccount(&accounts.Account{Lamports: 1, Owner: a.VoteProgramAddr}))
	require.False(t, isLiveVoteAccount(&accounts.Account{Lamports: 0, Owner: a.VoteProgramAddr}), "closed")
	require.False(t, isLiveVoteAccount(&accounts.Account{Lamports: 1, Owner: a.SystemProgramAddr}), "recreated under another owner")
}

// A vote account can be closed while stake stays delegated to it, and its
// address recreated under another program with arbitrary data. Rewards must
// skip that account, as Agave's vote-account cache never holds it, and must
// not decode its data: a crafted lockout count used to hang the decoder.
func TestVoteRewardsSkipAccountsNotOwnedByTheVoteProgram(t *testing.T) {
	fx := newVoteRewardFixture(t, 20, 20, false)
	mimic, crafted := fx.accounts[4], fx.accounts[6]
	require.NotEqual(t, fx.leader, mimic)
	require.NotEqual(t, fx.leader, crafted)
	_, mimicRewarded := fx.reward[mimic]
	_, craftedRewarded := fx.reward[crafted]
	require.True(t, mimicRewarded && craftedRewarded)

	// A valid vote state under another owner.
	fx.parents[mimic].Owner = a.SystemProgramAddr
	// A lockout count that used to loop forever in deque.SetBaseCap.
	fx.parents[crafted].Owner = a.SystemProgramAddr
	setFirstLockoutCount(t, fx.parents[crafted].Data, 1<<62+1)
	before := map[solana.PublicKey][]byte{
		mimic:   bytes.Clone(fx.parents[mimic].Data),
		crafted: bytes.Clone(fx.parents[crafted].Data),
	}

	slotCtx, _ := fx.bank(t)
	done := make(chan error, 1)
	go func() {
		_, err := runVoteAccountUpdates(slotCtx, fx.update(slotCtx))
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("vote rewards did not return")
	}

	for key, data := range before {
		require.False(t, slotCtx.ModifiedAccts[key], "%s must not be rewarded", key)
		acct, err := slotCtx.GetAccount(key)
		if err == nil && acct != nil {
			require.Equal(t, data, acct.Data)
		}
	}
	rewarded := 0
	for _, key := range fx.accounts {
		if slotCtx.ModifiedAccts[key] {
			rewarded++
		}
	}
	require.Equal(t, len(fx.accounts)-2, rewarded, "every other account is still rewarded")
}

// setFirstLockoutCount overwrites the lockout count of a V4 vote state built
// by testVoteAccount. V4 lays out version, node pubkey, withdrawer, collector,
// commissions, BLS option and then the votes; find the count by decoding.
func setFirstLockoutCount(t *testing.T, data []byte, count uint64) {
	t.Helper()
	// version(4) node(32) withdrawer(32) inflation collector(32)
	// block revenue collector(32) inflation bps(2) block bps(2) pending(8)
	off := 4 + 32 + 32 + 32 + 32 + 2 + 2 + 8
	if data[off] == 1 { // BLS public key present
		off += 1 + 48
	} else {
		off++
	}
	require.LessOrEqual(t, binary.LittleEndian.Uint64(data[off:]), uint64(31), "located the lockout count")
	binary.LittleEndian.PutUint64(data[off:], count)
}
