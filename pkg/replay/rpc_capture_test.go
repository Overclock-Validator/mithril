package replay

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/rpcindex"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func tokenAccount(key, mint, owner solana.PublicKey, program solana.PublicKey) *accounts.Account {
	data := make([]byte, 165)
	copy(data[0:32], mint[:])
	copy(data[32:64], owner[:])
	data[108] = 1
	return &accounts.Account{Key: key, Lamports: 2_039_280, Owner: [32]byte(program), Data: data}
}

func TestDiscoverTokenAccounts(t *testing.T) {
	reg := watch.NewRegistry()
	wallet, stranger, mint := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	closing := solana.PublicKey{4}
	reg.AddKeys(watch.Key{Key: wallet, Kind: watch.Wallet}, watch.Key{Key: closing, Kind: watch.TokenAccount})
	rpcWatch.Store(reg)
	defer rpcWatch.Store(nil)
	rpcCaptures.reset()

	slotCtx := &sealevel.SlotCtx{Slot: 50}
	mine := tokenAccount(solana.PublicKey{10}, mint, wallet, rpcindex.Token2022Program)
	theirs := tokenAccount(solana.PublicKey{11}, mint, stranger, rpcindex.SPLTokenProgram)
	plain := &accounts.Account{Key: solana.PublicKey{12}, Lamports: 5, Owner: [32]byte(solana.SystemProgramID)}
	closed := &accounts.Account{Key: closing} // zero lamports: closed
	discoverTokenAccounts(slotCtx, []*accounts.Account{mine, theirs, plain, closed})

	snap := reg.Current()
	require.True(t, snap.Match.HasKey(&mine.Key), "discovered account joins the watch set")
	require.False(t, snap.Match.HasKey(&theirs.Key))
	bc := rpcCaptures.take(slotCtx)
	require.NotNil(t, bc)
	require.Equal(t, []rpcindex.TokenAccountChange{
		{Owner: wallet, Mint: mint, Account: mine.Key, Program: rpcindex.Token2022Program},
		{Account: closing, Closed: true},
	}, bc.tokens)
}

func TestDiscoverTokenAccountsOffWithoutHistory(t *testing.T) {
	rpcWatch.Store(nil)
	rpcCaptures.reset()
	slotCtx := &sealevel.SlotCtx{Slot: 51}
	discoverTokenAccounts(slotCtx, []*accounts.Account{tokenAccount(solana.PublicKey{10}, solana.PublicKey{3}, solana.PublicKey{1}, rpcindex.SPLTokenProgram)})
	require.Nil(t, rpcCaptures.take(slotCtx))
}

func TestProcessTransactionCapturesWatchedTransfer(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	wire, err := txfixture.SignedTransferWire(7)
	require.NoError(t, err)
	tx, err := solana.TransactionFromDecoder(bin.NewBinDecoder(wire))
	require.NoError(t, err)

	run := func() *bankCaptures {
		rpcCaptures.reset()
		var sigverify sync.WaitGroup
		_, _, err := ProcessTransaction(slotCtx, &sigverify, tx, nil, nil, nil, false)
		sigverify.Wait()
		require.NoError(t, err)
		return rpcCaptures.take(slotCtx)
	}

	// No registry: nothing is captured.
	rpcWatch.Store(nil)
	require.Nil(t, run())

	// Watching an unrelated wallet: nothing is captured.
	reg := watch.NewRegistry()
	reg.AddKeys(watch.Key{Key: solana.PublicKey{9}, Kind: watch.Wallet})
	rpcWatch.Store(reg)
	defer rpcWatch.Store(nil)
	require.Nil(t, run())

	// Watching the recipient captures the transfer with balances and logs.
	reg.AddKeys(watch.Key{Key: txfixture.DestPubkey(), Kind: watch.Wallet})
	payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	bc := run()
	require.NotNil(t, bc)
	require.Len(t, bc.captured, 1)
	c := bc.captured[0]
	require.Equal(t, tx.Signatures[0], c.Signature)
	require.Nil(t, c.Err)
	require.Nil(t, c.ErrJSON)
	require.EqualValues(t, 5000, c.Fee)
	require.Equal(t, payerBefore.Lamports, c.PreBalances[0])
	payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	require.Equal(t, payerAfter.Lamports, c.PostBalances[0])
	require.Less(t, c.PostBalances[0], c.PreBalances[0])
	require.Greater(t, c.PostBalances[1], c.PreBalances[1], "the recipient was credited")
	// Agave's stable log lines for a system transfer.
	require.Equal(t, []string{
		"Program 11111111111111111111111111111111 invoke [1]",
		"Program 11111111111111111111111111111111 success",
	}, c.Logs)
}
