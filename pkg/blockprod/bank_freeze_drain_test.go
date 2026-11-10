package blockprod

import (
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type blockedExecutionAccounts struct {
	accounts.Accounts
	once            sync.Once
	entered, resume chan struct{}
}

func (a *blockedExecutionAccounts) GetAccount(key *[32]byte) (*accounts.Account, error) {
	if *key == [32]byte(txfixture.PayerPubkey()) {
		a.once.Do(func() { close(a.entered); <-a.resume })
	}
	return a.Accounts.GetAccount(key)
}

func TestFreezeQuiescesActualScratchExecution(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	first := mustSignedTransfer(t, 1)
	require.Equal(t, ForgeAccepted, forgeParallel(t, env.Bank, false, first).Result)
	payer, dest := lamportsOf(t, env, txfixture.PayerPubkey()), lamportsOf(t, env, txfixture.DestPubkey())
	prepared, result := env.Bank.PrepareBundle([]*solana.Transaction{mustSignedTransfer(t, 2), mustSignedTransfer(t, 3)}, true)
	require.Equal(t, ForgeAccepted, result.Result)
	blocked := &blockedExecutionAccounts{Accounts: env.SlotCtx.Accounts, entered: make(chan struct{}), resume: make(chan struct{})}
	env.SlotCtx.Accounts = blocked
	var once sync.Once
	unblock := func() { once.Do(func() { close(blocked.resume) }) }
	defer unblock()
	executed := make(chan *ExecutedBundle, 1)
	go func() { executed <- env.Bank.ExecuteBundle(prepared) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("scratch execution did not reach the blocked account read")
	}
	frozen := make(chan struct{})
	go func() { env.Bank.Freeze(); close(frozen) }()
	require.Eventually(t, func() bool {
		env.Bank.mu.Lock()
		defer env.Bank.mu.Unlock()
		return !env.Bank.accepting
	}, time.Second, time.Millisecond)
	select {
	case <-frozen:
		t.Fatal("Freeze returned while scratch execution still reads bank state")
	default:
	}
	// No new execution can extend the wait after admission has closed.
	rejected := env.Bank.ExecuteBundle(prepared)
	require.Equal(t, ForgeDroppedNoLeader, env.Bank.CommitBundle(rejected).Result)
	unblock()
	var scratch *ExecutedBundle
	select {
	case scratch = <-executed:
	case <-time.After(time.Second):
		t.Fatal("scratch execution did not finish")
	}
	select {
	case <-frozen:
	case <-time.After(time.Second):
		t.Fatal("Freeze did not finish after execution quiesced")
	}
	// These are representative fields bank finalization updates after Freeze.
	// With -race, this also verifies that no execution still reads the context.
	env.SlotCtx.NumSignatures++
	env.SlotCtx.Blockhash = solana.Hash{9}
	env.SlotCtx.Epoch++
	require.Equal(t, ForgeDroppedNoLeader, env.Bank.CommitBundle(scratch).Result)
	require.Equal(t, []*solana.Transaction{first}, env.Bank.ForgedTransactions())
	require.Equal(t, payer, lamportsOf(t, env, txfixture.PayerPubkey()))
	require.Equal(t, dest, lamportsOf(t, env, txfixture.DestPubkey()))
}
