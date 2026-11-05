package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRegisterTakeLocalLeaderCommit(t *testing.T) {
	t.Cleanup(ResetLocalLeaderCommits)
	slotCtx := &sealevel.SlotCtx{Slot: 7, FinalBankhash: []byte{1, 2, 3}}
	RegisterLocalLeaderCommit(slotCtx)

	got, ok := TakeLocalLeaderCommit(7)
	require.True(t, ok)
	require.Same(t, slotCtx, got.SlotCtx)

	_, ok = TakeLocalLeaderCommit(7)
	require.False(t, ok)
}

func TestResetChainTipDropsLocalLeaderCommits(t *testing.T) {
	t.Cleanup(ResetChainTip)
	RegisterLocalLeaderCommit(&sealevel.SlotCtx{Slot: 9})
	ResetChainTip()
	_, ok := TakeLocalLeaderCommit(9)
	require.False(t, ok)
}

func TestAdoptLocalLeaderBlockUsesProducerSlotCtx(t *testing.T) {
	t.Cleanup(ResetLocalLeaderCommits)

	key := solana.PublicKey{0x11}
	acct := &accounts.Account{Key: key, Lamports: 42, Data: []byte{9}}
	slotCtx := &sealevel.SlotCtx{
		Slot:              20,
		TransactionCounts: sealevel.BankTransactionCounts{Total: 1, NonVote: 1, ComputeUnits: 150},
		Accounts:          accounts.NewMemAccounts(),
		ModifiedAccts:     map[solana.PublicKey]bool{key: true},
		FinalBankhash:     append([]byte{8}, make([]byte, 31)...),
	}
	require.NoError(t, slotCtx.SetAccount(key, acct))
	bankSysvars, err := sealevel.NewBankSysvars(20, &accounts.Account{
		Key:      sealevel.SysvarClockAddr,
		Lamports: 1,
		Data:     (&sealevel.SysvarClock{Slot: 20}).MustMarshal(),
	})
	require.NoError(t, err)
	require.NoError(t, slotCtx.PublishBankSysvars(bankSysvars))
	RegisterLocalLeaderCommit(slotCtx)

	block := &b.Block{Slot: 20, ParentSlot: 19, FromLocalProduction: true}
	subject := solana.PublicKey{7}
	block.EnableCertificateObservation(subject)
	observation := b.CertificateObservation{VoteAccount: subject, Reward: b.CertificateMembership{Included: true}}
	block.RecordCertificateObservation(observation)
	persisted := &persistedTracker{}
	var tail *unrootedTail // classic replay passes a typed nil through the interface
	got, err := adoptLocalLeaderBlock(block, tail, NewTransactionStatusCache(), persisted)
	require.NoError(t, err)
	require.Same(t, slotCtx, got)
	require.Equal(t, uint64(150), got.TransactionCounts.ComputeUnits, "adoption retains actual producer execution work")
	require.Zero(t, got.TotalComputeUnitsConsumed, "the older replay-only field is not populated by production")
	captured, ok := block.CertificateObservation()
	require.True(t, ok)
	require.Equal(t, observation, captured, "adoption must not clear the producer's compact result")

	slot, hash := persisted.Get()
	require.Equal(t, uint64(20), slot)
	require.Equal(t, slotCtx.FinalBankhash, hash)

	_, ok = TakeLocalLeaderCommit(20)
	require.False(t, ok, "adopt must consume the registered SlotCtx")
}

func TestAdoptLocalLeaderBlockFailsClosedWithoutCommit(t *testing.T) {
	t.Cleanup(ResetLocalLeaderCommits)
	_, err := adoptLocalLeaderBlock(&b.Block{Slot: 3, FromLocalProduction: true}, nil, NewTransactionStatusCache(), nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing producer SlotCtx")
}

func TestAdoptLocalLeaderBlockFailsClosedWithoutBankSysvars(t *testing.T) {
	t.Cleanup(ResetLocalLeaderCommits)
	RegisterLocalLeaderCommit(&sealevel.SlotCtx{Slot: 4})
	_, err := adoptLocalLeaderBlock(&b.Block{Slot: 4, FromLocalProduction: true}, nil, NewTransactionStatusCache(), nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing bank sysvars")
}
