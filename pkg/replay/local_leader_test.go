package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
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
		Slot:          20,
		ParentSlot:    19,
		Accounts:      accounts.NewMemAccounts(),
		ModifiedAccts: map[solana.PublicKey]bool{key: true},
		FinalBankhash: append([]byte{8}, make([]byte, 31)...),
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
	persisted := &persistedTracker{}
	var tail *unrootedTail // classic replay passes a typed nil through the interface
	got, err := adoptLocalLeaderBlock(block, tail, NewTransactionStatusCache(), persisted)
	require.NoError(t, err)
	require.Same(t, slotCtx, got)

	slot, hash := persisted.Get()
	require.Equal(t, uint64(20), slot)
	require.Equal(t, slotCtx.FinalBankhash, hash)

	_, ok := TakeLocalLeaderCommit(20)
	require.False(t, ok, "adopt must consume the registered SlotCtx")
}

func TestAdoptLocalLeaderBlockFailsClosedWithoutCommit(t *testing.T) {
	t.Cleanup(ResetLocalLeaderCommits)
	_, err := adoptLocalLeaderBlock(&b.Block{Slot: 3, FromLocalProduction: true}, nil, NewTransactionStatusCache(), nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing producer SlotCtx")
	require.ErrorIs(t, err, ErrLocalLeaderCommitMissing)
}

func TestAdoptLocalLeaderBlockFailsClosedWithoutBankSysvars(t *testing.T) {
	t.Cleanup(ResetLocalLeaderCommits)
	RegisterLocalLeaderCommit(&sealevel.SlotCtx{Slot: 4})
	_, err := adoptLocalLeaderBlock(&b.Block{Slot: 4, FromLocalProduction: true}, nil, NewTransactionStatusCache(), nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing bank sysvars")
}

func TestAdoptLocalLeaderBlockRejectsStaleBankBeforePublication(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*b.Block)
	}{
		{"parent slot", func(block *b.Block) { block.ParentSlot-- }},
		{"source parent slot", func(block *b.Block) { block.SourceParentSlot = 18 }},
		{"same slot sibling parent", func(block *b.Block) { block.ParentBankhash[0]++ }},
		{"block identity", func(block *b.Block) { block.AlpenglowBlockID[0]++ }},
		{"parent identity", func(block *b.Block) { block.AlpenglowParentBlockID[0]++ }},
		{"footer hash", func(block *b.Block) { block.ExpectedBankhash[0]++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(ResetLocalLeaderCommits)
			resetVoteStakeDirty()
			t.Cleanup(resetVoteStakeDirty)
			key := solana.PublicKey{0xEF, 11}
			before := global.VoteCacheItem(key)
			bank := &sealevel.SlotCtx{Slot: 20, ParentSlot: 19, Accounts: accounts.NewMemAccounts(),
				ModifiedAccts: map[solana.PublicKey]bool{}, FinalBankhash: append([]byte{8}, make([]byte, 31)...),
				DeferVoteCachePublication: true}
			require.NoError(t, bank.PublishBankSysvars(testUnwindBankSysvars(t, 20, 0)))
			putVoteCacheItem(bank, key, &sealevel.VoteStateVersions{})
			produced := &b.Block{Slot: 20, ParentSlot: 19, ParentBankhash: [32]byte{1},
				HasAlpenglowBlockID: true, AlpenglowBlockID: [32]byte{2},
				HasAlpenglowParentBlockID: true, AlpenglowParentBlockID: [32]byte{3},
				HasExpectedBankhash: true, ExpectedBankhash: [32]byte{8}, FromLocalProduction: true}
			RegisterLocalLeaderBlockCommit(bank, produced)
			test.change(produced) // replay reconfiguration must not rewrite the receipt
			tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
			_, err := adoptLocalLeaderBlock(produced, tail, NewTransactionStatusCache(), nil)
			require.ErrorIs(t, err, ErrLocalLeaderCommitMismatch)
			require.Empty(t, tail.bankhashes)
			require.Equal(t, before, global.VoteCacheItem(key))
			require.Contains(t, bank.PendingVoteCache, key)
		})
	}
}
