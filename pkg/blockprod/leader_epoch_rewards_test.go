package blockprod

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLeaderStartsDuringRewardDistributionWithExactPlan(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	const parentSlot = uint64(8)
	global.SetReplayFrontier(parentSlot)
	global.SetAlpenglowBlockID(parentSlot, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(parentSlot, solana.Hash{2})
	parent := coherentTestParentContext(parentSlot, solana.Hash{3})
	parent.ParentBlockHeight = 49
	parent.EpochRewardsActive = true
	sysvar := sealevel.SysvarEpochRewards{Active: true, DistributionStartingBlockHeight: 50, NumPartitions: 1}
	var encoded bytes.Buffer
	require.NoError(t, sysvar.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
	var err error
	parent.EpochRewardPlan, err = rewards.NewPartitionedRewardPlan(t.TempDir(), 8, sysvar, nil)
	require.NoError(t, err)
	clock := sealevel.SysvarClock{Slot: parentSlot}
	rent := sealevel.NewDefaultRentSysvar()
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100, LeaderScheduleSlotOffset: 100}
	hashes := sealevel.SysvarSlotHashes{{Slot: 7}}
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{4}}}
	history := sealevel.SysvarSlotHistory{Bits: sealevel.SlotHistoryBitvec{Bits: sealevel.SlotHistoryInner{BlocksLen: 1, Blocks: []uint64{1}}, Len: 64}, NextSlot: 9}
	parent.BankSysvars, err = sealevel.NewBankSysvars(parentSlot,
		&accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()},
		leaderTestEpochScheduleAccount(t, schedule),
		&accounts.Account{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: hashes.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: recent.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: history.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarStakeHistoryAddr, Lamports: 1, Data: make([]byte, 8)},
		&accounts.Account{Key: sealevel.SysvarLastRestartSlotAddr, Lamports: 1, Data: make([]byte, 8)},
		&accounts.Account{Key: sealevel.SysvarEpochRewardsAddr, Lamports: 1, Data: encoded.Bytes()},
	)
	require.NoError(t, err)
	broadcaster := &captureBroadcaster{}
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity: identityHolder(txfixture.PayerPrivateKey()), AccountsDb: &accountsdb.AccountsDb{},
		Broadcaster: broadcaster, EpochSchedule: &schedule, AlpenglowClock: true,
		ParentContext: func(uint64) ParentContext { return parent },
	})
	require.NoError(t, loop.startSlotLocked(9))
	require.NotNil(t, loop.activeBank)
	t.Cleanup(func() { loop.abortActiveSlotLocked("test_done") })
	require.NotNil(t, loop.activeEpochRewards)
	child, present := loop.activeBank.SlotCtx().BankSysvars().EpochRewards()
	require.True(t, present)
	require.False(t, child.Active, "the empty final partition must complete before transactions are admitted")
	unchanged, present := parent.BankSysvars.EpochRewards()
	require.True(t, present)
	require.True(t, unchanged.Active, "parent bank must remain immutable")
	require.Positive(t, broadcaster.count())
}
