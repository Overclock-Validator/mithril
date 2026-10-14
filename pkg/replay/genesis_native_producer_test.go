package replay_test

import (
	"bytes"
	"crypto/ed25519"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGenesisNativeProducerAgave(t *testing.T) {
	runNativeProducer(t, replay.RunGenesisProducerFixture)
}

func TestGenesisNativeProducerCheckpointAgave(t *testing.T) {
	runNativeProducer(t, replay.RunGenesisProducerCheckpointFixture)
}

func runNativeProducer(t *testing.T, run func(*testing.T, func(*testing.T, replay.GenesisProducerInput) (*block.Block, *sealevel.SlotCtx))) {
	run(t, func(t *testing.T, in replay.GenesisProducerInput) (*block.Block, *sealevel.SlotCtx) {
		next := in.Next
		parent := blockprod.ParentContext{
			ReplayGeneration: next.Slot, ParentSlot: next.ParentSlot, ParentBankhash: next.ParentBankhash,
			ParentBlockID: in.ParentID, HasParentBlockID: true, ParentChainedMerkleRoot: in.ParentRoot, HasParentChainedMerkleRoot: true,
			ParentLastEntryHash: next.LastBlockhash, ParentLastBlockhash: next.LastBlockhash, ParentBlockHeight: next.BlockHeight - 1,
			PrevNumSigs: next.PrevNumSignatures, PrevFeeGovernor: next.PrevFeeRateGovernor, AcctsLtHash: next.AcctsLtHash,
			Features: next.Features, BankSysvars: in.ParentSysvars, EpochStakes: next.EpochStakesPerVoteAcct, TotalEpochStake: next.TotalEpochStake,
			VoteTimestamps: next.VoteTimestamps, UnrootedRead: in.ParentReader, TransactionStatuses: in.Statuses,
			NanosecondClockAccount: in.ParentNano, HasNanosecondClockAccount: in.ParentNano != nil,
		}
		if next.Slot == 1 {
			parent.GenesisParent = in.Seed
		}
		controller := blockprod.NewController()
		var wall atomic.Uint64
		var nanos atomic.Int64
		wall.Store(next.Slot)
		nanos.Store(int64(in.ProducerTime) - 400_000_000)
		completed := make(chan *block.Block, 1)
		loop := blockprod.NewLeaderLoop(blockprod.LeaderLoopConfig{
			Controller: controller, Identity: solana.PrivateKey(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, 32))),
			AccountsDb: in.DB, Broadcaster: in.Broadcaster, ShredVersion: in.Version, AlpenglowClock: true,
			ParentContext: func(uint64) blockprod.ParentContext { return parent },
			CurrentSlot:   wall.Load, LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
				leader, ok := in.Seed.LeaderForSlot(slot)
				return leader, ok && slot == next.Slot
			},
			Now: func() time.Time { return time.Unix(0, nanos.Load()) }, OnBlock: func(b *block.Block) { completed <- b },
		})
		stop, done := make(chan struct{}), make(chan struct{})
		go func() { defer close(done); loop.Run(stop) }()
		defer func() {
			close(stop)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("leader did not stop")
			}
		}()
		require.Eventually(t, func() bool { return controller.WorkingBank() != nil }, 10*time.Second, time.Millisecond)
		bank := controller.WorkingBank()
		in.Prepared(bank.SlotCtx())
		for _, tx := range in.Transactions {
			require.NoError(t, tx.VerifySignatures())
			wire, err := tx.MarshalBinary()
			require.NoError(t, err)
			result, reason := bank.Forge(wire)
			require.Equal(t, blockprod.ForgeAccepted, result)
			require.Equal(t, costmodel.ExceedNone, reason)
			// Duplicate admission must not change entries, fees or bank state.
			result, _ = bank.Forge(wire)
			require.Equal(t, blockprod.ForgeDroppedAlreadyProcessed, result)
		}
		nanos.Store(int64(in.ProducerTime))
		wall.Store(next.Slot + 1)
		select {
		case produced := <-completed:
			local, ok := replay.TakeLocalLeaderCommit(next.Slot)
			require.True(t, ok)
			require.Same(t, bank.SlotCtx(), local.SlotCtx)
			return produced, local.SlotCtx
		case <-time.After(10 * time.Second):
			t.Fatal("native leader did not finalize or broadcast")
		}
		return nil, nil
	})
}
