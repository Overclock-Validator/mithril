package replay

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type bankRewardDistribution struct {
	updated, parents []*accounts.Account
	paid, burned     uint64
	remaining        uint64
	commissions      map[solana.PublicKey]uint8
	epochRewards     sealevel.SysvarEpochRewards
}

// RewardPlanForDistribution publishes only immutable calculation output. The
// replay-local remaining counter must never escape into producer snapshots.
func RewardPlanForDistribution(info *rewards.PartitionedRewardDistributionInfo) *rewards.PartitionedRewardPlan {
	if info == nil {
		return nil
	}
	return info.Plan
}

// distributeBankRewardPartition is shared by replay and production. All account
// reads are selected-parent reads, all writes are owned return values, and the
// immutable plan can be reused for sibling banks and abandoned leader attempts.
func distributeBankRewardPartition(db *accountsdb.AccountsDb, loader rewards.RewardAccountLoader, plan *rewards.PartitionedRewardPlan, f *features.Features, slot, height, epoch uint64) (*bankRewardDistribution, error) {
	acct, err := loader(sealevel.SysvarEpochRewardsAddr)
	if err != nil {
		return nil, fmt.Errorf("load bank EpochRewards: %w", err)
	}
	if acct == nil {
		return nil, fmt.Errorf("missing bank EpochRewards account")
	}
	var sysvar sealevel.SysvarEpochRewards
	if err := sysvar.UnmarshalWithDecoder(bin.NewBinDecoder(acct.Data)); err != nil {
		return nil, fmt.Errorf("decode bank EpochRewards: %w", err)
	}
	result := &bankRewardDistribution{epochRewards: sysvar}
	if !sysvar.Active {
		return result, nil
	}
	if !plan.Matches(sysvar) {
		return nil, fmt.Errorf("active bank EpochRewards has no matching partition plan")
	}
	result.remaining = sysvar.NumPartitions
	if height < sysvar.DistributionStartingBlockHeight {
		return result, nil
	}
	index := height - sysvar.DistributionStartingBlockHeight
	if index >= sysvar.NumPartitions {
		return nil, fmt.Errorf("active bank EpochRewards partition %d beyond %d partitions", index, sysvar.NumPartitions)
	}
	config := rewards.StakeRewardDistributionConfig{RewardedEpoch: safemath.SaturatingSubU64(epoch, 1)}
	if f != nil && f.IsActive(features.RelaxPostExecMinBalanceCheck) {
		rent, err := loader(sealevel.SysvarRentAddr)
		if err != nil || rent == nil {
			return nil, fmt.Errorf("load reward distribution bank Rent: %v", err)
		}
		if err := config.Rent.UnmarshalWithDecoder(bin.NewBinDecoder(rent.Data)); err != nil {
			return nil, fmt.Errorf("decode reward distribution bank Rent: %w", err)
		}
		config.AdjustDelegationsForRent = true
	}
	result.updated, result.parents, result.paid, result.burned, result.commissions, err = plan.Distribute(db, index, slot, loader, config)
	if err != nil {
		return nil, err
	}
	amount, err := safemath.CheckedAddU64(result.paid, result.burned)
	if err != nil || sysvar.DistributedRewards > sysvar.TotalRewards || amount > sysvar.TotalRewards-sysvar.DistributedRewards {
		return nil, fmt.Errorf("reward partition %d exceeds recorded reward budget", index)
	}
	sysvar.DistributedRewards += amount
	result.remaining = sysvar.NumPartitions - index - 1
	sysvar.Active = result.remaining != 0
	result.epochRewards = sysvar
	result.parents = append(result.parents, acct.Clone())
	updated := acct.Clone()
	var encoded bytes.Buffer
	if err := sysvar.MarshalWithEncoder(bin.NewBinEncoder(&encoded)); err != nil {
		return nil, err
	}
	// Keep any serialization padding exactly as replay did previously.
	if len(updated.Data) < encoded.Len() {
		return nil, fmt.Errorf("short EpochRewards account: %d", len(updated.Data))
	}
	copy(updated.Data, encoded.Bytes())
	result.updated = append(result.updated, updated)
	return result, nil
}

// LeaderEpochRewards retains the exact pre-transaction partition application.
// Replay validates this receipt against its independently selected parent before
// adopting the producer bank; transactions may subsequently modify these accounts.
type LeaderEpochRewards struct {
	updated, parents []*accounts.Account
	records          []rpc.BlockReward
}

// PrepareLeaderEpochRewards applies a partition before admitting transactions.
// Capitalization is advanced exactly once by ordered replay, not by speculative
// production. The account images feed the normal producer LtHash finalizer.
func PrepareLeaderEpochRewards(slotCtx *sealevel.SlotCtx, block *b.Block, plan *rewards.PartitionedRewardPlan) (*LeaderEpochRewards, error) {
	if slotCtx == nil || block == nil || slotCtx.BankSysvars() == nil {
		return nil, fmt.Errorf("missing leader reward bank context")
	}
	sysvar, present := slotCtx.BankSysvars().EpochRewards()
	if !present || !sysvar.Active {
		return nil, nil
	}
	result, err := distributeBankRewardPartition(slotCtx.AccountsDb, slotCtx.GetAccountFromAccountsDb, plan, slotCtx.Features, block.Slot, block.BlockHeight, block.Epoch)
	if err != nil {
		return nil, fmt.Errorf("prepare leader epoch rewards: %w", err)
	}
	if len(result.updated) == 0 {
		return nil, nil
	}
	receipt := &LeaderEpochRewards{updated: result.updated, parents: result.parents}
	metadata := &b.Block{Slot: block.Slot}
	if err := replaceInflationRewardRecords(metadata, result.updated, result.parents, rpc.RewardTypeStaking, result.commissions); err != nil {
		return nil, err
	}
	receipt.records = metadata.Rewards
	bankSysvars, err := slotCtx.BankSysvars().WithAccounts(result.updated[len(result.updated)-1])
	if err != nil {
		return nil, err
	}
	for index, updated := range result.updated {
		if updated == nil {
			continue
		}
		if err := slotCtx.ParentAccts.SetAccountWithoutLock(updated.Key, result.parents[index].Clone()); err != nil {
			return nil, err
		}
		if err := slotCtx.SetAccount(updated.Key, updated.Clone()); err != nil {
			return nil, err
		}
		slotCtx.RecordModifiedAcct(updated.Key)
	}
	if err := slotCtx.PublishBankSysvars(bankSysvars); err != nil {
		return nil, err
	}
	return receipt, nil
}

// Attach records the initial reward updates, not the post-transaction accounts.
func (r *LeaderEpochRewards) Attach(block *b.Block) {
	if r == nil {
		return
	}
	for i, updated := range r.updated {
		block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, cloneRewardAccount(updated))
		block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, cloneRewardAccount(r.parents[i]))
	}
	block.Rewards = append(block.Rewards, r.records...)
}

func cloneRewardAccount(acct *accounts.Account) *accounts.Account {
	if acct == nil {
		return nil
	}
	return acct.Clone()
}

func (r *LeaderEpochRewards) validate(block *b.Block) error {
	if r == nil {
		if len(block.EpochUpdatedAccts) != 0 {
			return fmt.Errorf("producer has no receipt for replay epoch account updates")
		}
		return nil
	}
	// Replay coalesces account updates and removes nil burned-reward entries.
	wantUpdated, wantParents := coalesceEpochAccountUpdates(r.updated, r.parents)
	gotUpdated, gotParents := coalesceEpochAccountUpdates(block.EpochUpdatedAccts, block.ParentEpochUpdatedAccts)
	if len(wantUpdated) != len(gotUpdated) || len(wantParents) != len(gotParents) {
		return fmt.Errorf("producer/replay epoch reward account counts differ")
	}
	for i := range wantUpdated {
		if !equalRewardAccount(wantUpdated[i], gotUpdated[i]) || !equalRewardAccount(wantParents[i], gotParents[i]) {
			return fmt.Errorf("producer/replay epoch reward account %d differs", i)
		}
	}
	return nil
}

func equalRewardAccount(a, b *accounts.Account) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Key == b.Key && a.Lamports == b.Lamports && a.Owner == b.Owner && a.Executable == b.Executable && a.RentEpoch == b.RentEpoch && bytes.Equal(a.Data, b.Data)
}
