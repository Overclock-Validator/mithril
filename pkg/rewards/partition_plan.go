package rewards

import (
	"fmt"
	"os"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// PartitionedRewardPlan owns immutable calculation output. Progress lives in
// each bank's EpochRewards sysvar and block height, never in the plan. In
// particular, a discarded producer bank does not consume a reward partition.
type PartitionedRewardPlan struct {
	mu          sync.RWMutex
	retired     bool
	spoolDir    string
	spoolSlot   uint64
	initial     sealevel.SysvarEpochRewards
	commissions map[solana.PublicKey]uint8
	fileSizes   []int64 // -1 is a partition that was empty when calculation finished
}

func NewPartitionedRewardPlan(dir string, slot uint64, initial sealevel.SysvarEpochRewards, commissions map[solana.PublicKey]uint8) (*PartitionedRewardPlan, error) {
	if !initial.Active || initial.NumPartitions == 0 || initial.NumPartitions > 1<<32 || initial.DistributedRewards > initial.TotalRewards {
		return nil, fmt.Errorf("invalid partitioned reward plan metadata")
	}
	if stat, err := os.Stat(dir); err != nil || !stat.IsDir() {
		return nil, fmt.Errorf("reward plan spool directory is unavailable: %s", dir)
	}
	p := &PartitionedRewardPlan{spoolDir: dir, spoolSlot: slot, initial: initial, fileSizes: make([]int64, initial.NumPartitions)}
	if commissions != nil {
		p.commissions = make(map[solana.PublicKey]uint8, len(commissions))
		for key, commission := range commissions {
			p.commissions[key] = commission
		}
	}
	for index := range p.fileSizes {
		stat, err := os.Stat(partitionFilePath(dir, slot, uint32(index)))
		if os.IsNotExist(err) {
			p.fileSizes[index] = -1
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect reward partition %d: %w", index, err)
		}
		if !stat.Mode().IsRegular() || stat.Size()%SpoolRecordSize != 0 {
			return nil, fmt.Errorf("invalid reward partition %d spool", index)
		}
		p.fileSizes[index] = stat.Size()
	}
	return p, nil
}

func (p *PartitionedRewardPlan) NumPartitions() uint64 { return p.initial.NumPartitions }

// Matches binds the spool to the calculation identified by the selected bank.
// DistributedRewards and Active deliberately vary between its descendants.
func (p *PartitionedRewardPlan) Matches(sysvar sealevel.SysvarEpochRewards) bool {
	return p != nil && p.initial.DistributionStartingBlockHeight == sysvar.DistributionStartingBlockHeight &&
		p.initial.NumPartitions == sysvar.NumPartitions && p.initial.ParentBlockhash == sysvar.ParentBlockhash &&
		p.initial.TotalPoints == sysvar.TotalPoints && p.initial.TotalRewards == sysvar.TotalRewards &&
		sysvar.DistributedRewards >= p.initial.DistributedRewards && sysvar.DistributedRewards <= sysvar.TotalRewards
}

// Distribute reads a partition without consuming it. The lease excludes durable
// retirement until every speculative reader has finished opening/reading files.
func (p *PartitionedRewardPlan) Distribute(db *accountsdb.AccountsDb, index, slot uint64, loader RewardAccountLoader, config StakeRewardDistributionConfig) ([]*accounts.Account, []*accounts.Account, uint64, uint64, map[solana.PublicKey]uint8, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.retired || index >= uint64(len(p.fileSizes)) {
		return nil, nil, 0, 0, nil, fmt.Errorf("unavailable reward partition %d", index)
	}
	stat, err := os.Stat(partitionFilePath(p.spoolDir, p.spoolSlot, uint32(index)))
	if p.fileSizes[index] == -1 && os.IsNotExist(err) {
		return nil, nil, 0, 0, nil, nil
	}
	if err != nil {
		return nil, nil, 0, 0, nil, fmt.Errorf("read reward partition %d: %w", index, err)
	}
	if stat.Size() != p.fileSizes[index] {
		return nil, nil, 0, 0, nil, fmt.Errorf("reward partition %d spool changed after calculation", index)
	}
	updated, parents, paid, burned, commissions := DistributeStakingRewardsFromSpool(db, p.spoolDir, p.spoolSlot, index, slot, loader, p.commissions, config)
	return updated, parents, paid, burned, commissions, nil
}

// Retire is valid only after a completion bank is durable: no retained fork
// can then need this plan. Producer reads already in flight finish first.
func (p *PartitionedRewardPlan) Retire() {
	p.release()
}

// Discard abandons a calculation after the selected chain and every retained
// bank referencing it have been removed. This is distinct from normal durable
// retirement: a fork rewound before its boundary can never complete that plan.
func (p *PartitionedRewardPlan) Discard() {
	p.release()
}

func (p *PartitionedRewardPlan) release() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retired {
		return
	}
	p.retired = true
	CleanupPartitionedSpoolFiles(p.spoolDir, p.spoolSlot, p.initial.NumPartitions)
	// Calculation uses a private directory. Remove it only if now empty;
	// legacy plans may still share the AccountsDB parent directory.
	_ = os.Remove(p.spoolDir)
}
