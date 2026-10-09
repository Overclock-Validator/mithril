package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// GenesisEpochState persists bank-owned stake generations and historical vote
// states used by delayed commissions. These are execution metadata, not votes
// cast by a full node. Raw vote bytes preserve every versioned vote-state field.
// Reward spool files are deliberately excluded: active distribution is never a
// durable checkpoint, matching ReplayBlocks' promotion hold.
type GenesisEpochState struct {
	Stakes     map[uint64]json.RawMessage             `json:"stakes"`
	VoteStates map[uint64]map[solana.PublicKey][]byte `json:"vote_states"`
}

func genesisReplayContext(m genesis.BankMetadata) *ReplayCtx {
	return &ReplayCtx{Capitalization: m.Capitalization, SlotsPerYear: m.SlotsPerYear,
		Inflation: rewards.Inflation{Initial: m.Inflation.Initial, Terminal: m.Inflation.Terminal, Taper: m.Inflation.Taper,
			FoundationVal: m.Inflation.Foundation, FoundationTerm: m.Inflation.FoundationTerm}}
}

func initialGenesisEpochState(g *genesis.Genesis, seed *GenesisReplayBootstrap) (GenesisEpochState, error) {
	out := GenesisEpochState{Stakes: make(map[uint64]json.RawMessage), VoteStates: map[uint64]map[solana.PublicKey][]byte{0: {}}}
	votes := make(map[solana.PublicKey]*epochstakes.VoteAccount)
	for _, a := range g.Accounts {
		if a.Owner != solana.VoteProgramID {
			continue
		}
		state, err := sealevel.UnmarshalVersionedVoteState(a.Data)
		if err != nil {
			return out, err
		}
		votes[a.Pubkey] = &epochstakes.VoteAccount{Lamports: a.Lamports, NodePubkey: state.NodePubkey(), BlsPubkeyCompressed: state.BlsPubkeyCompressed(), Owner: a.Owner, RentEpoch: a.RentEpoch}
		out.VoteStates[0][a.Pubkey] = append([]byte(nil), a.Data...)
	}
	cache := epochstakes.NewEpochStakesCache()
	for _, e := range seed.metadata.EpochStakes {
		stakes := make(map[solana.PublicKey]uint64)
		for _, v := range e.Votes {
			stakes[solana.MustPublicKeyFromBase58(v.VoteAccount)] = v.Stake
		}
		cache.PutEpoch(e.Epoch, stakes, votes, e.TotalStake)
		raw, err := cache.SerializeEpoch(e.Epoch)
		if err != nil {
			return out, err
		}
		out.Stakes[e.Epoch] = raw
	}
	return out, nil
}

func (s GenesisEpochState) stakes(epoch uint64) (epochstakes.Snapshot, error) {
	cache := epochstakes.NewEpochStakesCache()
	loaded, err := cache.DeserializeAndLoadEpoch(s.Stakes[epoch])
	if err != nil || loaded != epoch {
		return epochstakes.Snapshot{}, fmt.Errorf("invalid checkpoint stakes for epoch %d: %v", epoch, err)
	}
	result, _ := cache.Snapshot(epoch)
	if len(result.Stakes) != len(result.VoteAccounts) {
		return result, fmt.Errorf("epoch %d stake/vote metadata mismatch", epoch)
	}
	var total uint64
	for key, stake := range result.Stakes {
		vote := result.VoteAccounts[key]
		if vote == nil || vote.NodePubkey.IsZero() || vote.Owner != solana.VoteProgramID || vote.BlsPubkeyCompressed == nil || vote.Executable != 0 || stake > math.MaxUint64-total {
			return result, fmt.Errorf("invalid epoch %d vote/stake metadata", epoch)
		}
		total += stake
	}
	if total != result.TotalStake {
		return result, fmt.Errorf("epoch %d total stake mismatch", epoch)
	}
	return result, nil
}

func firstRetainedGenesisEpoch(epoch uint64) uint64 {
	if epoch > 2 {
		return epoch - 2
	}
	return 0
}

func firstRetainedGenesisStakes(epoch uint64) uint64 {
	// Agave retains five leader-schedule stake generations (current + future
	// and up to three past epochs) in bank/snapshot metadata.
	if epoch > 3 {
		return epoch - 3
	}
	return 0
}

func (s GenesisEpochState) validate(epoch, leaderEpoch uint64) error {
	first := firstRetainedGenesisEpoch(epoch)
	if len(s.Stakes) != int(leaderEpoch-firstRetainedGenesisStakes(epoch)+1) || len(s.VoteStates) != int(epoch-first+1) {
		return fmt.Errorf("checkpoint missing epoch stake or commission history")
	}
	for e := firstRetainedGenesisStakes(epoch); e <= leaderEpoch; e++ {
		if _, err := s.stakes(e); err != nil {
			return err
		}
	}
	for e := first; e <= epoch; e++ {
		states, ok := s.VoteStates[e]
		if !ok || states == nil {
			return fmt.Errorf("missing epoch %d vote history", e)
		}
		for key, raw := range states {
			vote, err := sealevel.UnmarshalVersionedVoteState(raw)
			if err != nil || key.IsZero() || vote.Type != sealevel.VoteStateVersionV4 {
				return fmt.Errorf("invalid epoch %d vote history", e)
			}
		}
	}
	return nil
}

// Replay's historical caches are process-wide. Replace them from this bank's
// owned state before execution so an abandoned boundary or another offline
// database cannot seed a resumed bank with speculative future epoch stakes.
func (s GenesisEpochState) install() error {
	for _, e := range global.GetAllCachedEpochs() {
		global.ClearEpochStakes(e)
	}
	for e, raw := range s.Stakes {
		if loaded, err := global.DeserializeAndLoadEpochStakes(raw); err != nil || loaded != e {
			return fmt.Errorf("load epoch %d: %v", e, err)
		}
	}
	global.ClearEpochVoteStateSnapshots()
	epochs := make([]uint64, 0, len(s.VoteStates))
	for e := range s.VoteStates {
		epochs = append(epochs, e)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	for _, e := range epochs {
		votes := make(map[solana.PublicKey]*sealevel.VoteStateVersions)
		for key, raw := range s.VoteStates[e] {
			vote, err := sealevel.UnmarshalVersionedVoteState(raw)
			if err != nil {
				return err
			}
			votes[key] = vote
		}
		global.PutEpochVoteStateSnapshot(e, votes)
	}
	return nil
}

func (s *GenesisReplay) seedEpoch(slot uint64) uint64 {
	schedule, _ := s.seed.sysvars.EpochSchedule()
	return schedule.GetEpoch(slot)
}

// LeaderForSlot derives ingress's schedule from this session's computed epoch
// stakes, including the next epoch prepared at the previous boundary.
func (s *GenesisReplay) LeaderForSlot(slot uint64) (solana.PublicKey, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaderForSlot(slot)
}
func (s *GenesisReplay) leaderForSlot(slot uint64) (solana.PublicKey, bool) {
	schedule, _ := s.seed.sysvars.EpochSchedule()
	epoch := schedule.GetEpoch(slot)
	if leaders := s.leaders[epoch]; leaders != nil {
		return leaders.LeaderForSlot(slot)
	}
	stakes, err := s.epochs.stakes(epoch)
	if err != nil || stakes.TotalStake == 0 {
		return solana.PublicKey{}, false
	}
	leaders := leaderschedule.New(stakes.VoteAccounts, stakes.Stakes, &schedule, epoch, schedule.SlotsInEpoch(epoch), 4)
	if s.leaders == nil {
		s.leaders = make(map[uint64]*leaderschedule.LeaderSchedule)
	}
	s.leaders[epoch] = leaders
	return leaders.LeaderForSlot(slot)
}

func (s *GenesisReplay) prepareEpoch(ctx context.Context, block *b.Block, schedule *sealevel.SysvarEpochSchedule) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.epochs.install(); err != nil {
		return err
	}
	for key := range global.VoteCacheSnapshot() {
		global.DeleteVoteCacheItem(key)
	}
	// Rehydrate current vote states from this bank, not a previous process run.
	keys := make(map[solana.PublicKey]bool)
	for e := range s.epochs.Stakes {
		snapshot, err := s.epochs.stakes(e)
		if err != nil {
			return err
		}
		for key := range snapshot.VoteAccounts {
			keys[key] = true
		}
	}
	for key := range keys {
		acct, err := s.tail.GetAccount(s.height, key)
		if err != nil {
			return err
		}
		if acct.Lamports == 0 || acct.Owner != solana.VoteProgramID {
			continue
		}
		vote, err := sealevel.UnmarshalVersionedVoteState(acct.Data)
		if err != nil {
			return err
		}
		global.PutVoteCacheItem(key, vote)
	}
	rent, _ := s.seed.sysvars.Rent()
	sealevel.SysvarCache.Rent.Sysvar = &rent
	s.runtime.CurrentFeatures = s.seed.features.Clone() // no automatic profile upgrades
	previousEpoch := schedule.GetEpoch(s.height)
	if block.Epoch != previousEpoch {
		global.InvalidateStakePubkeyIndexCache()
		// This adapter owns its schedules. Keep the shared transition from publishing
		// a process-wide schedule that could replace another ingress's schedule.
		managed := global.ManageLeaderSchedule()
		global.SetManageLeaderSchedule(false)
		defer global.SetManageLeaderSchedule(managed)
		s.leaders = nil
		s.partitionedRewards = handleEpochTransition(s.db, true, s.tip, s.runtime, schedule, s.runtime.CurrentFeatures, block, previousEpoch, nil, nil)
		for _, e := range global.GetAllCachedEpochs() {
			if e < firstRetainedGenesisStakes(block.Epoch) {
				delete(s.epochs.Stakes, e)
				continue
			}
			raw, err := global.SerializeEpochStakes(e)
			if err != nil {
				return err
			}
			s.epochs.Stakes[e] = raw
		}
		snapshot := make(map[solana.PublicKey][]byte)
		for key, vote := range global.EpochVoteStateSnapshot(block.Epoch) {
			raw, err := sealevel.MarshalVersionedVoteState(vote)
			if err != nil {
				return err
			}
			snapshot[key] = raw
		}
		s.epochs.VoteStates[block.Epoch] = snapshot
		for e := range s.epochs.VoteStates {
			if e < firstRetainedGenesisEpoch(block.Epoch) {
				delete(s.epochs.VoteStates, e)
			}
		}
	}
	block.Features = s.runtime.CurrentFeatures
	stakes, err := s.epochs.stakes(block.Epoch)
	if err != nil {
		return err
	}
	block.EpochStakesPerVoteAcct, block.TotalEpochStake = stakes.Stakes, stakes.TotalStake
	var ok bool
	block.Leader, ok = s.leaderForSlot(block.Slot)
	if !ok {
		return fmt.Errorf("no admitted genesis replay leader for slot %d", block.Slot)
	}
	distributeEpochRewardsForBlock(s.db, s.tip, s.runtime, s.partitionedRewards, block)
	return nil
}
