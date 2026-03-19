package forkchoice

import (
	"fmt"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// BankhashStatus represents the confirmation status of a slot's bankhash.
type BankhashStatus int

const (
	BankhashHasSupermajority BankhashStatus = iota
	BankhashNoSupermajority
	BankhashNeedWait
)

func (s BankhashStatus) String() string {
	switch s {
	case BankhashHasSupermajority:
		return "has_supermajority"
	case BankhashNoSupermajority:
		return "no_supermajority"
	case BankhashNeedWait:
		return "need_wait"
	default:
		return "unknown"
	}
}

// BankhashResult provides detailed fork choice query results.
type BankhashResult struct {
	Status          BankhashStatus
	WinningHash     solana.Hash
	StakeForHash    uint64 // stake accumulated for the queried hash
	WinningStake    uint64 // stake accumulated for the winning hash (may differ from StakeForHash)
	TotalEpochStake uint64
	ThresholdStake  uint64
}

type blockJob struct {
	slot uint64
	txs  []*solana.Transaction

	// Epoch data captured at submission time so that each block is processed
	// with the epoch view that was current when it was submitted, not when
	// it happens to be dequeued. This prevents post-boundary epoch data from
	// being applied to pre-boundary blocks sitting in the queue.
	epochStakes           map[solana.PublicKey]uint64
	epochAuthorizedVoters *epochstakes.EpochAuthorizedVotersCache
	totalEpochStake       uint64
}

type voteUpdate struct {
	voteInfo *voteInfo
	stake    uint64
}

type forkChoiceState struct {
	voteStakeTotals       map[uint64]*slotVoteAccumulator
	epoch                 uint64
	epochStakes           map[solana.PublicKey]uint64
	epochAuthorizedVoters *epochstakes.EpochAuthorizedVotersCache
	totalEpochStake       uint64
	latestSlotIngested    uint64
	mu                    sync.Mutex
}

type ForkChoiceService struct {
	state    *forkChoiceState
	jobChan  chan *blockJob
	wg       sync.WaitGroup
	shutdown chan struct{}
}

func NewForkChoiceService(
	epoch uint64,
	epochStakes map[solana.PublicKey]uint64,
	totalEpochStake uint64,
	epochAuthorizedVoters *epochstakes.EpochAuthorizedVotersCache,
) *ForkChoiceService {

	state := &forkChoiceState{
		voteStakeTotals:       make(map[uint64]*slotVoteAccumulator),
		epoch:                 epoch,
		epochStakes:           epochStakes,
		epochAuthorizedVoters: epochAuthorizedVoters,
		totalEpochStake:       totalEpochStake,
	}

	return &ForkChoiceService{
		state:    state,
		jobChan:  make(chan *blockJob, 32),
		shutdown: make(chan struct{}),
	}
}

func (s *ForkChoiceService) Start() {
	s.wg.Add(1)
	go s.run()
}

func (s *ForkChoiceService) Stop() {
	close(s.shutdown)
	s.wg.Wait()
	close(s.jobChan)
}

func (s *ForkChoiceService) run() {
	defer s.wg.Done()
	for {
		select {
		case job, ok := <-s.jobChan:
			if !ok {
				return
			}
			s.processBlock(job)
		case <-s.shutdown:
			// Drain remaining jobs before exiting.
			for {
				select {
				case job, ok := <-s.jobChan:
					if !ok {
						return
					}
					s.processBlock(job)
				default:
					return
				}
			}
		}
	}
}

func (s *ForkChoiceService) SubmitBlock(slot uint64, txs []*solana.Transaction) {
	// Snapshot epoch data at submission time so the job carries a consistent
	// epoch view regardless of when it is actually processed. This prevents
	// UpdateEpoch from changing the epoch mid-queue.
	s.state.mu.Lock()
	job := &blockJob{
		slot:                  slot,
		txs:                   txs,
		epochStakes:           s.state.epochStakes,
		epochAuthorizedVoters: s.state.epochAuthorizedVoters,
		totalEpochStake:       s.state.totalEpochStake,
	}
	s.state.mu.Unlock()

	select {
	case s.jobChan <- job:
	case <-s.shutdown:
		fmt.Printf("fork choice service shutting down, discarding job for slot %d\n", slot)
	}
}

func (s *ForkChoiceService) processBlock(job *blockJob) {
	// Use epoch data captured at SubmitBlock time. This ensures vote parsing,
	// stake lookups, and threshold computation all use a consistent epoch view
	// — even if UpdateEpoch fires between submission and processing.
	epochStakes := job.epochStakes
	epochAuthorizedVoters := job.epochAuthorizedVoters
	totalEpochStake := job.totalEpochStake

	var updatesToApply []voteUpdate

	for _, tx := range job.txs {
		if !tx.IsVote() {
			continue
		}

		voteInfo, ok := parseAndValidateVoteTx(tx, epochAuthorizedVoters)
		if !ok {
			continue
		}

		stakeForVoteAcct, ok := epochStakes[voteInfo.votePubkey]
		if !ok {
			continue
		}

		updatesToApply = append(updatesToApply, voteUpdate{
			voteInfo: voteInfo,
			stake:    stakeForVoteAcct,
		})
	}

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	for _, update := range updatesToApply {
		accumulator, exists := s.state.voteStakeTotals[update.voteInfo.slot]
		if !exists {
			accumulator = newSlotVoteAccumulator(totalEpochStake, update.voteInfo.slot)
			s.state.voteStakeTotals[update.voteInfo.slot] = accumulator
		}

		thresholdCrossed, _ := accumulator.addVote(
			update.voteInfo.bankHash,
			update.voteInfo.votePubkey,
			update.stake,
		)

		if thresholdCrossed {
			mlog.Log.Infof("forkchoice: slot %d hash %s crossed supermajority (stake=%d/%d threshold=%d)",
				update.voteInfo.slot,
				base58.Encode(update.voteInfo.bankHash[:]),
				accumulator.stakeForHash(update.voteInfo.bankHash),
				totalEpochStake,
				accumulator.thresholdStake,
			)
		}
	}

	if s.state.latestSlotIngested < job.slot {
		s.state.latestSlotIngested = job.slot
	}
}

// UpdateEpoch swaps in new epoch stake data. Called at epoch boundaries
// so that vote stake weights and authorized voter lookups use current data.
func (s *ForkChoiceService) UpdateEpoch(
	epoch uint64,
	epochStakes map[solana.PublicKey]uint64,
	totalEpochStake uint64,
	epochAuthorizedVoters *epochstakes.EpochAuthorizedVotersCache,
) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	s.state.epoch = epoch
	s.state.epochStakes = epochStakes
	s.state.totalEpochStake = totalEpochStake
	s.state.epochAuthorizedVoters = epochAuthorizedVoters

	mlog.Log.Infof("forkchoice: updated epoch stakes for epoch %d (total_stake=%d, validators=%d)",
		epoch, totalEpochStake, len(epochStakes))
}

// VoteConfirmationTimeoutSlots is the grace window (in slots) before an
// unresolved slot (no supermajority winner) transitions from NeedWait to
// NoSupermajority. This is NOT a mandatory delay before confirmation — a slot
// with observed supermajority is confirmed immediately regardless of this window.
const VoteConfirmationTimeoutSlots = 32

// IsBankhashCorrect queries the confirmation status of a slot's bankhash.
// Returns a BankhashResult with status, winning hash, stake details, and threshold.
//
// A slot is confirmed immediately when a winner is observed — no mandatory delay.
// The timeout window (VoteConfirmationTimeoutSlots) only governs how long an
// unresolved slot (no winner) stays in NeedWait before becoming NoSupermajority.
func (s *ForkChoiceService) IsBankhashCorrect(slot uint64, bankHash solana.Hash) BankhashResult {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	accumulator, exists := s.state.voteStakeTotals[slot]

	// Fast path: if a winner exists, return immediately regardless of timeout.
	if exists {
		winningHash, hasWinner := accumulator.winningHash()
		if hasWinner {
			stakeForHash := accumulator.stakeForHash(bankHash)
			winningStake := accumulator.stakeForHash(winningHash)

			if winningHash == bankHash {
				return BankhashResult{
					Status:          BankhashHasSupermajority,
					WinningHash:     winningHash,
					StakeForHash:    stakeForHash,
					WinningStake:    winningStake,
					TotalEpochStake: accumulator.totalEpochStake,
					ThresholdStake:  accumulator.thresholdStake,
				}
			}

			// Mismatch: our hash lost to a different supermajority hash.
			mlog.Log.Warnf("forkchoice: slot %d bankhash mismatch! our=%s winning=%s (our_stake=%d winning_stake=%d/%d)",
				slot,
				base58.Encode(bankHash[:]),
				base58.Encode(winningHash[:]),
				stakeForHash,
				winningStake,
				accumulator.totalEpochStake,
			)
			return BankhashResult{
				Status:          BankhashNoSupermajority,
				WinningHash:     winningHash,
				StakeForHash:    stakeForHash,
				WinningStake:    winningStake,
				TotalEpochStake: accumulator.totalEpochStake,
				ThresholdStake:  accumulator.thresholdStake,
			}
		}
	}

	// No winner yet — use timeout to decide NeedWait vs NoSupermajority.
	if s.state.latestSlotIngested < (slot + VoteConfirmationTimeoutSlots) {
		totalStake := s.state.totalEpochStake
		if exists {
			totalStake = accumulator.totalEpochStake
		}
		return BankhashResult{
			Status:          BankhashNeedWait,
			TotalEpochStake: totalStake,
		}
	}

	// Timeout expired with no winner.
	if exists {
		stakeForHash := accumulator.stakeForHash(bankHash)
		return BankhashResult{
			Status:          BankhashNoSupermajority,
			StakeForHash:    stakeForHash,
			TotalEpochStake: accumulator.totalEpochStake,
			ThresholdStake:  accumulator.thresholdStake,
		}
	}

	return BankhashResult{
		Status:          BankhashNoSupermajority,
		TotalEpochStake: s.state.totalEpochStake,
	}
}

// GetSupermajorityHash returns the vote-confirmed hash for a slot, if any hash
// has crossed the 2/3 supermajority threshold. Used by the consensus coordinator
// to obtain the target hash for skipPath solving.
//
// Returns immediately when a winner is observed. The timeout window only governs
// when an unresolved slot transitions from NeedWait to NoSupermajority.
func (s *ForkChoiceService) GetSupermajorityHash(slot uint64) (solana.Hash, BankhashStatus) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	// Fast path: winner exists → return immediately.
	if accumulator, exists := s.state.voteStakeTotals[slot]; exists {
		if winningHash, ok := accumulator.winningHash(); ok {
			return winningHash, BankhashHasSupermajority
		}
	}

	// No winner — use timeout to decide NeedWait vs NoSupermajority.
	if s.state.latestSlotIngested < (slot + VoteConfirmationTimeoutSlots) {
		return solana.Hash{}, BankhashNeedWait
	}

	return solana.Hash{}, BankhashNoSupermajority
}
