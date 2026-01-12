// This package acts as a singleton for storage and retrieval of data shared between replay and RPC

package global

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/forkchoice"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// StakePubkeyIndexFileName is the name of the stake pubkey index file
const StakePubkeyIndexFileName = "stake_pubkeys.idx"

type GlobalCtx struct {
	latestBlockhash            [32]byte
	blockHeight                uint64
	slot                       uint64
	epoch                      uint64
	transactionCount           uint64
	stakeCache                 map[solana.PublicKey]*sealevel.Delegation
	pendingNewStakePubkeys     []solana.PublicKey // New stake pubkeys to append to index after block commit
	voteCache                  map[solana.PublicKey]*sealevel.VoteStateVersions
	epochStakes                *epochstakes.EpochStakesCache
	epochAuthorizedVoters      *epochstakes.EpochAuthorizedVotersCache
	forkChoice                 *forkchoice.ForkChoiceService
	slotsConfirmed             map[uint64]struct{}
	leaderSchedule             *leaderschedule.LeaderSchedule
	calcUnixTimeForClockSysvar bool
	manageLeaderSchedule       bool
	manageBlockHeight          bool
	stakeCacheMutex            sync.Mutex // Changed from RWMutex - simpler, used for both cache and pending
	voteCacheMutex             sync.RWMutex
	slotsConfirmedMutex        sync.Mutex
	mu                         sync.Mutex
}

var instance GlobalCtx

func SetLatestBlockHash(blockHash [32]byte) {
	instance.SetLatestBlockhash(blockHash)
}

func SetBlockHeight(blockHeight uint64) {
	instance.SetBlockHeight(blockHeight)
}

func SetSlot(slot uint64) {
	instance.SetSlot(slot)
}

func SetEpoch(epoch uint64) {
	instance.SetEpoch(epoch)
}

func SetForkChoice(forkChoice *forkchoice.ForkChoiceService) {
	instance.forkChoice = forkChoice
}

func SubmitBlockToForkChoiceService(slot uint64, txs []*solana.Transaction) {
	instance.forkChoice.SubmitBlock(slot, txs)
}

func BankhashConfirmedForSlot(slot uint64, bankHash solana.Hash) int {
	return instance.forkChoice.IsBankhashCorrect(slot, bankHash)
}

func IncrTransactionCount(num uint64) {
	instance.IncrTransactionCount(num)
}

// PutStakeCacheItem adds or updates a stake cache entry during replay.
// If this is a NEW pubkey (not already in cache), it's added to pendingNewStakePubkeys
// for later append to the index file via FlushPendingStakePubkeys.
func PutStakeCacheItem(pubkey solana.PublicKey, delegation *sealevel.Delegation) {
	instance.stakeCacheMutex.Lock()
	defer instance.stakeCacheMutex.Unlock()
	if instance.stakeCache == nil {
		instance.stakeCache = make(map[solana.PublicKey]*sealevel.Delegation)
	}
	// Track new pubkeys for index append
	_, exists := instance.stakeCache[pubkey]
	if !exists {
		instance.pendingNewStakePubkeys = append(instance.pendingNewStakePubkeys, pubkey)
	}
	instance.stakeCache[pubkey] = delegation
}

// PutStakeCacheItemBulk adds a stake cache entry during bulk population (startup).
// Does NOT track new pubkeys - use this when loading cache from index/snapshot/scan
// to avoid enqueueing the entire cache on rebuild.
func PutStakeCacheItemBulk(pubkey solana.PublicKey, delegation *sealevel.Delegation) {
	instance.stakeCacheMutex.Lock()
	defer instance.stakeCacheMutex.Unlock()
	if instance.stakeCache == nil {
		instance.stakeCache = make(map[solana.PublicKey]*sealevel.Delegation)
	}
	instance.stakeCache[pubkey] = delegation
}

func DeleteStakeCacheItem(pubkey solana.PublicKey) {
	instance.stakeCacheMutex.Lock()
	defer instance.stakeCacheMutex.Unlock()
	delete(instance.stakeCache, pubkey)
}

func PutEpochAuthorizedVoter(voteAcct solana.PublicKey, authorizedVoter solana.PublicKey) {
	if instance.epochAuthorizedVoters == nil {
		instance.epochAuthorizedVoters = epochstakes.NewEpochAuthorizedVotersCache()
	}
	instance.epochAuthorizedVoters.PutEntry(voteAcct, authorizedVoter)
}

func EpochAuthorizedVoters() *epochstakes.EpochAuthorizedVotersCache {
	return instance.epochAuthorizedVoters
}

func PutSlotConfirmed(slot uint64) {
	if instance.slotsConfirmed == nil {
		instance.slotsConfirmed = make(map[uint64]struct{})
	}
	instance.slotsConfirmedMutex.Lock()
	defer instance.slotsConfirmedMutex.Unlock()
	instance.slotsConfirmed[slot] = struct{}{}
}

func SlotConfirmed(slot uint64) bool {
	instance.slotsConfirmedMutex.Lock()
	defer instance.slotsConfirmedMutex.Unlock()
	_, exists := instance.slotsConfirmed[slot]
	return exists
}

func StakeCache() map[solana.PublicKey]*sealevel.Delegation {
	return instance.stakeCache
}

func StakeCacheSnapshot() map[solana.PublicKey]*sealevel.Delegation {
	instance.stakeCacheMutex.Lock()
	defer instance.stakeCacheMutex.Unlock()

	if instance.stakeCache == nil {
		return nil
	}

	snapshot := make(map[solana.PublicKey]*sealevel.Delegation, len(instance.stakeCache))
	for pk, delegation := range instance.stakeCache {
		snapshot[pk] = delegation
	}
	return snapshot
}

func PutVoteCacheItem(pubkey solana.PublicKey, voteState *sealevel.VoteStateVersions) {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()
	if instance.voteCache == nil {
		instance.voteCache = make(map[solana.PublicKey]*sealevel.VoteStateVersions)
	}
	instance.voteCache[pubkey] = voteState
}

func VoteCacheItem(pubkey solana.PublicKey) *sealevel.VoteStateVersions {
	instance.voteCacheMutex.RLock()
	defer instance.voteCacheMutex.RUnlock()
	return instance.voteCache[pubkey]
}

func DeleteVoteCacheItem(pubkey solana.PublicKey) {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()
	delete(instance.voteCache, pubkey)
}

func VoteCache() map[solana.PublicKey]*sealevel.VoteStateVersions {
	return instance.voteCache
}

func VoteCacheSnapshot() map[solana.PublicKey]*sealevel.VoteStateVersions {
	instance.voteCacheMutex.RLock()
	defer instance.voteCacheMutex.RUnlock()

	if instance.voteCache == nil {
		return nil
	}

	snapshot := make(map[solana.PublicKey]*sealevel.VoteStateVersions, len(instance.voteCache))
	for pk, voteState := range instance.voteCache {
		snapshot[pk] = voteState
	}
	return snapshot
}

func PutEpochStakesEntry(epoch uint64, pubkey solana.PublicKey, stake uint64, voteAcct *epochstakes.VoteAccount) {
	if instance.epochStakes == nil {
		instance.epochStakes = epochstakes.NewEpochStakesCache()
	}
	instance.epochStakes.PutEntry(epoch, pubkey, stake, voteAcct)
}

func EpochStakes(epoch uint64) map[solana.PublicKey]uint64 {
	return instance.epochStakes.EpochStakes(epoch)
}

func HasEpochStakes(epoch uint64) bool {
	return instance.epochStakes.HasEpochStakes(epoch)
}

func PutEpochTotalStake(epoch uint64, totalStake uint64) {
	if instance.epochStakes == nil {
		instance.epochStakes = epochstakes.NewEpochStakesCache()
	}
	instance.epochStakes.PutTotalEpochStake(epoch, totalStake)
}

func EpochTotalStake(epoch uint64) uint64 {
	return instance.epochStakes.TotalStake(epoch)
}

func StakeForVoteAcct(epoch uint64, voteAcct solana.PublicKey) uint64 {
	epochStakes := instance.epochStakes.EpochStakes(epoch)
	return epochStakes[voteAcct]
}

func EpochStakesVoteAccts(epoch uint64) map[solana.PublicKey]*epochstakes.VoteAccount {
	return instance.epochStakes.EpochStakesAccts(epoch)
}

// ClearEpochStakes removes all stakes for a specific epoch.
// Used on resume to force rebuild from AccountsDB.
func ClearEpochStakes(epoch uint64) {
	if instance.epochStakes != nil {
		instance.epochStakes.ClearEpochStakes(epoch)
	}
}

// SerializeEpochStakes serializes the stakes for a single epoch to JSON.
func SerializeEpochStakes(epoch uint64) ([]byte, error) {
	if instance.epochStakes == nil {
		return nil, nil
	}
	return instance.epochStakes.SerializeEpoch(epoch)
}

// DeserializeAndLoadEpochStakes deserializes and loads epoch stakes from JSON.
// Returns the epoch number that was loaded.
func DeserializeAndLoadEpochStakes(data []byte) (uint64, error) {
	if instance.epochStakes == nil {
		instance.epochStakes = epochstakes.NewEpochStakesCache()
	}
	return instance.epochStakes.DeserializeAndLoadEpoch(data)
}

// GetAllCachedEpochs returns all epochs currently in the epoch stakes cache.
func GetAllCachedEpochs() []uint64 {
	if instance.epochStakes == nil {
		return nil
	}
	return instance.epochStakes.GetAllEpochs()
}

func LatestBlockHash() [32]byte {
	return instance.LatestBlockhash()
}

func BlockHeight() uint64 {
	return instance.BlockHeight()
}

func Slot() uint64 {
	return instance.Slot()
}

func Epoch() uint64 {
	return instance.Epoch()
}

func TransactionCount() uint64 {
	return instance.TransactionCount()
}

func SetCalcUnixTimeForClockSysvar(calcUnixTime bool) {
	instance.calcUnixTimeForClockSysvar = calcUnixTime
}

func CalcUnixTimeForClockSysvar() bool {
	return instance.calcUnixTimeForClockSysvar
}

func SetManageLeaderSchedule(manageLeaderSchedule bool) {
	instance.manageLeaderSchedule = manageLeaderSchedule
}

func ManageLeaderSchedule() bool {
	return instance.manageLeaderSchedule
}

func SetManageBlockHeight(manageBlockHeight bool) {
	instance.manageBlockHeight = true
}

func ManageBlockHeight() bool {
	return instance.manageBlockHeight
}

func SetLeaderSchedule(ls *leaderschedule.LeaderSchedule) {
	instance.leaderSchedule = ls
}

func LeaderForSlot(slot uint64) (solana.PublicKey, bool) {
	if instance.leaderSchedule == nil {
		return solana.PublicKey{}, false
	}
	return instance.leaderSchedule.LeaderForSlot(slot)
}

func (globctx *GlobalCtx) SetLatestBlockhash(blockhash [32]byte) {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	globctx.latestBlockhash = blockhash
}

func (globctx *GlobalCtx) SetBlockHeight(blockHeight uint64) {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	globctx.blockHeight = blockHeight
}

func (globctx *GlobalCtx) SetSlot(slot uint64) {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	globctx.slot = slot
}

func (globctx *GlobalCtx) SetEpoch(epoch uint64) {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	globctx.epoch = epoch
}

func (globctx *GlobalCtx) IncrTransactionCount(num uint64) {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	globctx.transactionCount += num
}

func (globctx *GlobalCtx) LatestBlockhash() [32]byte {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	return globctx.latestBlockhash
}

func (globctx *GlobalCtx) BlockHeight() uint64 {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	return globctx.blockHeight
}

func (globctx *GlobalCtx) Slot() uint64 {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	return globctx.slot
}

func (globctx *GlobalCtx) Epoch() uint64 {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	return globctx.epoch
}

func (globctx *GlobalCtx) TransactionCount() uint64 {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	return globctx.transactionCount
}

// FlushPendingStakePubkeys appends any new stake pubkeys discovered during replay
// to the stake pubkey index file. Called after each block commit.
// Returns the number of pubkeys flushed.
func FlushPendingStakePubkeys(accountsDbDir string) (int, error) {
	instance.stakeCacheMutex.Lock()
	if len(instance.pendingNewStakePubkeys) == 0 {
		instance.stakeCacheMutex.Unlock()
		return 0, nil
	}
	// Copy pending slice and clear it while holding lock
	pending := make([]solana.PublicKey, len(instance.pendingNewStakePubkeys))
	copy(pending, instance.pendingNewStakePubkeys)
	instance.pendingNewStakePubkeys = nil
	instance.stakeCacheMutex.Unlock()

	// Append to index file (don't hold lock during I/O)
	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	f, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("opening stake pubkey index for append: %w", err)
	}
	defer f.Close()

	for _, pk := range pending {
		if _, err := f.Write(pk[:]); err != nil {
			return 0, fmt.Errorf("writing stake pubkey to index: %w", err)
		}
	}

	// Ensure data is flushed to disk before returning.
	// This is critical: the index must be at least as current as the state file.
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("syncing stake pubkey index: %w", err)
	}

	return len(pending), nil
}

// ClearPendingStakePubkeys discards any pending stake pubkeys without writing them.
// Used for rollback on failed block replay.
func ClearPendingStakePubkeys() {
	instance.stakeCacheMutex.Lock()
	defer instance.stakeCacheMutex.Unlock()
	instance.pendingNewStakePubkeys = nil
}

// LoadStakePubkeyIndex reads the stake pubkey index file and returns deduplicated pubkeys.
// Validates that file length is a multiple of 32 bytes.
func LoadStakePubkeyIndex(accountsDbDir string) ([]solana.PublicKey, error) {
	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}

	// Validate file length
	if len(data)%32 != 0 {
		return nil, fmt.Errorf("stake pubkey index file corrupt: length %d is not a multiple of 32", len(data))
	}

	numPubkeys := len(data) / 32
	if numPubkeys == 0 {
		return nil, nil
	}

	// Deduplicate pubkeys using a map
	seen := make(map[solana.PublicKey]struct{}, numPubkeys)
	pubkeys := make([]solana.PublicKey, 0, numPubkeys)

	for i := 0; i < len(data); i += 32 {
		var pk solana.PublicKey
		copy(pk[:], data[i:i+32])
		if _, exists := seen[pk]; !exists {
			seen[pk] = struct{}{}
			pubkeys = append(pubkeys, pk)
		}
	}

	return pubkeys, nil
}

// SaveStakePubkeyIndex writes the current stake cache pubkeys to the index file.
// This compacts the index by removing duplicates and closed accounts.
// Used during graceful shutdown.
func SaveStakePubkeyIndex(accountsDbDir string) error {
	instance.stakeCacheMutex.Lock()
	// Get current cache pubkeys
	pubkeys := make([]solana.PublicKey, 0, len(instance.stakeCache))
	for pk := range instance.stakeCache {
		pubkeys = append(pubkeys, pk)
	}
	instance.stakeCacheMutex.Unlock()

	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	f, err := os.Create(indexPath)
	if err != nil {
		return fmt.Errorf("creating stake pubkey index: %w", err)
	}
	defer f.Close()

	for _, pk := range pubkeys {
		if _, err := f.Write(pk[:]); err != nil {
			return fmt.Errorf("writing stake pubkey to index: %w", err)
		}
	}

	return nil
}
