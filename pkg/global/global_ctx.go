// This package acts as a singleton for storage and retrieval of data shared between replay and RPC

package global

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/forkchoice"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/panjf2000/ants/v2"
)

// StakePubkeyIndexFileName is the name of the stake pubkey index file
const StakePubkeyIndexFileName = "stake_pubkeys.idx"

type GlobalCtx struct {
	latestBlockhash            [32]byte
	blockHeight                uint64
	slot                       uint64
	epoch                      uint64
	transactionCount           uint64
	pendingStakeBySlot         map[uint64][]accountsdb.StakeIndexEntry // Speculative stake entries, grouped by producing slot
	cachedStakeEntries         []accountsdb.StakeIndexEntry            // Parsed+sorted index, populated on first load
	entriesFlushedSinceCompact int                                     // Appended entries since last compaction
	voteCache                  map[solana.PublicKey]*sealevel.VoteStateVersions
	epochVoteStateSnapshots    map[uint64]map[solana.PublicKey]*sealevel.VoteStateVersions
	epochStakes                *epochstakes.EpochStakesCache
	epochAuthorizedVoters      *epochstakes.EpochAuthorizedVotersCache
	forkChoice                 *forkchoice.ForkChoiceService
	slotsConfirmed             map[uint64]struct{}
	leaderSchedule             *leaderschedule.LeaderSchedule
	nextLeaderSchedule         *leaderschedule.LeaderSchedule
	calcUnixTimeForClockSysvar bool
	manageLeaderSchedule       bool
	pendingStakeMutex          sync.Mutex // Protects pendingStakeBySlot and cachedStakeEntries
	stakeIndexIOMutex          sync.Mutex // Serializes stake-index reads, appends, and rewrites
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
	notifyWallClockSlot(slot)
}

func SetEpoch(epoch uint64) {
	instance.SetEpoch(epoch)
}

func SetForkChoice(forkChoice *forkchoice.ForkChoiceService) {
	instance.forkChoice = forkChoice
}

func HasForkChoice() bool {
	return instance.forkChoice != nil
}

func SubmitBlockToForkChoiceService(slot uint64, txs []*solana.Transaction) {
	if instance.forkChoice == nil {
		return
	}
	instance.forkChoice.SubmitBlock(slot, txs)
}

func BankhashConfirmedForSlot(slot uint64, bankHash solana.Hash) int {
	if instance.forkChoice == nil {
		return 0
	}
	return int(instance.forkChoice.IsBankhashCorrect(slot, bankHash).Status)
}

func IncrTransactionCount(num uint64) {
	instance.IncrTransactionCount(num)
}

func SetTransactionCount(num uint64) {
	instance.SetTransactionCount(num)
}

// EnqueuePendingStakePubkey records a stake pubkey for later append to the index file.
// Called during tx processing when a stake account is created or modified.
// Deduplication happens at index load time (LoadStakePubkeyIndex keeps last occurrence).
func EnqueuePendingStakePubkey(slot uint64, pubkey solana.PublicKey) {
	instance.pendingStakeMutex.Lock()
	defer instance.pendingStakeMutex.Unlock()
	if instance.pendingStakeBySlot == nil {
		instance.pendingStakeBySlot = make(map[uint64][]accountsdb.StakeIndexEntry)
	}
	instance.pendingStakeBySlot[slot] = append(instance.pendingStakeBySlot[slot], accountsdb.StakeIndexEntry{Pubkey: pubkey})
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

// SetEpochAuthorizedVoters replaces the entire authorized voters cache.
// Called at epoch boundaries after rebuilding from vote accounts.
func SetEpochAuthorizedVoters(cache *epochstakes.EpochAuthorizedVotersCache) {
	instance.epochAuthorizedVoters = cache
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

// ResetVoteCache clears the current epoch's derived vote-state cache before a
// branch-aware rebuild. Epoch stake tables remain untouched because they are
// keyed by epoch and derived from the prior epoch's rooted state.
func ResetVoteCache() {
	instance.voteCacheMutex.Lock()
	instance.voteCache = make(map[solana.PublicKey]*sealevel.VoteStateVersions)
	instance.voteCacheMutex.Unlock()
}

func PutEpochVoteStateSnapshot(epoch uint64, snapshot map[solana.PublicKey]*sealevel.VoteStateVersions) {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()

	if instance.epochVoteStateSnapshots == nil {
		instance.epochVoteStateSnapshots = make(map[uint64]map[solana.PublicKey]*sealevel.VoteStateVersions)
	}

	snapshotCopy := make(map[solana.PublicKey]*sealevel.VoteStateVersions, len(snapshot))
	for pk, voteState := range snapshot {
		snapshotCopy[pk] = voteState
	}
	instance.epochVoteStateSnapshots[epoch] = snapshotCopy

	for cachedEpoch := range instance.epochVoteStateSnapshots {
		if cachedEpoch+2 < epoch {
			delete(instance.epochVoteStateSnapshots, cachedEpoch)
		}
	}
}

func EpochVoteStateSnapshot(epoch uint64) map[solana.PublicKey]*sealevel.VoteStateVersions {
	instance.voteCacheMutex.RLock()
	defer instance.voteCacheMutex.RUnlock()

	snapshot := instance.epochVoteStateSnapshots[epoch]
	if snapshot == nil {
		return nil
	}

	snapshotCopy := make(map[solana.PublicKey]*sealevel.VoteStateVersions, len(snapshot))
	for pk, voteState := range snapshot {
		snapshotCopy[pk] = voteState
	}
	return snapshotCopy
}

func ClearEpochVoteStateSnapshots() {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()

	instance.epochVoteStateSnapshots = nil
}

// DropEpochVoteStateSnapshotsAfter removes snapshots produced by speculative
// epoch transitions above the selected rollback epoch.
func DropEpochVoteStateSnapshotsAfter(epoch uint64) {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()
	for cachedEpoch := range instance.epochVoteStateSnapshots {
		if cachedEpoch > epoch {
			delete(instance.epochVoteStateSnapshots, cachedEpoch)
		}
	}
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

// ClearEpochStakesAfter removes future tables derived by a losing speculative
// epoch transition while retaining the selected epoch and its valid lookahead.
func ClearEpochStakesAfter(maxEpoch uint64) {
	if instance.epochStakes != nil {
		instance.epochStakes.ClearAfter(maxEpoch)
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

func SetLeaderSchedule(ls *leaderschedule.LeaderSchedule) {
	instance.leaderSchedule = ls
	instance.nextLeaderSchedule = nil
}

// SetNextLeaderSchedule stores the following epoch's schedule for look-ahead queries.
func SetNextLeaderSchedule(ls *leaderschedule.LeaderSchedule) {
	instance.nextLeaderSchedule = ls
}

func LeaderForSlot(slot uint64) (solana.PublicKey, bool) {
	if instance.leaderSchedule != nil {
		if leader, ok := instance.leaderSchedule.LeaderForSlot(slot); ok {
			return leader, true
		}
	}
	if instance.nextLeaderSchedule != nil {
		return instance.nextLeaderSchedule.LeaderForSlot(slot)
	}
	return solana.PublicKey{}, false
}

func NextLeaderSlotForIdentity(identity solana.PublicKey, fromSlot uint64) (uint64, bool) {
	if instance.leaderSchedule != nil {
		if next, ok := instance.leaderSchedule.NextSlotForLeader(identity, fromSlot); ok {
			return next, true
		}
	}
	if instance.nextLeaderSchedule != nil {
		return instance.nextLeaderSchedule.NextSlotForLeader(identity, fromSlot)
	}
	return 0, false
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

func (globctx *GlobalCtx) SetTransactionCount(num uint64) {
	globctx.mu.Lock()
	globctx.transactionCount = num
	globctx.mu.Unlock()
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

// PendingStakeEntriesSnapshot returns every speculative stake-index entry. The
// returned slice is detached from global state and ordered by producing slot.
func PendingStakeEntriesSnapshot() []accountsdb.StakeIndexEntry {
	instance.pendingStakeMutex.Lock()
	defer instance.pendingStakeMutex.Unlock()
	return pendingStakeEntriesLocked(math.MaxUint64)
}

func pendingStakeEntriesLocked(through uint64) []accountsdb.StakeIndexEntry {
	slots := make([]uint64, 0, len(instance.pendingStakeBySlot))
	for slot := range instance.pendingStakeBySlot {
		if slot <= through {
			slots = append(slots, slot)
		}
	}
	slices.Sort(slots)
	var pending []accountsdb.StakeIndexEntry
	for _, slot := range slots {
		pending = append(pending, instance.pendingStakeBySlot[slot]...)
	}
	return pending
}

// FlushPendingStakePubkeys appends every pending stake entry. Non-speculative
// callers retain the legacy all-through behavior.
// Writes 48-byte records to match the header written at snapshot time.
// Returns the number of entries flushed.
func FlushPendingStakePubkeys(accountsDbDir string) (int, error) {
	return FlushPendingStakePubkeysThrough(accountsDbDir, math.MaxUint64)
}

// FlushPendingStakePubkeysThrough persists only entries produced at or below
// through. Entries above the durable watermark remain branch-local in memory.
// The stake index is intentionally flushed before the matching AccountsDB fold:
// a crash may leave complete extra pubkeys in this index, but account lookup
// filters those harmless false positives. Flushing after the fold could instead
// omit a committed stake account permanently.
func FlushPendingStakePubkeysThrough(accountsDbDir string, through uint64) (int, error) {
	instance.stakeIndexIOMutex.Lock()
	defer instance.stakeIndexIOMutex.Unlock()

	instance.pendingStakeMutex.Lock()
	pending := pendingStakeEntriesLocked(through)
	if len(pending) == 0 {
		instance.pendingStakeMutex.Unlock()
		return 0, nil
	}
	removed := make(map[uint64][]accountsdb.StakeIndexEntry)
	for slot, entries := range instance.pendingStakeBySlot {
		if slot <= through {
			removed[slot] = entries
			delete(instance.pendingStakeBySlot, slot)
		}
	}
	instance.cachedStakeEntries = nil // file is changing, invalidate cache
	instance.pendingStakeMutex.Unlock()
	restore := func() {
		instance.pendingStakeMutex.Lock()
		defer instance.pendingStakeMutex.Unlock()
		if instance.pendingStakeBySlot == nil {
			instance.pendingStakeBySlot = make(map[uint64][]accountsdb.StakeIndexEntry)
		}
		for slot, entries := range removed {
			// A retry may duplicate a partially appended record. The index format is
			// explicitly last-wins, so retaining the full retry set is safe.
			instance.pendingStakeBySlot[slot] = append(entries, instance.pendingStakeBySlot[slot]...)
		}
	}
	// Append to index file (don't hold lock during I/O)
	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	f, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		restore()
		return 0, fmt.Errorf("opening stake pubkey index for append: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()

	// If file is empty/new, write header first to avoid a headerless file
	info, err := f.Stat()
	if err != nil {
		restore()
		return 0, fmt.Errorf("stat stake pubkey index: %w", err)
	}
	if info.Size() == 0 {
		var header [8]byte
		copy(header[0:4], accountsdb.StakeIndexMagic[:])
		binary.LittleEndian.PutUint32(header[4:8], accountsdb.StakeIndexVersion)
		if _, err := f.Write(header[:]); err != nil {
			restore()
			return 0, fmt.Errorf("writing stake index header: %w", err)
		}
	}

	var record [accountsdb.StakeIndexRecordSize]byte
	for _, e := range pending {
		copy(record[0:32], e.Pubkey[:])
		binary.LittleEndian.PutUint64(record[32:40], e.FileId)
		binary.LittleEndian.PutUint64(record[40:48], e.Offset)
		if _, err := f.Write(record[:]); err != nil {
			restore()
			return 0, fmt.Errorf("writing stake index entry: %w", err)
		}
	}

	// Ensure data is flushed to disk before returning.
	// This is critical: the index must be at least as current as the state file.
	if err := f.Sync(); err != nil {
		restore()
		return 0, fmt.Errorf("syncing stake pubkey index: %w", err)
	}
	if err := f.Close(); err != nil {
		restore()
		return 0, fmt.Errorf("closing stake pubkey index: %w", err)
	}
	closed = true

	instance.pendingStakeMutex.Lock()
	instance.entriesFlushedSinceCompact += len(pending)
	instance.pendingStakeMutex.Unlock()

	return len(pending), nil
}

// DropPendingStakePubkeysFrom discards speculative index entries at and above
// fromSlot while preserving entries belonging to an earlier surviving branch.
func DropPendingStakePubkeysFrom(fromSlot uint64) int {
	instance.pendingStakeMutex.Lock()
	defer instance.pendingStakeMutex.Unlock()
	dropped := 0
	for slot, entries := range instance.pendingStakeBySlot {
		if slot >= fromSlot {
			dropped += len(entries)
			delete(instance.pendingStakeBySlot, slot)
		}
	}
	return dropped
}

// compactThreshold is the minimum number of appended entries before compaction triggers.
// At ~1 new stake account per block × 432k blocks/epoch ≈ a few thousand new entries max.
// 1000 keeps the file clean without rewriting 24MB every boundary when only a handful changed.
const compactThreshold = 1000

// CompactStakePubkeyIndex atomically rewrites the index from its durable,
// deduplicated view. It only runs after compactThreshold appended entries.
func CompactStakePubkeyIndex(accountsDbDir string) error {
	instance.pendingStakeMutex.Lock()
	flushed := instance.entriesFlushedSinceCompact
	instance.pendingStakeMutex.Unlock()

	if flushed < compactThreshold {
		return nil // Not enough new entries to justify rewrite
	}
	instance.stakeIndexIOMutex.Lock()
	defer instance.stakeIndexIOMutex.Unlock()

	// Recheck after acquiring the I/O lock; another compactor may have consumed
	// the threshold while this call was waiting.
	instance.pendingStakeMutex.Lock()
	flushed = instance.entriesFlushedSinceCompact
	cached := append([]accountsdb.StakeIndexEntry(nil), instance.cachedStakeEntries...)
	instance.pendingStakeMutex.Unlock()
	if flushed < compactThreshold {
		return nil
	}

	if len(cached) == 0 {
		var err error
		cached, err = readDurableStakePubkeyIndex(accountsDbDir)
		if err != nil {
			return err
		}
		if len(cached) == 0 {
			return nil
		}
	}

	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	if err := accountsdb.WriteStakePubkeyIndex(indexPath, cached); err != nil {
		return fmt.Errorf("compacting stake pubkey index: %w", err)
	}

	instance.pendingStakeMutex.Lock()
	instance.cachedStakeEntries = cached
	instance.entriesFlushedSinceCompact = 0
	instance.pendingStakeMutex.Unlock()

	mlog.Log.Infof("compacted stake pubkey index: %d entries", len(cached))
	return nil
}

// LoadStakePubkeyIndex reads the stake pubkey index file, auto-detecting format.
// Returns deduplicated entries sorted by (FileId, Offset) for sequential I/O.
// Results are cached after first load; every call returns a detached slice.
// Legacy format: 32-byte pubkeys with no location hints (FileId=0, Offset=0).
// Current format: 8-byte header ("STKI" + version) + 48-byte records (pubkey + fileId + offset).
func LoadStakePubkeyIndex(accountsDbDir string) ([]accountsdb.StakeIndexEntry, error) {
	instance.pendingStakeMutex.Lock()
	if instance.cachedStakeEntries != nil {
		cached := append([]accountsdb.StakeIndexEntry(nil), instance.cachedStakeEntries...)
		pending := pendingStakeEntriesLocked(math.MaxUint64)
		instance.pendingStakeMutex.Unlock()
		return dedupeAndSortStakeEntries(append(cached, pending...)), nil
	}
	instance.pendingStakeMutex.Unlock()
	instance.stakeIndexIOMutex.Lock()
	defer instance.stakeIndexIOMutex.Unlock()

	// Another reader or compactor may have populated the cache while this call
	// waited for the file lock.
	instance.pendingStakeMutex.Lock()
	if instance.cachedStakeEntries != nil {
		cached := append([]accountsdb.StakeIndexEntry(nil), instance.cachedStakeEntries...)
		pending := pendingStakeEntriesLocked(math.MaxUint64)
		instance.pendingStakeMutex.Unlock()
		return dedupeAndSortStakeEntries(append(cached, pending...)), nil
	}
	instance.pendingStakeMutex.Unlock()

	deduped, err := readDurableStakePubkeyIndex(accountsDbDir)
	if err != nil {
		return nil, err
	}
	if len(deduped) == 0 {
		return nil, fmt.Errorf("stake pubkey index file is empty (0 entries) - indicates corrupt or incomplete AccountsDB")
	}

	// Cache only the durable file view. Speculative additions are merged into a
	// detached result on every call so branch unwind never contaminates the cache.
	instance.pendingStakeMutex.Lock()
	instance.cachedStakeEntries = deduped
	pending := pendingStakeEntriesLocked(math.MaxUint64)
	instance.pendingStakeMutex.Unlock()

	return dedupeAndSortStakeEntries(append(append([]accountsdb.StakeIndexEntry(nil), deduped...), pending...)), nil
}

func readDurableStakePubkeyIndex(accountsDbDir string) ([]accountsdb.StakeIndexEntry, error) {
	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}

	var entries []accountsdb.StakeIndexEntry

	// Detect format: current format starts with "STKI" magic
	if len(data) >= 8 && string(data[0:4]) == "STKI" {
		if version := binary.LittleEndian.Uint32(data[4:8]); version != accountsdb.StakeIndexVersion {
			return nil, fmt.Errorf("stake pubkey index: unsupported version %d", version)
		}
		if remainder := (len(data) - 8) % accountsdb.StakeIndexRecordSize; remainder != 0 {
			// Append happens before the AccountsDB fold commit. Therefore an
			// incomplete trailing record can only describe an undecided fold and is
			// safe to discard. Complete records remain as a harmless index superset.
			completeLen := len(data) - remainder
			if err := truncateInterruptedStakeIndexAppend(indexPath, int64(completeLen)); err != nil {
				return nil, err
			}
			mlog.Log.Warnf("stake pubkey index: discarded %d-byte interrupted append tail", remainder)
			data = data[:completeLen]
		}
		entries, err = loadStakePubkeyIndexCurrent(data)
	} else {
		entries, err = loadStakePubkeyIndexLegacy(data)
	}
	if err != nil {
		return nil, err
	}

	return dedupeAndSortStakeEntries(entries), nil
}

func truncateInterruptedStakeIndexAppend(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening stake pubkey index to repair interrupted append: %w", err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("truncating interrupted stake pubkey index append: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing repaired stake pubkey index: %w", err)
	}
	return nil
}

func dedupeAndSortStakeEntries(entries []accountsdb.StakeIndexEntry) []accountsdb.StakeIndexEntry {
	// Deduplicate by pubkey (keep last occurrence = freshest location hint).
	seen := make(map[solana.PublicKey]int, len(entries))
	deduped := make([]accountsdb.StakeIndexEntry, 0, len(entries))
	for _, e := range entries {
		if idx, exists := seen[e.Pubkey]; exists {
			deduped[idx] = e // overwrite with newer entry
		} else {
			seen[e.Pubkey] = len(deduped)
			deduped = append(deduped, e)
		}
	}

	// Sort by (FileId, Offset) for sequential appendvec I/O
	slices.SortFunc(deduped, func(a, b accountsdb.StakeIndexEntry) int {
		if a.FileId != b.FileId {
			if a.FileId < b.FileId {
				return -1
			}
			return 1
		}
		if a.Offset < b.Offset {
			return -1
		}
		if a.Offset > b.Offset {
			return 1
		}
		return 0
	})

	return deduped
}

func loadStakePubkeyIndexLegacy(data []byte) ([]accountsdb.StakeIndexEntry, error) {
	if len(data)%32 != 0 {
		return nil, fmt.Errorf("stake pubkey index V1 corrupt: length %d not multiple of 32", len(data))
	}
	count := len(data) / 32
	entries := make([]accountsdb.StakeIndexEntry, count)
	for i := 0; i < count; i++ {
		copy(entries[i].Pubkey[:], data[i*32:(i+1)*32])
		// FileId=0, Offset=0 — no location hint, sort is effectively no-op
	}
	return entries, nil
}

func loadStakePubkeyIndexCurrent(data []byte) ([]accountsdb.StakeIndexEntry, error) {
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != accountsdb.StakeIndexVersion {
		return nil, fmt.Errorf("stake pubkey index: unsupported version %d", version)
	}
	body := data[8:]
	if len(body)%accountsdb.StakeIndexRecordSize != 0 {
		return nil, fmt.Errorf("stake pubkey index corrupt: body length %d not multiple of %d", len(body), accountsdb.StakeIndexRecordSize)
	}
	count := len(body) / accountsdb.StakeIndexRecordSize
	entries := make([]accountsdb.StakeIndexEntry, count)
	for i := 0; i < count; i++ {
		base := i * accountsdb.StakeIndexRecordSize
		copy(entries[i].Pubkey[:], body[base:base+32])
		entries[i].FileId = binary.LittleEndian.Uint64(body[base+32 : base+40])
		entries[i].Offset = binary.LittleEndian.Uint64(body[base+40 : base+48])
	}
	return entries, nil
}

// StreamStakeAccounts iterates all stake accounts from the pubkey index,
// calling fn for each valid delegation. Returns count of processed accounts.
// This streams directly from AccountsDB without building a full stake cache.
// Entries are sorted by (FileId, Offset) and batched by FileId for sequential I/O.
func StreamStakeAccounts(
	acctsDb *accountsdb.AccountsDb,
	slot uint64,
	fn func(pubkey solana.PublicKey, delegation *sealevel.Delegation, creditsObserved uint64),
) (int, error) {
	// Load stake entries from index file (already sorted by FileId, Offset)
	acctsDbDir := filepath.Join(acctsDb.AcctsDir, "..")
	stakeEntries, err := LoadStakePubkeyIndex(acctsDbDir)
	if err != nil {
		return 0, fmt.Errorf("loading stake pubkey index: %w", err)
	}

	var processedCount atomic.Int64
	var getAccountErrors atomic.Int64
	var unmarshalErrors atomic.Int64
	var statusNotStake atomic.Int64
	var wg sync.WaitGroup

	totalEntries := len(stakeEntries)

	// Batched worker pool for parallel processing
	const maxBatchSize = 2000
	workerPool, err := ants.NewPoolWithFunc(runtime.NumCPU()*2, func(i interface{}) {
		defer wg.Done()

		batch := i.([]accountsdb.StakeIndexEntry)

		for _, entry := range batch {
			// Read stake account from AccountsDB
			stakeAcct, err := acctsDb.GetAccount(slot, entry.Pubkey)
			if err != nil {
				getAccountErrors.Add(1)
				continue // Account not found or closed
			}

			stakeState, err := sealevel.UnmarshalStakeState(stakeAcct.Data)
			if err != nil {
				unmarshalErrors.Add(1)
				continue // Invalid stake state
			}

			// Only process delegated stake accounts (status must be "Stake")
			if stakeState.Status != sealevel.StakeStateV2StatusStake {
				statusNotStake.Add(1)
				continue
			}

			// Call the callback with delegation data
			delegation := &stakeState.Stake.Stake.Delegation
			creditsObserved := stakeState.Stake.Stake.CreditsObserved
			fn(entry.Pubkey, delegation, creditsObserved)
			processedCount.Add(1)
		}
	})
	if err != nil {
		return 0, fmt.Errorf("creating worker pool: %w", err)
	}
	defer workerPool.Release()

	// Submit FileId-aligned batches for sequential appendvec I/O
	var invokeErr error
	batchStart := 0
	for i := 1; i <= len(stakeEntries); i++ {
		flush := i == len(stakeEntries) ||
			stakeEntries[i].FileId != stakeEntries[batchStart].FileId ||
			i-batchStart >= maxBatchSize
		if flush && i > batchStart {
			wg.Add(1)
			if err := workerPool.Invoke(stakeEntries[batchStart:i]); err != nil {
				wg.Done()
				invokeErr = fmt.Errorf("worker pool invoke failed: %w", err)
				break
			}
			batchStart = i
		}
	}

	// Always wait for all queued workers to finish before returning
	wg.Wait()

	if invokeErr != nil {
		return 0, invokeErr
	}

	// Log diagnostic counters for debugging stake account streaming
	mlog.Log.Infof("StreamStakeAccounts: totalEntries=%d processed=%d getAccountErrors=%d unmarshalErrors=%d statusNotStake=%d",
		totalEntries, processedCount.Load(), getAccountErrors.Load(), unmarshalErrors.Load(), statusNotStake.Load())

	return int(processedCount.Load()), nil
}
