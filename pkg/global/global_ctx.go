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
	pendingStakeBySlot         map[uint64][]accountsdb.StakeIndexEntry // New stake entries keyed by the slot that created them; flushed to the index file only when that slot FOLDS (branch-safe: unwound slots drop their entries), merged into stake scans from RAM meanwhile
	cachedStakeEntries         []accountsdb.StakeIndexEntry            // Parsed+sorted index, populated on first load
	entriesFlushedSinceCompact int                                     // Appended entries since last compaction
	voteCache                  map[solana.PublicKey]*sealevel.VoteStateVersions
	epochVoteStateSnapshots    map[uint64]map[solana.PublicKey]*sealevel.VoteStateVersions
	epochStakes                *epochstakes.EpochStakesCache
	slotsConfirmed             map[uint64]struct{}
	leaderSchedule             *leaderschedule.LeaderSchedule // compatibility schedule installed by SetLeaderSchedule
	leaderSchedules            map[uint64]*leaderschedule.LeaderSchedule
	calcUnixTimeForClockSysvar bool
	manageLeaderSchedule       bool
	pendingStakeMutex          sync.Mutex // Protects pendingNewStakePubkeys
	voteCacheMutex             sync.RWMutex
	slotsConfirmedMutex        sync.Mutex
	leaderScheduleMutex        sync.RWMutex
	mu                         sync.Mutex
}

var instance = GlobalCtx{epochStakes: epochstakes.NewEpochStakesCache()}

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

func IncrTransactionCount(num uint64) {
	instance.IncrTransactionCount(num)
}

func SetTransactionCount(num uint64) {
	instance.SetTransactionCount(num)
}

// EnqueuePendingStakePubkey records a stake pubkey created/modified while
// executing `slot`, for later append to the index file. Entries stay in RAM,
// keyed by slot, until that slot FOLDS to durable storage — so a wrong-fork
// block's entries are dropped by the unwind instead of leaking into the file
// — and are merged into stake scans from RAM in the meantime. Deduplication
// happens at scan/load time (last occurrence wins).
func EnqueuePendingStakePubkey(slot uint64, pubkey solana.PublicKey) {
	instance.pendingStakeMutex.Lock()
	defer instance.pendingStakeMutex.Unlock()
	if instance.pendingStakeBySlot == nil {
		instance.pendingStakeBySlot = make(map[uint64][]accountsdb.StakeIndexEntry)
	}
	instance.pendingStakeBySlot[slot] = append(instance.pendingStakeBySlot[slot], accountsdb.StakeIndexEntry{Pubkey: pubkey})
}

// PendingStakeEntriesSnapshot returns a copy of all not-yet-flushed stake
// entries (any slot). Stake scans merge these with the file-backed index so
// the index file itself only ever needs entries for FOLDED slots.
func PendingStakeEntriesSnapshot() []accountsdb.StakeIndexEntry {
	instance.pendingStakeMutex.Lock()
	defer instance.pendingStakeMutex.Unlock()
	var out []accountsdb.StakeIndexEntry
	for _, entries := range instance.pendingStakeBySlot {
		out = append(out, entries...)
	}
	return out
}

// DropPendingStakePubkeysFrom discards pending entries for slots >= fromSlot.
// Called by the fork-switch unwind so wrong-fork stake entries never reach the
// durable index. Returns the number of entries dropped.
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

func PutEpochStakesEntry(epoch uint64, pubkey solana.PublicKey, stake uint64, voteAcct *epochstakes.VoteAccount) {
	instance.epochStakes.PutEntry(epoch, pubkey, stake, voteAcct)
}

// PutEpochStakes atomically replaces all stake material for epoch.
func PutEpochStakes(epoch uint64, stakes map[solana.PublicKey]uint64, voteAccts map[solana.PublicKey]*epochstakes.VoteAccount, totalStake uint64) uint64 {
	return instance.epochStakes.PutEpoch(epoch, stakes, voteAccts, totalStake)
}

// EpochStakesSnapshot returns one coherent, immutable view of epoch.
func EpochStakesSnapshot(epoch uint64) (epochstakes.Snapshot, bool) {
	return instance.epochStakes.Snapshot(epoch)
}

// EpochStakes returns the immutable, cache-owned epoch view. Callers must not
// mutate it. Epoch publication is copy-on-write, so the view remains valid
// across later updates without copying hundreds of thousands of entries on
// per-slot read paths.
func EpochStakes(epoch uint64) map[solana.PublicKey]uint64 {
	snapshot, ok := instance.epochStakes.Snapshot(epoch)
	if !ok {
		return nil
	}
	return snapshot.Stakes
}

func HasEpochStakes(epoch uint64) bool {
	return instance.epochStakes.HasEpochStakes(epoch)
}

func PutEpochTotalStake(epoch uint64, totalStake uint64) {
	instance.epochStakes.PutTotalEpochStake(epoch, totalStake)
}

func EpochTotalStake(epoch uint64) uint64 {
	return instance.epochStakes.TotalStake(epoch)
}

func EpochStakesGeneration(epoch uint64) uint64 {
	return instance.epochStakes.Generation(epoch)
}

func StakeForVoteAcct(epoch uint64, voteAcct solana.PublicKey) uint64 {
	snapshot, ok := instance.epochStakes.Snapshot(epoch)
	if !ok {
		return 0
	}
	return snapshot.Stakes[voteAcct]
}

// EpochStakesVoteAccts follows the same immutable-view contract as
// EpochStakes.
func EpochStakesVoteAccts(epoch uint64) map[solana.PublicKey]*epochstakes.VoteAccount {
	snapshot, ok := instance.epochStakes.Snapshot(epoch)
	if !ok {
		return nil
	}
	return snapshot.VoteAccounts
}

// ClearEpochStakes removes all stakes for a specific epoch.
// Used on resume to force rebuild from AccountsDB.
func ClearEpochStakes(epoch uint64) {
	instance.epochStakes.ClearEpochStakes(epoch)
}

// SerializeEpochStakes serializes the stakes for a single epoch to JSON.
func SerializeEpochStakes(epoch uint64) ([]byte, error) {
	return instance.epochStakes.SerializeEpoch(epoch)
}

// DeserializeAndLoadEpochStakes deserializes and loads epoch stakes from JSON.
// Returns the epoch number that was loaded.
func DeserializeAndLoadEpochStakes(data []byte) (uint64, error) {
	return instance.epochStakes.DeserializeAndLoadEpoch(data)
}

// GetAllCachedEpochs returns all epochs currently in the epoch stakes cache.
func GetAllCachedEpochs() []uint64 {
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
	instance.leaderScheduleMutex.Lock()
	defer instance.leaderScheduleMutex.Unlock()

	instance.leaderSchedule = ls
	instance.leaderSchedules = nil
}

// SetLeaderScheduleForEpoch installs one epoch's schedule without discarding
// schedules for other epochs. Turbine can receive current live shreds while
// replay repair is still consuming an older epoch, so both must remain
// verifiable concurrently.
func SetLeaderScheduleForEpoch(epoch uint64, ls *leaderschedule.LeaderSchedule) {
	instance.leaderScheduleMutex.Lock()
	defer instance.leaderScheduleMutex.Unlock()

	// Epoch-aware production callers supersede the legacy single-schedule API.
	instance.leaderSchedule = nil
	if ls == nil {
		delete(instance.leaderSchedules, epoch)
		return
	}
	if instance.leaderSchedules == nil {
		instance.leaderSchedules = make(map[uint64]*leaderschedule.LeaderSchedule)
	}
	instance.leaderSchedules[epoch] = ls
}

func LeaderForSlot(slot uint64) (solana.PublicKey, bool) {
	instance.leaderScheduleMutex.RLock()
	defer instance.leaderScheduleMutex.RUnlock()

	if instance.leaderSchedule != nil {
		return instance.leaderSchedule.LeaderForSlot(slot)
	}
	for _, schedule := range instance.leaderSchedules {
		if leader, ok := schedule.LeaderForSlot(slot); ok {
			return leader, true
		}
	}
	return solana.PublicKey{}, false
}

// NextSlotForLeader returns the earliest scheduled slot at or after from
// for identity across installed epoch schedules.
func NextSlotForLeader(identity solana.PublicKey, from uint64) (uint64, bool) {
	instance.leaderScheduleMutex.RLock()
	defer instance.leaderScheduleMutex.RUnlock()

	if instance.leaderSchedule != nil {
		return instance.leaderSchedule.NextSlotForLeader(identity, from)
	}
	var best uint64
	found := false
	for _, schedule := range instance.leaderSchedules {
		slot, ok := schedule.NextSlotForLeader(identity, from)
		if !ok {
			continue
		}
		if !found || slot < best {
			best = slot
			found = true
		}
	}
	return best, found
}

// LeaderForSlotWithVoteAccount returns the coherent node and vote-account pair
// sampled for slot by a locally built vote-keyed schedule. Compatibility
// schedules built from node-keyed RPC data return false.
func LeaderForSlotWithVoteAccount(slot uint64) (solana.PublicKey, solana.PublicKey, bool) {
	instance.leaderScheduleMutex.RLock()
	defer instance.leaderScheduleMutex.RUnlock()

	if instance.leaderSchedule != nil {
		return instance.leaderSchedule.LeaderForSlotWithVoteAccount(slot)
	}
	for _, schedule := range instance.leaderSchedules {
		if _, ok := schedule.LeaderForSlot(slot); !ok {
			continue
		}
		return schedule.LeaderForSlotWithVoteAccount(slot)
	}
	return solana.PublicKey{}, solana.PublicKey{}, false
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

// SetTransactionCount overwrites the running transaction count — used to seed it
// from a resume checkpoint and to restore it after an in-loop fork-switch unwind
// (so a discarded fork's transactions do not linger in the count).
func (globctx *GlobalCtx) SetTransactionCount(num uint64) {
	globctx.mu.Lock()
	defer globctx.mu.Unlock()
	globctx.transactionCount = num
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

// FlushPendingStakePubkeys appends ALL pending stake entries to the index
// file regardless of slot. Only correct where forks are impossible (legacy /
// verify replay modes); the rooted-durable live path must use
// FlushPendingStakePubkeysThrough at fold time instead.
func FlushPendingStakePubkeys(accountsDbDir string) (int, error) {
	return FlushPendingStakePubkeysThrough(accountsDbDir, math.MaxUint64)
}

// FlushPendingStakePubkeysThrough appends pending stake entries for slots <=
// through to the index file and fsyncs. Called at FOLD time, BEFORE the batch
// commit that makes those slots durable: on the index side a crash can then
// only leave a harmless SUPERSET (re-executed slots re-enqueue; dedup at scan
// time), never a subset — the index feeding epoch-stakes enumeration must
// never miss a folded slot's stake accounts. Entries for slots > through stay
// in RAM (merged into scans) until their own fold or unwind decides them.
// Returns the number of entries flushed.
func FlushPendingStakePubkeysThrough(accountsDbDir string, through uint64) (int, error) {
	instance.pendingStakeMutex.Lock()
	var pending []accountsdb.StakeIndexEntry
	for slot, entries := range instance.pendingStakeBySlot {
		if slot <= through {
			pending = append(pending, entries...)
			delete(instance.pendingStakeBySlot, slot)
		}
	}
	if len(pending) == 0 {
		instance.pendingStakeMutex.Unlock()
		return 0, nil
	}
	instance.cachedStakeEntries = nil // file is changing, invalidate cache
	instance.pendingStakeMutex.Unlock()

	// Append to index file (don't hold lock during I/O)
	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	f, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("opening stake pubkey index for append: %w", err)
	}
	defer f.Close()

	// If file is empty/new, write header first to avoid a headerless file
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat stake pubkey index: %w", err)
	}
	if info.Size() == 0 {
		var header [8]byte
		copy(header[0:4], accountsdb.StakeIndexMagic[:])
		binary.LittleEndian.PutUint32(header[4:8], accountsdb.StakeIndexVersion)
		if _, err := f.Write(header[:]); err != nil {
			return 0, fmt.Errorf("writing stake index header: %w", err)
		}
	}

	var record [accountsdb.StakeIndexRecordSize]byte
	for _, e := range pending {
		copy(record[0:32], e.Pubkey[:])
		binary.LittleEndian.PutUint64(record[32:40], e.FileId)
		binary.LittleEndian.PutUint64(record[40:48], e.Offset)
		if _, err := f.Write(record[:]); err != nil {
			return 0, fmt.Errorf("writing stake index entry: %w", err)
		}
	}

	// Ensure data is flushed to disk before returning.
	// This is critical: the index must be at least as current as the state file.
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("syncing stake pubkey index: %w", err)
	}

	instance.pendingStakeMutex.Lock()
	instance.entriesFlushedSinceCompact += len(pending)
	instance.pendingStakeMutex.Unlock()

	return len(pending), nil
}

// ClearPendingStakePubkeys discards any pending stake pubkeys without writing them.
// Used for rollback on failed block replay.
func ClearPendingStakePubkeys() {
	instance.pendingStakeMutex.Lock()
	defer instance.pendingStakeMutex.Unlock()
	instance.pendingStakeBySlot = nil
}

// compactThreshold is the minimum number of appended entries before compaction triggers.
// At ~1 new stake account per block × 432k blocks/epoch ≈ a few thousand new entries max.
// 1000 keeps the file clean without rewriting 24MB every boundary when only a handful changed.
const compactThreshold = 1000

// CompactStakePubkeyIndex rewrites the index file from the cached deduplicated entries.
// Only triggers when at least compactThreshold entries have been appended since last compaction.
// Should be called at epoch boundary when the cache is already populated.
func CompactStakePubkeyIndex(accountsDbDir string) error {
	instance.pendingStakeMutex.Lock()
	flushed := instance.entriesFlushedSinceCompact
	cached := instance.cachedStakeEntries
	instance.pendingStakeMutex.Unlock()

	if flushed < compactThreshold {
		return nil // Not enough new entries to justify rewrite
	}

	if cached == nil || len(cached) == 0 {
		return nil // Nothing to compact
	}

	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	if err := accountsdb.WriteStakePubkeyIndex(indexPath, cached); err != nil {
		return fmt.Errorf("compacting stake pubkey index: %w", err)
	}

	instance.pendingStakeMutex.Lock()
	instance.entriesFlushedSinceCompact = 0
	instance.pendingStakeMutex.Unlock()

	mlog.Log.Infof("compacted stake pubkey index: %d entries", len(cached))
	return nil
}

// LoadStakePubkeyIndex reads the stake pubkey index file, auto-detecting format.
// Returns deduplicated entries sorted by (FileId, Offset) for sequential I/O.
// Results are cached after first load; subsequent calls return the cached slice.
// IMPORTANT: The returned slice is shared — callers must NOT mutate it.
// Legacy format: 32-byte pubkeys with no location hints (FileId=0, Offset=0).
// Current format: 8-byte header ("STKI" + version) + 48-byte records (pubkey + fileId + offset).
func LoadStakePubkeyIndex(accountsDbDir string) ([]accountsdb.StakeIndexEntry, error) {
	instance.pendingStakeMutex.Lock()
	if instance.cachedStakeEntries != nil {
		cached := instance.cachedStakeEntries
		instance.pendingStakeMutex.Unlock()
		return cached, nil
	}
	instance.pendingStakeMutex.Unlock()

	indexPath := filepath.Join(accountsDbDir, StakePubkeyIndexFileName)
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}

	var entries []accountsdb.StakeIndexEntry

	// Detect format: current format starts with "STKI" magic
	if len(data) >= 8 && string(data[0:4]) == "STKI" {
		entries, err = loadStakePubkeyIndexCurrent(data)
	} else {
		entries, err = loadStakePubkeyIndexLegacy(data)
	}
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("stake pubkey index file is empty (0 entries) - indicates corrupt or incomplete AccountsDB")
	}

	// Deduplicate by pubkey (keep last occurrence = freshest location hint)
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

	// Cache for subsequent calls
	instance.pendingStakeMutex.Lock()
	instance.cachedStakeEntries = deduped
	instance.pendingStakeMutex.Unlock()

	return deduped, nil
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
	// Merge stake accounts created by not-yet-folded slots: their entries live
	// only in RAM (flushed to the file at fold time; dropped on unwind), so the
	// scan must union them in for completeness at epoch boundaries. File
	// entries win on duplicates — they carry location hints; the entry is just
	// a pointer, the delegation itself is read from the account.
	if pending := PendingStakeEntriesSnapshot(); len(pending) > 0 {
		inFile := make(map[solana.PublicKey]struct{}, len(stakeEntries))
		for _, e := range stakeEntries {
			inFile[e.Pubkey] = struct{}{}
		}
		merged := stakeEntries
		appended := false
		for _, e := range pending {
			if _, dup := inFile[e.Pubkey]; dup {
				continue
			}
			if !appended {
				// Copy-on-write: LoadStakePubkeyIndex's slice is shared/cached.
				merged = append(append([]accountsdb.StakeIndexEntry(nil), stakeEntries...), e)
				appended = true
			} else {
				merged = append(merged, e)
			}
			inFile[e.Pubkey] = struct{}{}
		}
		stakeEntries = merged
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
