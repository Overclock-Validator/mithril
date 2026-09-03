package accountsdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
)

var systemProgramAddr [32]byte

// appendVecReadChunkSize keeps a large appendvec from being serialized behind
// one worker. Chunks share one descriptor and are offset-sorted. Within each
// chunk, nearby headers and account data are read as bounded ranges.
const (
	appendVecReadChunkSize           = 64
	appendVecHeaderCoalesceGapBytes  = int64(4 << 10)
	appendVecHeaderReadRangeMaxBytes = int64(256 << 10)
	appendVecDataCoalesceGapBytes    = int64(4 << 10)
	appendVecDataReadRangeMaxBytes   = int64(1 << 20)
	appendVecReadMaxWorkers          = 64
	batchDedupeLinearScanMax         = 16
	batchDedupeContextCheckEvery     = 4096
)

type batchReadTestHooks struct {
	beforeCacheAdmission func(solana.PublicKey)
	afterAppendVecPread  func()
}

type batchAccountLocation struct {
	outputIdx int
	pubkey    solana.PublicKey
	entry     AccountIndexEntry
	source    accountIndexSource
	tombstone bool
	// logicalCopies is the number of positions in the caller's request that
	// refer to this stable-deduplicated key. Zero means one for helpers and
	// focused tests that manufacture locations directly.
	logicalCopies uint64
}

type appendVecID struct {
	slot   uint64
	fileID uint64
}

type appendVecReadChunk struct {
	group     *appendVecReadGroup
	locations []batchAccountLocation
}

// appendVecReadGroup owns the one descriptor shared by all read chunks for an
// appendvec. os.File.ReadAt is safe for concurrent use, so retaining chunked
// scheduling preserves parallel I/O without paying open/close per chunk.
type appendVecReadGroup struct {
	id appendVecID

	openOnce  sync.Once
	closeOnce sync.Once
	remaining atomic.Int64
	file      *os.File
	openErr   error
	fileSize  int64
}

func (group *appendVecReadGroup) open(path string) (*os.File, error) {
	group.openOnce.Do(func() {
		var info os.FileInfo
		group.file, info, group.openErr = openRegularFileNoFollow(path)
		if group.openErr != nil {
			group.openErr = &os.PathError{Op: "open appendvec", Path: path, Err: group.openErr}
			return
		}
		group.fileSize = info.Size()
	})
	return group.file, group.openErr
}

func (group *appendVecReadGroup) size(path string) (int64, error) {
	_, err := group.open(path)
	if err != nil {
		return 0, err
	}
	return group.fileSize, nil
}

func (group *appendVecReadGroup) chunkDone() {
	if group.remaining.Add(-1) == 0 {
		group.close()
	}
}

func (group *appendVecReadGroup) close() {
	group.closeOnce.Do(func() {
		if group.file != nil {
			_ = group.file.Close()
		}
	})
}

// BatchReadStats is one durable batch read's wall-time and work breakdown.
// Allocation counters describe logical objects created directly by this path;
// they deliberately exclude OS/runtime internals.
type BatchReadStats struct {
	RequestedKeys uint64
	DurableKeys   uint64
	// UniqueKeys and DuplicateKeys expose the amount of stable request
	// deduplication. DurableKeys remains a logical/caller-position count for
	// compatibility; UniqueDurableKeys is the actual AccountsDB work set.
	UniqueKeys        uint64
	DuplicateKeys     uint64
	UniqueDurableKeys uint64

	WorkingSetHits       uint64
	InProgressHits       uint64
	PendingFoldHits      uint64
	CacheHits            uint64
	IndexHits            uint64
	IndexMisses          uint64
	DeltaIndexProbes     uint64
	DeltaIndexHits       uint64
	DeltaIndexTombstones uint64
	BaseIndexProbes      uint64
	BaseIndexCandidates  uint64
	BaseIndexHits        uint64
	// BaseIndexFalsePositives are StreamHash candidates rejected by the
	// authoritative full-pubkey appendvec check (including stale base paths
	// already relocated into the delta).
	BaseIndexFalsePositives uint64
	UniqueAppendVecs        uint64
	AppendVecChunks         uint64
	AppendVecAccounts       uint64
	OpenFailures            uint64
	ReadFailures            uint64
	RetryAccounts           uint64

	// AppendVecRequestedBytes is the sum of header and data bytes needed for
	// unique candidates. AppendVecPhysicalReadBytes is the number of bytes
	// returned into bounded coalesced read buffers (including gap over-read).
	// Their ratio is the userspace read-amplification input; it does not claim
	// to measure device-sector traffic below the page cache.
	AppendVecReadRanges        uint64
	AppendVecPreadCalls        uint64
	AppendVecRequestedBytes    uint64
	AppendVecPhysicalReadBytes uint64

	CommonCacheAdmissions        uint64
	CommonCacheAdmissionsSkipped uint64
	VoteCacheAdmissions          uint64
	VoteCacheAdmissionsSkipped   uint64
	CachePublicationEpochRejects uint64

	DecodedAccountObjects uint64
	DecodedAccountBytes   uint64
	PlaceholderObjects    uint64

	WorkingSetLookupNanoseconds     uint64
	InProgressNanoseconds           uint64
	AppendVecPinWaitNanoseconds     uint64
	ReadCacheEpochWaitNanoseconds   uint64
	CacheLookupNanoseconds          uint64
	AdmissionFilterNanoseconds      uint64
	IndexLookupNanoseconds          uint64
	ReadPlanningNanoseconds         uint64
	AppendVecReadNanoseconds        uint64
	CachePublicationWaitNanoseconds uint64
	CachePublicationNanoseconds     uint64
}

type batchChunkReadStats struct {
	appendVecAccounts     uint64
	openFailures          uint64
	readFailures          uint64
	indexHits             uint64
	indexMisses           uint64
	baseIndexHits         uint64
	baseFalsePositives    uint64
	decodedAccountObjects uint64
	decodedAccountBytes   uint64
	placeholderObjects    uint64
	readRanges            uint64
	preadCalls            uint64
	requestedBytes        uint64
	physicalReadBytes     uint64
}

func (db *AccountsDb) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out, _, err := db.getAccountsBatchWithStats(ctx, slot, pks)
	return out, err
}

// GetAccountsBatchShared is the immutable-parent fast path consumed by replay.
// AccountsDb read-cache values have always been shared; the distinct method
// makes that ownership explicit and lets an unrooted overlay avoid cloning
// those values before transaction execution copy-on-writes them.
func (db *AccountsDb) GetAccountsBatchShared(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out, _, err := db.getAccountsBatchWithStats(ctx, slot, pks)
	return out, err
}

// GetAccountsBatchSharedWithStats is replay's instrumented immutable-parent
// path. The non-instrumented APIs above share the same implementation.
func (db *AccountsDb) GetAccountsBatchSharedWithStats(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, BatchReadStats, error) {
	return db.getAccountsBatchWithStats(ctx, slot, pks)
}

// GetUniqueAccountsBatchSharedWithStats is replay's immutable-parent fast path
// for a key list whose uniqueness has already been established by the caller.
// It deliberately skips AccountsDB's defensive membership map. Callers that do
// not already own that proof must use GetAccountsBatchSharedWithStats instead.
func (db *AccountsDb) GetUniqueAccountsBatchSharedWithStats(
	ctx context.Context,
	slot uint64,
	pks []solana.PublicKey,
) ([]*accounts.Account, BatchReadStats, error) {
	return db.getAccountsBatchWithStatsMode(ctx, slot, pks, true)
}

type stableBatchRequest struct {
	keys          []solana.PublicKey
	scatter       []int
	logicalCopies []uint64
}

// dedupeBatchRequest retains the first occurrence of every pubkey. When the
// input is already unique, keys aliases pks and the scatter/copy slices are
// nil, keeping the common scatter-free return path cheap.
func dedupeBatchRequest(ctx context.Context, pks []solana.PublicKey) (stableBatchRequest, error) {
	if ctx == nil {
		return stableBatchRequest{}, errors.New("accountsdb: nil batch lookup context")
	}
	if len(pks) < 2 {
		return stableBatchRequest{keys: pks}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return stableBatchRequest{}, err
	}

	// Avoid a map allocation for the common transaction/sysvar-sized request.
	// If this tiny scan finds a duplicate, the regular map pass below remains
	// bounded and establishes all stable first-occurrence indexes.
	if len(pks) <= batchDedupeLinearScanMax {
		allUnique := true
		for i := 1; i < len(pks) && allUnique; i++ {
			for j := 0; j < i; j++ {
				if pks[i] == pks[j] {
					allUnique = false
					break
				}
			}
		}
		if allUnique {
			return stableBatchRequest{keys: pks}, nil
		}
	}

	// Defer the O(n) slices until the first duplicate. An all-unique block still
	// needs the membership map to prove uniqueness, but it need not copy all
	// pubkeys or allocate scatter and fanout arrays that it will never use.
	var unique []solana.PublicKey
	var scatter []int
	var copies []uint64
	positions := make(map[solana.PublicKey]int, len(pks))
	for i, key := range pks {
		if i%batchDedupeContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return stableBatchRequest{}, err
			}
		}
		if uniqueIdx, ok := positions[key]; ok {
			if unique == nil {
				unique = make([]solana.PublicKey, i, len(pks))
				copy(unique, pks[:i])
				scatter = make([]int, len(pks))
				copies = make([]uint64, i, len(pks))
				for previous := 0; previous < i; previous++ {
					scatter[previous] = previous
					copies[previous] = 1
				}
			}
			scatter[i] = uniqueIdx
			copies[uniqueIdx]++
			continue
		}
		uniqueIdx := i
		if unique != nil {
			uniqueIdx = len(unique)
			unique = append(unique, key)
			scatter[i] = uniqueIdx
			copies = append(copies, 1)
		}
		positions[key] = uniqueIdx
	}
	if err := ctx.Err(); err != nil {
		return stableBatchRequest{}, err
	}
	if unique == nil {
		return stableBatchRequest{keys: pks}, nil
	}
	return stableBatchRequest{keys: unique, scatter: scatter, logicalCopies: copies}, nil
}

func (request stableBatchRequest) copiesFor(uniqueIdx int) uint64 {
	if request.logicalCopies == nil {
		return 1
	}
	return request.logicalCopies[uniqueIdx]
}

func (request stableBatchRequest) scatterAccounts(unique []*accounts.Account) []*accounts.Account {
	if request.scatter == nil {
		return unique
	}
	out := make([]*accounts.Account, len(request.scatter))
	for outputIdx, uniqueIdx := range request.scatter {
		out[outputIdx] = unique[uniqueIdx]
	}
	return out
}

func (db *AccountsDb) getAccountsBatchWithStats(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, BatchReadStats, error) {
	return db.getAccountsBatchWithStatsMode(ctx, slot, pks, false)
}

func (db *AccountsDb) getAccountsBatchWithStatsMode(
	ctx context.Context,
	slot uint64,
	pks []solana.PublicKey,
	alreadyUnique bool,
) ([]*accounts.Account, BatchReadStats, error) {
	stats := BatchReadStats{RequestedKeys: uint64(len(pks)), DurableKeys: uint64(len(pks))}
	if len(pks) == 0 {
		stats.UniqueKeys = 0
		stats.UniqueDurableKeys = 0
		return nil, stats, nil
	}
	request := stableBatchRequest{keys: pks}
	var err error
	if alreadyUnique {
		if ctx == nil {
			return nil, stats, errors.New("accountsdb: nil batch lookup context")
		}
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
	} else {
		request, err = dedupeBatchRequest(ctx, pks)
		if err != nil {
			return nil, stats, err
		}
	}
	pks = request.keys
	stats.UniqueKeys = uint64(len(pks))
	stats.UniqueDurableKeys = uint64(len(pks))
	stats.DuplicateKeys = stats.RequestedKeys - stats.UniqueKeys
	phaseStart := time.Now()
	db.appendVecReadMu.RLock()
	stats.AppendVecPinWaitNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())
	appendVecPinned := true
	defer func() {
		if appendVecPinned {
			db.appendVecReadMu.RUnlock()
		}
	}()

	phaseStart = time.Now()
	out := db.getStoreInProgressAccounts(pks)
	stats.InProgressNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())
	cold := make([]int, 0, len(pks))
	var indexSnapshot *MutableAccountIndexSnapshot
	var productionSnapshot *ProductionAccountIndexSnapshot
	var cacheEpoch uint64
	var admission *commonCacheAdmission
	var snapshotSetupNanoseconds uint64
	waitStart := time.Now()
	db.readCacheEpochMu.RLock()
	stats.ReadCacheEpochWaitNanoseconds = uint64(time.Since(waitStart).Nanoseconds())
	phaseStart = time.Now()
	cacheEpoch = db.readCacheEpoch
	admission = db.commonAdmission
	for i, pk := range pks {
		if out[i] != nil {
			stats.InProgressHits += request.copiesFor(i)
			continue
		}
		if acct, ok, pending := db.getCachedAccountLocked(pk); ok {
			if pending {
				stats.PendingFoldHits += request.copiesFor(i)
			} else {
				stats.CacheHits += request.copiesFor(i)
			}
			if acct == nil || acct.Lamports == 0 {
				stats.PlaceholderObjects++
			}
			out[i] = batchAccountOrPlaceholder(pk, acct)
			continue
		}
		cold = append(cold, i)
	}
	stats.CacheLookupNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())
	if len(cold) > 0 && db.ProductionIndex != nil {
		// Capture the exact mutable epoch and its corresponding root generation
		// under the same cache publication epoch.
		snapshotStart := time.Now()
		coldKeys := make([]solana.PublicKey, len(cold))
		for i, idx := range cold {
			coldKeys[i] = pks[idx]
		}
		productionSnapshot, err = db.ProductionIndex.NewSnapshot(coldKeys)
		snapshotSetupNanoseconds = uint64(time.Since(snapshotStart).Nanoseconds())
		if err != nil {
			db.readCacheEpochMu.RUnlock()
			return nil, stats, fmt.Errorf("capture production account-index snapshot: %w", err)
		}
	} else if len(cold) > 0 && db.Index != nil {
		// The cache probes and snapshot share one publication epoch. A fold
		// cannot flip the index and refresh caches between these two views.
		snapshotStart := time.Now()
		indexSnapshot = db.Index.NewSnapshot()
		snapshotSetupNanoseconds = uint64(time.Since(snapshotStart).Nanoseconds())
	}
	db.readCacheEpochMu.RUnlock()
	if len(cold) == 0 {
		return request.scatterAccounts(out), stats, nil
	}
	if !db.hasAccountIndex() {
		for _, idx := range cold {
			out[idx] = missingAccount(pks[idx])
			stats.IndexMisses += request.copiesFor(idx)
			stats.PlaceholderObjects++
		}
		return request.scatterAccounts(out), stats, nil
	}

	phaseStart = time.Now()
	admitCommon := admission.classifyAndObserve(pks, cold)
	stats.AdmissionFilterNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())
	phaseStart = time.Now()
	var locations []batchAccountLocation
	var found []bool
	if productionSnapshot != nil {
		locations, found, err = resolveProductionBatchAccountLocations(ctx, productionSnapshot, pks, cold)
	} else {
		locations, found, err = resolveBatchAccountLocations(ctx, indexSnapshot, pks, cold, out)
	}
	var closeErr error
	if productionSnapshot != nil {
		closeErr = productionSnapshot.Close()
	}
	if indexSnapshot != nil {
		closeErr = indexSnapshot.Close()
	}
	if err != nil {
		return nil, stats, err
	}
	if closeErr != nil {
		return nil, stats, fmt.Errorf("close account index snapshot: %w", closeErr)
	}
	// The exact mutable epoch is now fully materialized in locations/found.
	// Release its read pin before issuing potentially cold immutable mmap probes
	// so a fold publication is never delayed by base-index page faults.
	if productionSnapshot == nil {
		if err := resolveBatchBaseCandidates(ctx, db.BaseIndex, pks, cold, out, locations, found); err != nil {
			return nil, stats, err
		}
	}
	stats.IndexLookupNanoseconds = snapshotSetupNanoseconds + uint64(time.Since(phaseStart).Nanoseconds())

	phaseStart = time.Now()
	planned := locations[:0]
	hasImmutableBase := db.ProductionIndex != nil || db.BaseIndex != nil
	if hasImmutableBase {
		for _, idx := range cold {
			stats.DeltaIndexProbes += request.copiesFor(idx)
		}
	}
	for i, location := range locations {
		uniqueIdx := cold[i]
		logicalCopies := request.copiesFor(uniqueIdx)
		location.logicalCopies = logicalCopies
		if hasImmutableBase {
			switch {
			case location.tombstone:
				stats.DeltaIndexTombstones += logicalCopies
			case location.source == accountIndexSourceDelta:
				stats.DeltaIndexHits += logicalCopies
			default:
				stats.BaseIndexProbes += logicalCopies
				if location.source == accountIndexSourceBase {
					stats.BaseIndexCandidates += logicalCopies
				}
			}
		}
		if !found[i] {
			idx := uniqueIdx
			out[idx] = missingAccount(pks[idx])
			stats.IndexMisses += logicalCopies
			stats.PlaceholderObjects++
			continue
		}
		planned = append(planned, location)
	}
	sort.Slice(planned, func(i, j int) bool {
		left, right := planned[i].entry, planned[j].entry
		if left.Slot != right.Slot {
			return left.Slot < right.Slot
		}
		if left.FileId != right.FileId {
			return left.FileId < right.FileId
		}
		return left.Offset < right.Offset
	})

	chunks := make([]appendVecReadChunk, 0, (len(planned)+appendVecReadChunkSize-1)/appendVecReadChunkSize)
	var groups []*appendVecReadGroup
	for groupStart := 0; groupStart < len(planned); {
		id := appendVecID{slot: planned[groupStart].entry.Slot, fileID: planned[groupStart].entry.FileId}
		groupEnd := groupStart + 1
		for groupEnd < len(planned) &&
			planned[groupEnd].entry.Slot == id.slot &&
			planned[groupEnd].entry.FileId == id.fileID {
			groupEnd++
		}
		group := &appendVecReadGroup{id: id}
		groups = append(groups, group)
		chunkCount := 0
		for start := groupStart; start < groupEnd; start += appendVecReadChunkSize {
			end := min(start+appendVecReadChunkSize, groupEnd)
			chunks = append(chunks, appendVecReadChunk{group: group, locations: planned[start:end]})
			chunkCount++
		}
		group.remaining.Store(int64(chunkCount))
		groupStart = groupEnd
	}
	stats.UniqueAppendVecs = uint64(len(groups))
	stats.AppendVecChunks = uint64(len(chunks))
	chunkStats := make([]batchChunkReadStats, len(chunks))
	decodedForCache := make([]*accounts.Account, len(pks))
	stats.ReadPlanningNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())

	// Chunks for one appendvec share a descriptor and use ReadAt concurrently.
	// The monotonically assigned, group-contiguous jobs bound live descriptors
	// by the worker count, while appendVecReadMu pins every resolved source path
	// until all reads finish.
	phaseStart = time.Now()
	err = runBatchWorkersLimited(ctx, len(chunks), appendVecReadMaxWorkers, func(job int) error {
		chunk := chunks[job]
		group := chunk.group
		defer group.chunkDone()
		jobStats := &chunkStats[job]
		path := filepath.Join(db.AcctsDir, fmt.Sprintf("%d.%d", group.id.slot, group.id.fileID))
		file, openErr := group.open(path)
		if openErr != nil {
			allBaseCandidates := true
			for _, location := range chunk.locations {
				if location.source != accountIndexSourceBase {
					allBaseCandidates = false
					break
				}
			}
			if allBaseCandidates && os.IsNotExist(openErr) {
				retired, markerErr := db.isAppendVecRetired(group.id.slot, group.id.fileID)
				if markerErr != nil {
					return markerErr
				}
				if retired {
					for _, location := range chunk.locations {
						out[location.outputIdx] = missingAccount(location.pubkey)
						jobStats.indexMisses += batchLocationCopies(location)
						jobStats.baseFalsePositives += batchLocationCopies(location)
						jobStats.placeholderObjects++
					}
					return nil
				}
				jobStats.openFailures++
				return fmt.Errorf("accountsdb: active base-index appendvec %s is missing", path)
			}
			jobStats.openFailures++
			return fmt.Errorf("open account appendvec %s: %w", path, openErr)
		}
		fileSize, statErr := group.size(path)
		if statErr != nil {
			jobStats.readFailures++
			return fmt.Errorf("stat account appendvec %s: %w", path, statErr)
		}
		return db.readBatchAppendVecChunk(
			ctx, file, path, fileSize, chunk.locations, out, decodedForCache, jobStats,
		)
	})
	// On cancellation/error, some chunks can remain unstarted and therefore
	// cannot release the group's descriptor themselves.
	for _, group := range groups {
		group.close()
	}
	for _, chunkStat := range chunkStats {
		stats.AppendVecAccounts += chunkStat.appendVecAccounts
		stats.OpenFailures += chunkStat.openFailures
		stats.ReadFailures += chunkStat.readFailures
		stats.IndexHits += chunkStat.indexHits
		stats.IndexMisses += chunkStat.indexMisses
		stats.BaseIndexHits += chunkStat.baseIndexHits
		stats.BaseIndexFalsePositives += chunkStat.baseFalsePositives
		stats.DecodedAccountObjects += chunkStat.decodedAccountObjects
		stats.DecodedAccountBytes += chunkStat.decodedAccountBytes
		stats.PlaceholderObjects += chunkStat.placeholderObjects
		stats.AppendVecReadRanges += chunkStat.readRanges
		stats.AppendVecPreadCalls += chunkStat.preadCalls
		stats.AppendVecRequestedBytes += chunkStat.requestedBytes
		stats.AppendVecPhysicalReadBytes += chunkStat.physicalReadBytes
	}
	stats.AppendVecReadNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())
	if err != nil {
		return nil, stats, err
	}

	// File paths are no longer needed. Let compaction/rewind proceed while the
	// decoded values pass through the epoch-checked selective cache policy.
	db.appendVecReadMu.RUnlock()
	appendVecPinned = false
	phaseStart = time.Now()
	publicationJobs := cold[:0]
	for _, idx := range cold {
		acct := decodedForCache[idx]
		if acct == nil {
			continue
		}
		isVote := solana.PublicKeyFromBytes(acct.Owner[:]) == addresses.VoteProgramAddr
		if !isVote && !admitCommon[idx] {
			stats.CommonCacheAdmissionsSkipped++
			continue
		}
		publicationJobs = append(publicationJobs, idx)
	}
	publicationResults := make([]batchCacheAdmission, len(publicationJobs))
	publicationWaits := make([]uint64, len(publicationJobs))
	err = runBatchWorkers(ctx, len(publicationJobs), func(job int) error {
		idx := publicationJobs[job]
		acct := decodedForCache[idx]
		publicationResults[job], publicationWaits[job] = db.cacheBatchReadAccount(
			pks[idx], acct, admitCommon[idx], cacheEpoch,
		)
		return nil
	})
	for idx, result := range publicationResults {
		stats.CachePublicationWaitNanoseconds += publicationWaits[idx]
		switch result {
		case batchCacheVote:
			stats.VoteCacheAdmissions++
		case batchCacheCommon:
			stats.CommonCacheAdmissions++
		case batchCacheCommonSkipped:
			stats.CommonCacheAdmissionsSkipped++
		case batchCacheVoteSkipped:
			stats.VoteCacheAdmissionsSkipped++
		case batchCacheEpochRejected:
			stats.CachePublicationEpochRejects++
		}
	}
	stats.CachePublicationNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())
	if err != nil {
		return nil, stats, err
	}
	return request.scatterAccounts(out), stats, nil
}

type batchPendingAccountData struct {
	location batchAccountLocation
	header   AppendVecAccount
	start    int64
	end      int64
}

func batchLocationCopies(location batchAccountLocation) uint64 {
	if location.logicalCopies == 0 {
		return 1
	}
	return location.logicalCopies
}

func trackedBatchReadAt(
	ctx context.Context,
	file *os.File,
	data []byte,
	offset int64,
	stats *batchChunkReadStats,
	afterRead func(),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stats.readRanges++
	stats.preadCalls++
	n, err := file.ReadAt(data, offset)
	stats.physicalReadBytes += uint64(n)
	if afterRead != nil {
		afterRead()
	}
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrUnexpectedEOF
	}
	return ctx.Err()
}

func countBatchReadFailure(err error, stats *batchChunkReadStats) {
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		stats.readFailures++
	}
}

func markBatchBaseFalsePositive(
	location batchAccountLocation,
	out []*accounts.Account,
	stats *batchChunkReadStats,
) {
	out[location.outputIdx] = missingAccount(location.pubkey)
	stats.indexMisses += batchLocationCopies(location)
	stats.baseFalsePositives += batchLocationCopies(location)
	stats.placeholderObjects++
}

// handleBatchCandidateBoundsError preserves the distinction between a stale
// candidate in a deliberately retired immutable-base appendvec and corruption
// of an active path. Only the former is an exact miss.
func (db *AccountsDb) handleBatchCandidateBoundsError(
	path string,
	location batchAccountLocation,
	cause error,
	out []*accounts.Account,
	stats *batchChunkReadStats,
) (bool, error) {
	if location.source == accountIndexSourceBase &&
		(errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF)) {
		retired, err := db.isAppendVecRetired(location.entry.Slot, location.entry.FileId)
		if err != nil {
			return false, err
		}
		if retired {
			markBatchBaseFalsePositive(location, out, stats)
			return true, nil
		}
	}
	stats.readFailures++
	return false, fmt.Errorf("unmarshal account at %s@%d: %w", path, location.entry.Offset, cause)
}

func finishBatchDecodedAccount(
	location batchAccountLocation,
	header AppendVecAccount,
	data []byte,
	out []*accounts.Account,
	decodedForCache []*accounts.Account,
	stats *batchChunkReadStats,
) {
	header.Data = data
	acct := header.ToAccount()
	acct.Slot = location.entry.Slot
	stats.indexHits += batchLocationCopies(location)
	if location.source == accountIndexSourceBase {
		stats.baseIndexHits += batchLocationCopies(location)
	}
	stats.appendVecAccounts++
	stats.decodedAccountObjects++
	stats.decodedAccountBytes += uint64(len(data))
	decodedForCache[location.outputIdx] = acct
	if acct.Lamports == 0 {
		stats.placeholderObjects++
	}
	out[location.outputIdx] = batchAccountOrPlaceholder(location.pubkey, acct)
}

// readBatchAppendVecChunk performs two bounded passes. The first coalesces
// nearby fixed-size headers. Data already over-read between nearby headers is
// reused; remaining small payloads are coalesced in a second pass. A payload
// larger than the data-range bound is read directly into its final allocation,
// avoiding an equally large temporary buffer.
func (db *AccountsDb) readBatchAppendVecChunk(
	ctx context.Context,
	file *os.File,
	path string,
	fileSize int64,
	locations []batchAccountLocation,
	out []*accounts.Account,
	decodedForCache []*accounts.Account,
	stats *batchChunkReadStats,
) error {
	headerLocations := make([]batchAccountLocation, 0, len(locations))
	headerSpans := make([]batchReadSpan, 0, len(locations))
	for i, location := range locations {
		if i&15 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		stats.requestedBytes += uint64(hdrLen)
		if location.entry.Offset > math.MaxInt64 {
			stats.readFailures++
			return fmt.Errorf("account offset %d overflows int64", location.entry.Offset)
		}
		offset := int64(location.entry.Offset)
		if offset > math.MaxInt64-int64(hdrLen) || offset > fileSize-int64(hdrLen) {
			handled, err := db.handleBatchCandidateBoundsError(
				path, location, io.ErrUnexpectedEOF, out, stats,
			)
			if err != nil {
				return err
			}
			if handled {
				continue
			}
		}
		headerLocations = append(headerLocations, location)
		headerSpans = append(headerSpans, batchReadSpan{start: offset, end: offset + int64(hdrLen)})
	}

	headerRanges, err := coalesceBatchReadSpans(
		headerSpans, appendVecHeaderCoalesceGapBytes, appendVecHeaderReadRangeMaxBytes,
	)
	if err != nil {
		stats.readFailures++
		return err
	}
	pending := make([]batchPendingAccountData, 0, len(headerLocations))
	for _, readRange := range headerRanges {
		if err := ctx.Err(); err != nil {
			return err
		}
		spanBytes := readRange.end - readRange.start
		buf := make([]byte, int(spanBytes))
		if err := trackedBatchReadAt(ctx, file, buf, readRange.start, stats, db.batchHooks.afterAppendVecPread); err != nil {
			countBatchReadFailure(err, stats)
			return fmt.Errorf("read account headers at %s@%d: %w", path, readRange.start, err)
		}
		for item := readRange.first; item < readRange.limit; item++ {
			location := headerLocations[item]
			headerOffset := int64(location.entry.Offset)
			relative := headerOffset - readRange.start
			headerBytes := (*[hdrLen]byte)(buf[int(relative) : int(relative)+hdrLen])
			var header AppendVecAccount
			header.unmarshalHeaderBytes(headerBytes)
			if header.isTerminator() {
				handled, boundsErr := db.handleBatchCandidateBoundsError(
					path, location, io.EOF, out, stats,
				)
				if boundsErr != nil {
					return boundsErr
				}
				if handled {
					continue
				}
			}
			if header.Pubkey != location.pubkey {
				if location.source == accountIndexSourceBase {
					markBatchBaseFalsePositive(location, out, stats)
					continue
				}
				stats.readFailures++
				return fmt.Errorf(
					"record at %s@%d holds %s (stale index entry)",
					path, location.entry.Offset, header.Pubkey,
				)
			}

			dataStart := headerOffset + int64(hdrLen)
			if header.DataLen > uint64(maxInt) || header.DataLen > uint64(math.MaxInt64-dataStart) {
				handled, boundsErr := db.handleBatchCandidateBoundsError(
					path,
					location,
					fmt.Errorf("appendvec account data length %d overflows addressable range: %w", header.DataLen, io.ErrUnexpectedEOF),
					out,
					stats,
				)
				if boundsErr != nil {
					return boundsErr
				}
				if handled {
					continue
				}
			}
			if header.DataLen > maxAppendVecAccountDataLen {
				handled, boundsErr := db.handleBatchCandidateBoundsError(
					path,
					location,
					fmt.Errorf(
						"appendvec account data length %d exceeds maximum %d: %w",
						header.DataLen,
						maxAppendVecAccountDataLen,
						io.ErrUnexpectedEOF,
					),
					out,
					stats,
				)
				if boundsErr != nil {
					return boundsErr
				}
				if handled {
					continue
				}
			}
			dataEnd := dataStart + int64(header.DataLen)
			if dataEnd > fileSize {
				available := max(int64(0), fileSize-dataStart)
				handled, boundsErr := db.handleBatchCandidateBoundsError(
					path,
					location,
					fmt.Errorf("appendvec account data length %d exceeds %d available bytes: %w", header.DataLen, available, io.ErrUnexpectedEOF),
					out,
					stats,
				)
				if boundsErr != nil {
					return boundsErr
				}
				if handled {
					continue
				}
			}
			stats.requestedBytes += header.DataLen
			if dataEnd <= readRange.end {
				data := make([]byte, int(header.DataLen))
				copy(data, buf[int(dataStart-readRange.start):int(dataEnd-readRange.start)])
				finishBatchDecodedAccount(location, header, data, out, decodedForCache, stats)
				continue
			}
			pending = append(pending, batchPendingAccountData{
				location: location, header: header, start: dataStart, end: dataEnd,
			})
		}
	}

	dataSpans := make([]batchReadSpan, len(pending))
	for i := range pending {
		dataSpans[i] = batchReadSpan{start: pending[i].start, end: pending[i].end}
	}
	dataRanges, err := coalesceBatchReadSpans(
		dataSpans, appendVecDataCoalesceGapBytes, appendVecDataReadRangeMaxBytes,
	)
	if err != nil {
		stats.readFailures++
		return err
	}
	for _, readRange := range dataRanges {
		if err := ctx.Err(); err != nil {
			return err
		}
		spanBytes := readRange.end - readRange.start
		if readRange.limit-readRange.first == 1 && spanBytes > appendVecDataReadRangeMaxBytes {
			item := &pending[readRange.first]
			data := make([]byte, int(spanBytes))
			if err := trackedBatchReadAt(ctx, file, data, readRange.start, stats, db.batchHooks.afterAppendVecPread); err != nil {
				countBatchReadFailure(err, stats)
				return fmt.Errorf("read large account data at %s@%d: %w", path, readRange.start, err)
			}
			finishBatchDecodedAccount(item.location, item.header, data, out, decodedForCache, stats)
			continue
		}

		buf := make([]byte, int(spanBytes))
		if err := trackedBatchReadAt(ctx, file, buf, readRange.start, stats, db.batchHooks.afterAppendVecPread); err != nil {
			countBatchReadFailure(err, stats)
			return fmt.Errorf("read account data at %s@%d: %w", path, readRange.start, err)
		}
		for itemIdx := readRange.first; itemIdx < readRange.limit; itemIdx++ {
			item := &pending[itemIdx]
			data := make([]byte, int(item.end-item.start))
			copy(data, buf[int(item.start-readRange.start):int(item.end-readRange.start)])
			finishBatchDecodedAccount(item.location, item.header, data, out, decodedForCache, stats)
		}
	}
	return nil
}

func resolveBatchAccountLocations(
	ctx context.Context,
	snapshot *MutableAccountIndexSnapshot,
	pks []solana.PublicKey,
	cold []int,
	out []*accounts.Account,
) ([]batchAccountLocation, []bool, error) {
	locations := make([]batchAccountLocation, len(cold))
	found := make([]bool, len(cold))
	if snapshot != nil {
		if len(cold) < batchStaticParallelMinJobs || snapshot.index.checkpoint == nil {
			for job, idx := range cold {
				if job&15 == 0 {
					if err := ctx.Err(); err != nil {
						return nil, nil, err
					}
				}
				value, ok, err := snapshot.LookupWithError(pks[idx])
				if err != nil {
					return nil, nil, fmt.Errorf("mutable index lookup %s: %w", pks[idx], err)
				}
				if !ok {
					continue
				}
				if value.Tombstone {
					locations[job].tombstone = true
					continue
				}
				locations[job] = batchAccountLocation{
					outputIdx: idx, pubkey: pks[idx], entry: value.Entry, source: accountIndexSourceDelta,
				}
				found[job] = true
			}
			return locations, found, nil
		}
		// Probe the active/frozen maps serially under one pinned epoch, then
		// issue independent immutable-checkpoint misses over static worker
		// ranges. The checkpoint mapping is pinned by the outer snapshot, so the
		// batch takes no per-key checkpoint lock.
		err := snapshot.lookupBatchWithError(
			ctx,
			len(cold),
			func(job int) solana.PublicKey { return pks[cold[job]] },
			func(job int, value deltaIndexValue, ok bool) {
				if !ok {
					return
				}
				idx := cold[job]
				if value.Tombstone {
					locations[job].tombstone = true
					return
				}
				locations[job] = batchAccountLocation{
					outputIdx: idx, pubkey: pks[idx], entry: value.Entry, source: accountIndexSourceDelta,
				}
				found[job] = true
			},
		)
		if err != nil {
			return nil, nil, fmt.Errorf("mutable index batch lookup: %w", err)
		}
	}
	return locations, found, nil
}

func resolveProductionBatchAccountLocations(
	ctx context.Context,
	snapshot *ProductionAccountIndexSnapshot,
	pks []solana.PublicKey,
	cold []int,
) ([]batchAccountLocation, []bool, error) {
	locations := make([]batchAccountLocation, len(cold))
	found := make([]bool, len(cold))
	values := make([]deltaIndexValue, len(cold))
	sources := make([]accountIndexSource, len(cold))
	resolved := make([]bool, len(cold))
	if err := snapshot.LookupBatch(ctx, values, sources, resolved); err != nil {
		return nil, nil, fmt.Errorf("production account-index batch lookup: %w", err)
	}
	for job, ok := range resolved {
		if !ok {
			continue
		}
		value := values[job]
		if value.Tombstone {
			locations[job].tombstone = true
			continue
		}
		idx := cold[job]
		locations[job] = batchAccountLocation{
			outputIdx: idx,
			pubkey:    pks[idx],
			entry:     value.Entry,
			source:    sources[job],
		}
		found[job] = true
	}
	return locations, found, nil
}

// resolveBatchBaseCandidates fills only exact delta misses. Tombstones remain
// misses and suppress immutable lookup. Base membership is probabilistic, so
// each returned location is marked for full pubkey verification during the
// appendvec read.
func resolveBatchBaseCandidates(
	ctx context.Context,
	baseIndex *StreamAccountIndex,
	pks []solana.PublicKey,
	cold []int,
	out []*accounts.Account,
	locations []batchAccountLocation,
	found []bool,
) error {
	if baseIndex == nil {
		return nil
	}
	return runBatchStaticRanges(ctx, len(cold), func(workerCtx context.Context, start, end int) error {
		for job := start; job < end; job++ {
			if job&255 == 0 {
				if err := workerCtx.Err(); err != nil {
					return err
				}
			}
			if found[job] || locations[job].tombstone {
				continue
			}
			idx := cold[job]
			entry, ok, err := baseIndex.LookupCandidate(pks[idx])
			if err != nil {
				return fmt.Errorf("base index lookup %s: %w", pks[idx], err)
			}
			if !ok {
				continue
			}
			locations[job] = batchAccountLocation{
				outputIdx: idx, pubkey: pks[idx], entry: entry, source: accountIndexSourceBase,
			}
			found[job] = true
		}
		return nil
	})
}

func readBatchAccountAt(file *os.File, path string, location batchAccountLocation) (*accounts.Account, error) {
	if location.entry.Offset > math.MaxInt64 {
		return nil, fmt.Errorf("account offset %d overflows int64", location.entry.Offset)
	}
	offset := int64(location.entry.Offset)
	acct, exact, err := unmarshalAcctFromAppendVecAcctHeaderExpectedAt(file, offset, location.pubkey)
	if err != nil {
		return nil, fmt.Errorf("unmarshal account at %s@%d: %w", path, location.entry.Offset, err)
	}
	if !exact {
		if location.source == accountIndexSourceBase {
			return nil, ErrNoAccount
		}
		return nil, fmt.Errorf("record at %s@%d holds %s (stale index entry)", path, location.entry.Offset, acct.Key)
	}
	acct.Slot = location.entry.Slot
	return acct, nil
}

func batchAccountOrPlaceholder(pubkey solana.PublicKey, acct *accounts.Account) *accounts.Account {
	if acct == nil || acct.Lamports == 0 {
		return missingAccount(pubkey)
	}
	return acct
}

func missingAccount(pubkey solana.PublicKey) *accounts.Account {
	return &accounts.Account{Key: pubkey, Owner: systemProgramAddr, RentEpoch: math.MaxUint64}
}

// runBatchWorkers executes count indexed jobs using at most 2*GOMAXPROCS
// goroutines. The first error stops new work and is returned after active jobs
// drain. This bounds both goroutine count and scheduler traffic independently
// of block size.
func runBatchWorkers(ctx context.Context, count int, work func(int) error) error {
	return runBatchWorkersLimited(ctx, count, max(1, runtime.GOMAXPROCS(0)*2), work)
}

func runBatchWorkersLimited(ctx context.Context, count, maxWorkers int, work func(int) error) error {
	if count == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	workerCount := min(count, max(1, maxWorkers), max(1, runtime.GOMAXPROCS(0)*2))
	var next atomic.Uint64
	var stopped atomic.Bool
	var firstErr error
	var errOnce sync.Once
	setError := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			stopped.Store(true)
		})
	}

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for !stopped.Load() {
				if err := ctx.Err(); err != nil {
					setError(err)
					return
				}
				job := int(next.Add(1) - 1)
				if job >= count {
					return
				}
				if err := work(job); err != nil {
					setError(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
