package accountsdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"golang.org/x/sync/errgroup"
)

var systemProgramAddr [32]byte

// appendVecReadChunkSize amortizes open/close calls without serializing every
// read from a large fold segment behind one worker. Chunks are offset-sorted,
// which also gives the kernel a chance to coalesce nearby appendvec reads.
const (
	appendVecReadChunkSize = 64

	// Point Gets remain useful for tiny requests and for recent keys high in a
	// deep LSM. Above this conservative crossover, monotonic snapshot iterators
	// amortize Pebble's per-key read-state and iterator setup.
	batchIndexIteratorThreshold = 16
	batchIndexKeysPerIterator   = 1024
	batchIndexMinIterators      = 4
	batchIndexMaxIterators      = 8
)

type batchReadTestHooks struct {
	beforeCacheAdmission func(solana.PublicKey)
}

type batchAccountLocation struct {
	outputIdx int
	pubkey    solana.PublicKey
	entry     AccountIndexEntry
}

type appendVecID struct {
	slot   uint64
	fileID uint64
}

type appendVecReadChunk struct {
	id        appendVecID
	locations []batchAccountLocation
}

// BatchReadStats is one durable batch read's wall-time and work breakdown.
// Allocation counters describe logical objects created directly by this path;
// they deliberately exclude Pebble/OS/runtime internals.
type BatchReadStats struct {
	RequestedKeys uint64
	DurableKeys   uint64

	WorkingSetHits    uint64
	InProgressHits    uint64
	PendingFoldHits   uint64
	CacheHits         uint64
	IndexHits         uint64
	IndexMisses       uint64
	UniqueAppendVecs  uint64
	AppendVecChunks   uint64
	AppendVecAccounts uint64
	OpenFailures      uint64
	ReadFailures      uint64
	RetryAccounts     uint64

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
	decodedAccountObjects uint64
	decodedAccountBytes   uint64
	placeholderObjects    uint64
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

func (db *AccountsDb) getAccountsBatchWithStats(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, BatchReadStats, error) {
	stats := BatchReadStats{RequestedKeys: uint64(len(pks)), DurableKeys: uint64(len(pks))}
	if len(pks) == 0 {
		return nil, stats, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
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
	var indexSnapshot *pebble.Snapshot
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
			stats.InProgressHits++
			continue
		}
		if acct, ok, pending := db.getCachedAccountLocked(pk); ok {
			if pending {
				stats.PendingFoldHits++
			} else {
				stats.CacheHits++
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
	if len(cold) > 0 && db.Index != nil {
		// The cache probes and snapshot share one publication epoch. A fold
		// cannot flip the index and refresh caches between these two views.
		snapshotStart := time.Now()
		indexSnapshot = db.Index.NewSnapshot()
		snapshotSetupNanoseconds = uint64(time.Since(snapshotStart).Nanoseconds())
	}
	db.readCacheEpochMu.RUnlock()
	if len(cold) == 0 {
		return out, stats, nil
	}
	if db.Index == nil {
		for _, idx := range cold {
			out[idx] = missingAccount(pks[idx])
		}
		stats.IndexMisses = uint64(len(cold))
		stats.PlaceholderObjects += uint64(len(cold))
		return out, stats, nil
	}

	phaseStart = time.Now()
	admitCommon := admission.classifyAndObserve(pks, cold)
	stats.AdmissionFilterNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())
	phaseStart = time.Now()
	locations, found, err := resolveBatchAccountLocations(ctx, indexSnapshot, pks, cold, out)
	closeErr := indexSnapshot.Close()
	stats.IndexLookupNanoseconds = snapshotSetupNanoseconds + uint64(time.Since(phaseStart).Nanoseconds())
	if err != nil {
		return nil, stats, err
	}
	if closeErr != nil {
		return nil, stats, fmt.Errorf("close account index snapshot: %w", closeErr)
	}

	phaseStart = time.Now()
	groups := make(map[appendVecID][]batchAccountLocation)
	for i, location := range locations {
		if !found[i] {
			stats.IndexMisses++
			stats.PlaceholderObjects++
			continue
		}
		stats.IndexHits++
		id := appendVecID{slot: location.entry.Slot, fileID: location.entry.FileId}
		groups[id] = append(groups[id], location)
	}
	stats.UniqueAppendVecs = uint64(len(groups))

	chunks := make([]appendVecReadChunk, 0, len(groups))
	for id, group := range groups {
		sort.Slice(group, func(i, j int) bool {
			return group[i].entry.Offset < group[j].entry.Offset
		})
		for start := 0; start < len(group); start += appendVecReadChunkSize {
			end := min(start+appendVecReadChunkSize, len(group))
			chunks = append(chunks, appendVecReadChunk{id: id, locations: group[start:end]})
		}
	}
	stats.AppendVecChunks = uint64(len(chunks))
	chunkStats := make([]batchChunkReadStats, len(chunks))
	decodedForCache := make([]*accounts.Account, len(pks))
	stats.ReadPlanningNanoseconds = uint64(time.Since(phaseStart).Nanoseconds())

	// Each worker opens an appendvec once per small chunk, instead of once per
	// account. appendVecReadMu pins every resolved source path until all reads
	// finish, so an error is real I/O/corruption rather than a compaction race.
	phaseStart = time.Now()
	err = runBatchWorkers(ctx, len(chunks), func(job int) error {
		chunk := chunks[job]
		jobStats := &chunkStats[job]
		path := db.appendVecPath(chunk.id.slot, chunk.id.fileID)
		file, openErr := os.Open(path)
		if openErr != nil {
			jobStats.openFailures++
			return fmt.Errorf("open account appendvec %s: %w", path, openErr)
		}
		defer file.Close()

		for _, location := range chunk.locations {
			if err := ctx.Err(); err != nil {
				return err
			}
			acct, readErr := readBatchAccountAt(file, path, location)
			if readErr != nil {
				jobStats.readFailures++
				return readErr
			}
			jobStats.appendVecAccounts++
			jobStats.decodedAccountObjects++
			jobStats.decodedAccountBytes += uint64(len(acct.Data))
			decodedForCache[location.outputIdx] = acct
			if acct.Lamports == 0 {
				jobStats.placeholderObjects++
			}
			out[location.outputIdx] = batchAccountOrPlaceholder(location.pubkey, acct)
		}
		return nil
	})
	for _, chunkStat := range chunkStats {
		stats.AppendVecAccounts += chunkStat.appendVecAccounts
		stats.OpenFailures += chunkStat.openFailures
		stats.ReadFailures += chunkStat.readFailures
		stats.DecodedAccountObjects += chunkStat.decodedAccountObjects
		stats.DecodedAccountBytes += chunkStat.decodedAccountBytes
		stats.PlaceholderObjects += chunkStat.placeholderObjects
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
	return out, stats, nil
}

func resolveBatchAccountLocations(
	ctx context.Context,
	snapshot *pebble.Snapshot,
	pks []solana.PublicKey,
	cold []int,
	out []*accounts.Account,
) ([]batchAccountLocation, []bool, error) {
	if len(cold) < batchIndexIteratorThreshold {
		return resolveBatchAccountLocationsPointGets(ctx, snapshot, pks, cold, out)
	}
	return resolveBatchAccountLocationsIterators(
		ctx,
		snapshot,
		pks,
		cold,
		out,
		batchIndexIteratorWorkers(len(cold)),
	)
}

func batchIndexIteratorWorkers(keyCount int) int {
	workers := (keyCount + batchIndexKeysPerIterator - 1) / batchIndexKeysPerIterator
	workers = max(workers, batchIndexMinIterators)
	workers = min(workers, batchIndexMaxIterators, runtime.GOMAXPROCS(0))
	return min(keyCount, max(1, workers))
}

// resolveBatchAccountLocationsPointGets retains Pebble's specialized point
// iterator for small batches, where sorting and a full merging iterator cost
// more than the per-key setup it would amortize.
func resolveBatchAccountLocationsPointGets(
	ctx context.Context,
	snapshot *pebble.Snapshot,
	pks []solana.PublicKey,
	cold []int,
	out []*accounts.Account,
) ([]batchAccountLocation, []bool, error) {
	locations := make([]batchAccountLocation, len(cold))
	found := make([]bool, len(cold))
	err := runBatchWorkers(ctx, len(cold), func(job int) error {
		idx := cold[job]
		entryBytes, closer, err := snapshot.Get(pks[idx][:])
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				out[idx] = missingAccount(pks[idx])
				return nil
			}
			return fmt.Errorf("index get %s: %w", pks[idx], err)
		}
		entry, decodeErr := UnmarshalAcctIdxEntryValue(entryBytes)
		closeErr := closer.Close()
		if decodeErr != nil {
			return fmt.Errorf("unmarshal index entry for %s: %w", pks[idx], decodeErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close index value for %s: %w", pks[idx], closeErr)
		}
		locations[job] = batchAccountLocation{outputIdx: idx, pubkey: pks[idx], entry: entry}
		found[job] = true
		return nil
	})
	return locations, found, err
}

// resolveBatchAccountLocationsIterators sorts the local cold-index slice and
// assigns contiguous key ranges to worker-local iterators created from one
// snapshot. Repeated monotonic SeekGE calls let Pebble reuse forward seek state
// and amortize read-state/iterator setup without scanning sparse key gaps.
func resolveBatchAccountLocationsIterators(
	ctx context.Context,
	snapshot *pebble.Snapshot,
	pks []solana.PublicKey,
	cold []int,
	out []*accounts.Account,
	workerCount int,
) ([]batchAccountLocation, []bool, error) {
	sort.Slice(cold, func(i, j int) bool {
		return bytes.Compare(pks[cold[i]][:], pks[cold[j]][:]) < 0
	})
	locations := make([]batchAccountLocation, len(cold))
	found := make([]bool, len(cold))
	workerCount = min(len(cold), max(1, workerCount))

	g, workerCtx := errgroup.WithContext(ctx)
	upperBounds := make([][32]byte, workerCount)
	for worker := range workerCount {
		start := len(cold) * worker / workerCount
		end := len(cold) * (worker + 1) / workerCount
		g.Go(func() (retErr error) {
			options := pebble.IterOptions{
				KeyTypes:   pebble.IterKeyTypePointsOnly,
				LowerBound: pks[cold[start]][:],
			}
			if nextPubkey(pks[cold[end-1]], &upperBounds[worker]) {
				options.UpperBound = upperBounds[worker][:]
			}
			iter, err := snapshot.NewIterWithContext(workerCtx, &options)
			if err != nil {
				return fmt.Errorf("create account index iterator: %w", err)
			}
			defer func() {
				if closeErr := iter.Close(); retErr == nil && closeErr != nil {
					retErr = fmt.Errorf("close account index iterator: %w", closeErr)
				}
			}()

			for job := start; job < end; job++ {
				if err := workerCtx.Err(); err != nil {
					return err
				}
				idx := cold[job]
				if !iter.SeekGE(pks[idx][:]) {
					if err := iter.Error(); err != nil {
						return fmt.Errorf("index seek %s: %w", pks[idx], err)
					}
					out[idx] = missingAccount(pks[idx])
					continue
				}
				if !bytes.Equal(iter.Key(), pks[idx][:]) {
					out[idx] = missingAccount(pks[idx])
					continue
				}
				entryBytes, err := iter.ValueAndErr()
				if err != nil {
					return fmt.Errorf("read index value for %s: %w", pks[idx], err)
				}
				entry, err := UnmarshalAcctIdxEntryValue(entryBytes)
				if err != nil {
					return fmt.Errorf("unmarshal index entry for %s: %w", pks[idx], err)
				}
				locations[job] = batchAccountLocation{outputIdx: idx, pubkey: pks[idx], entry: entry}
				found[job] = true
			}
			return iter.Error()
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	return locations, found, nil
}

// nextPubkey returns the smallest 32-byte key strictly greater than key. A
// false result means key is the maximal all-0xff value and needs no upper bound.
func nextPubkey(key solana.PublicKey, out *[32]byte) bool {
	*out = key
	for idx := len(out) - 1; idx >= 0; idx-- {
		out[idx]++
		if out[idx] != 0 {
			return true
		}
	}
	return false
}

func readBatchAccountAt(file *os.File, path string, location batchAccountLocation) (*accounts.Account, error) {
	if location.entry.Offset > math.MaxInt64 {
		return nil, fmt.Errorf("account offset %d overflows int64", location.entry.Offset)
	}
	offset := int64(location.entry.Offset)
	acct, err := unmarshalAcctFromAppendVecAcctHeader(io.NewSectionReader(file, offset, math.MaxInt64-offset))
	if err != nil {
		return nil, fmt.Errorf("unmarshal account at %s@%d: %w", path, location.entry.Offset, err)
	}
	if acct.Key != location.pubkey {
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
	if count == 0 {
		return nil
	}
	workerCount := min(count, max(1, runtime.GOMAXPROCS(0)*2))
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
	return firstErr
}
