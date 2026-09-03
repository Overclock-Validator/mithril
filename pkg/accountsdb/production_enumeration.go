package accountsdb

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/gagliardetto/solana-go"
	"golang.org/x/sys/unix"
)

const productionEnumerationFDHeadroom = 64

// scanPhysicalAccountIndexRange visits one exact physical account-index epoch
// in bytewise public-key order. startPrefix and endPrefix are inclusive,
// big-endian values for the first eight bytes of the public key. It is
// intentionally unexported: only AccountsDb can hold accountIndexWriteMu while
// capturing the snapshot, which is required to make a multi-frame oversized
// fold one logical enumeration epoch. Tests which exercise the physical index
// directly must not infer that stronger AccountsDb transaction guarantee.
//
// The scan captures one coherent mutable/root epoch before doing any disk I/O:
// active RAM overrides frozen RAM, which overrides the exact checkpoint,
// which overrides the immutable base. Exact tombstones suppress older live
// records. The callback runs without an account-index lock held and may be
// relatively slow, but it must not retain entry references (all values are
// passed by value).
func (index *ProductionAccountIndex) scanPhysicalAccountIndexRange(
	ctx context.Context,
	startPrefix uint64,
	endPrefix uint64,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	if visit == nil {
		return errors.New("accountsdb: nil production account-index scan visitor")
	}
	release, err := index.acquireEnumerationPermit(ctx)
	if err != nil {
		return err
	}
	defer release()
	snapshot, err := index.captureEnumerationSnapshot(ctx, startPrefix, endPrefix)
	if err != nil {
		return err
	}
	defer snapshot.Close()
	return snapshot.scan(ctx, visit)
}

func (index *ProductionAccountIndex) acquireEnumerationPermit(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("accountsdb: nil production account-index scan context")
	}
	if index == nil || index.enumerationGate == nil {
		return nil, errors.New("accountsdb: production account-index enumeration is not initialized")
	}
	select {
	case index.enumerationGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := index.checkEnumerationFileDescriptorBudget(); err != nil {
		<-index.enumerationGate
		return nil, err
	}
	return func() { <-index.enumerationGate }, nil
}

func (index *ProductionAccountIndex) checkEnumerationFileDescriptorBudget() error {
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return fmt.Errorf("accountsdb: read file-descriptor limit for keyed-shard enumeration: %w", err)
	}
	openCount := uint64(0)
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		openCount = uint64(len(entries))
	}
	return validateProductionEnumerationFileDescriptorBudget(
		index.config.ShardCount, openCount, limit.Cur,
	)
}

func validateProductionEnumerationFileDescriptorBudget(
	shardCount int,
	openCount uint64,
	softLimit uint64,
) error {
	if shardCount < 1 {
		return errors.New("accountsdb: keyed-shard enumeration has an invalid shard count")
	}
	required := uint64(shardCount) + productionEnumerationFDHeadroom
	if openCount > ^uint64(0)-required || openCount+required > softLimit {
		return fmt.Errorf(
			"accountsdb: keyed-shard enumeration needs up to %d sidecar descriptors plus %d descriptor headroom (currently open %d, RLIMIT_NOFILE %d); raise the process file-descriptor limit",
			shardCount,
			productionEnumerationFDHeadroom,
			openCount,
			softLimit,
		)
	}
	return nil
}

// captureEnumerationSnapshot performs the bounded in-memory portion of a
// range scan. AccountsDb holds its logical index-writer fence only around this
// call, so a multi-frame fold cannot be observed half-published while disk I/O
// and the caller's visitor remain completely outside that fence.
func (index *ProductionAccountIndex) captureEnumerationSnapshot(
	ctx context.Context,
	startPrefix uint64,
	endPrefix uint64,
) (*productionEnumerationSnapshot, error) {
	if ctx == nil {
		return nil, errors.New("accountsdb: nil production account-index scan context")
	}
	if startPrefix > endPrefix {
		return nil, fmt.Errorf(
			"accountsdb: invalid account-index prefix range: start %d exceeds end %d",
			startPrefix,
			endPrefix,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := index.checkUsable(); err != nil {
		return nil, err
	}
	snapshot, err := index.mutable.captureEnumerationSnapshot(ctx, startPrefix, endPrefix)
	if err != nil {
		return nil, err
	}
	snapshot.lower = accountIndexPrefixBoundary(startPrefix, 0x00)
	snapshot.upper = accountIndexPrefixBoundary(endPrefix, 0xff)
	return snapshot, nil
}

// scan streams a previously captured coherent epoch. It owns no AccountsDb
// writer lock; checkpoint handles and rootPin keep every immutable dependency
// alive until the caller closes the snapshot.
func (snapshot *productionEnumerationSnapshot) scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) (retErr error) {
	if snapshot == nil || snapshot.immutable == nil {
		return errors.New("accountsdb: invalid production account-index enumeration snapshot")
	}
	if ctx == nil {
		return errors.New("accountsdb: nil production account-index scan context")
	}
	if visit == nil {
		return errors.New("accountsdb: nil production account-index scan visitor")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	iterators := make([]*productionShardEnumerationIterator, len(snapshot.hot))
	defer func() {
		for _, iterator := range iterators {
			if iterator != nil {
				retErr = errors.Join(retErr, iterator.Close())
			}
		}
	}()

	queue := make(productionEnumerationHeap, 0, len(iterators))
	for shardID := range iterators {
		if err := ctx.Err(); err != nil {
			return err
		}
		iterator, err := newProductionShardEnumerationIterator(
			ctx,
			uint32(shardID),
			snapshot.hot[shardID],
			snapshot.checkpoints[shardID],
			snapshot.immutable.shards[shardID].base,
			snapshot.lower,
			snapshot.upper,
		)
		if err != nil {
			if errors.Is(err, unix.EMFILE) {
				return fmt.Errorf(
					"accountsdb: open enumeration shard %d exceeded RLIMIT_NOFILE despite preflight; reduce concurrent process descriptor use or raise the limit: %w",
					shardID, err,
				)
			}
			return fmt.Errorf("accountsdb: open enumeration shard %d: %w", shardID, err)
		}
		iterators[shardID] = iterator
		key, entry, ok, err := iterator.Next(ctx)
		if err != nil {
			return fmt.Errorf("accountsdb: prime enumeration shard %d: %w", shardID, err)
		}
		if ok {
			heap.Push(&queue, productionEnumerationHeapItem{
				shardID: uint32(shardID), key: key, entry: entry,
			})
		}
	}

	var emitted uint64
	var previous solana.PublicKey
	havePrevious := false
	for queue.Len() != 0 {
		if emitted&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		item := heap.Pop(&queue).(productionEnumerationHeapItem)
		if havePrevious && bytes.Compare(previous[:], item.key[:]) >= 0 {
			return fmt.Errorf(
				"accountsdb: keyed-shard enumeration is not globally strictly sorted at key %x",
				item.key,
			)
		}
		if err := visit(item.key, item.entry); err != nil {
			return err
		}
		previous = item.key
		havePrevious = true
		emitted++
		key, entry, ok, err := iterators[item.shardID].Next(ctx)
		if err != nil {
			return fmt.Errorf("accountsdb: advance enumeration shard %d: %w", item.shardID, err)
		}
		if ok {
			heap.Push(&queue, productionEnumerationHeapItem{
				shardID: item.shardID, key: key, entry: entry,
			})
		}
	}
	return ctx.Err()
}

// productionEnumerationSnapshot owns a coherent read epoch. Hot maps are
// private copies, checkpoint handles are retained, and immutable is protected
// by rootPin until Close.
type productionEnumerationSnapshot struct {
	lower       solana.PublicKey
	upper       solana.PublicKey
	hot         [][]productionHotEnumerationRecord
	checkpoints []*ShardedDeltaCheckpointHandle
	rootPin     ShardedMutableImmutablePin
	immutable   *ShardedImmutableIndex
	closeOnce   sync.Once
	closeErr    error
}

type productionHotEnumerationRecord struct {
	key        solana.PublicKey
	value      deltaIndexValue
	precedence uint8
}

func (snapshot *productionEnumerationSnapshot) Close() error {
	if snapshot == nil {
		return nil
	}
	snapshot.closeOnce.Do(func() {
		for i, checkpoint := range snapshot.checkpoints {
			if checkpoint != nil {
				snapshot.closeErr = errors.Join(snapshot.closeErr, checkpoint.Release())
				snapshot.checkpoints[i] = nil
			}
		}
		if snapshot.rootPin != nil {
			snapshot.closeErr = errors.Join(snapshot.closeErr, snapshot.rootPin.Close())
			snapshot.rootPin = nil
		}
		snapshot.immutable = nil
	})
	return snapshot.closeErr
}

func (idx *ShardedMutableAccountIndex) captureEnumerationSnapshot(
	ctx context.Context,
	startPrefix uint64,
	endPrefix uint64,
) (*productionEnumerationSnapshot, error) {
	if idx == nil {
		return nil, errors.New("accountsdb: nil sharded mutable account index")
	}
	if ctx == nil {
		return nil, errors.New("accountsdb: nil sharded mutable enumeration context")
	}
	if startPrefix > endPrefix {
		return nil, errors.New("accountsdb: invalid sharded mutable enumeration prefix range")
	}
	// Keyed routing intentionally destroys the raw-prefix/shard correspondence.
	// Exact raw-prefix scans are maintenance/rent paths, so they inspect every
	// shard and perform a bounded global merge. Point and batch account loading
	// still touches exactly one well-balanced shard per key.
	count := idx.router.Count()
	snapshot := &productionEnumerationSnapshot{
		hot:         make([][]productionHotEnumerationRecord, count),
		checkpoints: make([]*ShardedDeltaCheckpointHandle, count),
	}

	// Obtain allocation hints separately so the potentially large backing
	// arrays are allocated without holding the mutable epoch lock. The second
	// lock below captures the one authoritative epoch; concurrent changes can
	// only make append grow a hinted slice and cannot affect correctness.
	hints := make([]int, count)
	idx.stateMu.RLock()
	if idx.closed || idx.closing {
		idx.stateMu.RUnlock()
		return nil, ErrShardedMutableClosed
	}
	if idx.poison != nil {
		err := fmt.Errorf("accountsdb: sharded mutable index is poisoned: %w", idx.poison)
		idx.stateMu.RUnlock()
		return nil, err
	}
	for position := range idx.shards {
		hints[position] = len(idx.shards[position].frozen) + len(idx.shards[position].active)
	}
	idx.stateMu.RUnlock()
	for position, capacity := range hints {
		if capacity != 0 {
			snapshot.hot[position] = make([]productionHotEnumerationRecord, 0, capacity)
		}
	}

	idx.stateMu.RLock()
	fail := func(err error) (*productionEnumerationSnapshot, error) {
		idx.stateMu.RUnlock()
		_ = snapshot.Close()
		return nil, err
	}
	if idx.closed || idx.closing {
		return fail(ErrShardedMutableClosed)
	}
	if idx.poison != nil {
		return fail(fmt.Errorf("accountsdb: sharded mutable index is poisoned: %w", idx.poison))
	}
	checked := 0
	for position := range idx.shards {
		shardID := uint32(position)
		shard := &idx.shards[shardID]
		copyLayer := func(layer map[solana.PublicKey]deltaIndexValue, precedence uint8) error {
			for key, value := range layer {
				checked++
				if checked&4095 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if !accountIndexKeyInPrefixRange(key, startPrefix, endPrefix) {
					continue
				}
				snapshot.hot[position] = append(snapshot.hot[position], productionHotEnumerationRecord{
					key: key, value: value, precedence: precedence,
				})
			}
			return nil
		}
		// Frozen is older. Active overwrites duplicate keys below.
		if err := copyLayer(shard.frozen, 0); err != nil {
			return fail(err)
		}
		if err := copyLayer(shard.active, 1); err != nil {
			return fail(err)
		}
		if shard.checkpoint != nil {
			if err := shard.checkpoint.Retain(); err != nil {
				return fail(fmt.Errorf("accountsdb: retain enumeration checkpoint shard %d: %w", shardID, err))
			}
			snapshot.checkpoints[position] = shard.checkpoint
		}
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	acquire := idx.config.Callbacks.AcquireImmutable
	if acquire == nil {
		return fail(errors.New("accountsdb: enumeration requires an immutable-generation pin callback"))
	}
	pin, err := acquire()
	if err != nil {
		return fail(fmt.Errorf("accountsdb: pin immutable enumeration generation: %w", err))
	}
	if pin == nil {
		return fail(errors.New("accountsdb: immutable enumeration pin callback returned nil"))
	}
	snapshot.rootPin = pin
	view, ok := pin.(*IndexReadView)
	if !ok || view == nil {
		return fail(errors.New("accountsdb: immutable enumeration pin has an invalid type"))
	}
	immutable, ok := view.Payload().(*ShardedImmutableIndex)
	if !ok || immutable == nil {
		return fail(errors.New("accountsdb: immutable enumeration pin has an invalid payload"))
	}
	if immutable.router.Count() != idx.router.Count() ||
		immutable.router.RoutingKey() != idx.router.RoutingKey() ||
		len(immutable.shards) != idx.router.Count() {
		return fail(errors.New("accountsdb: mutable and immutable enumeration routers disagree"))
	}
	snapshot.immutable = immutable
	idx.stateMu.RUnlock()
	for position := range snapshot.hot {
		normalizeProductionHotEnumerationRecords(snapshot.hot[position])
		snapshot.hot[position] = compactProductionHotEnumerationRecords(snapshot.hot[position])
	}
	return snapshot, nil
}

func normalizeProductionHotEnumerationRecords(records []productionHotEnumerationRecord) {
	sort.Slice(records, func(i, j int) bool {
		if cmp := bytes.Compare(records[i].key[:], records[j].key[:]); cmp != 0 {
			return cmp < 0
		}
		return records[i].precedence > records[j].precedence
	})
}

func compactProductionHotEnumerationRecords(
	records []productionHotEnumerationRecord,
) []productionHotEnumerationRecord {
	write := 0
	for read := 0; read < len(records); {
		winner := records[read]
		next := read + 1
		for next < len(records) && records[next].key == winner.key {
			next++
		}
		records[write] = winner
		write++
		read = next
	}
	clear(records[write:])
	return records[:write]
}

func accountIndexPrefixBoundary(prefix uint64, suffix byte) solana.PublicKey {
	var key solana.PublicKey
	binary.BigEndian.PutUint64(key[:8], prefix)
	for i := 8; i < len(key); i++ {
		key[i] = suffix
	}
	return key
}

func accountIndexKeyInPrefixRange(key solana.PublicKey, startPrefix, endPrefix uint64) bool {
	prefix := binary.BigEndian.Uint64(key[:8])
	return prefix >= startPrefix && prefix <= endPrefix
}

const productionEnumerationBaseBufferBytes = 16 << 10

type productionEnumerationHeapItem struct {
	shardID uint32
	key     solana.PublicKey
	entry   AccountIndexEntry
}

type productionEnumerationHeap []productionEnumerationHeapItem

func (items productionEnumerationHeap) Len() int { return len(items) }

func (items productionEnumerationHeap) Less(i, j int) bool {
	return bytes.Compare(items[i].key[:], items[j].key[:]) < 0
}

func (items productionEnumerationHeap) Swap(i, j int) { items[i], items[j] = items[j], items[i] }

func (items *productionEnumerationHeap) Push(value any) {
	*items = append(*items, value.(productionEnumerationHeapItem))
}

func (items *productionEnumerationHeap) Pop() any {
	old := *items
	last := len(old) - 1
	value := old[last]
	old[last] = productionEnumerationHeapItem{}
	*items = old[:last]
	return value
}

// productionShardEnumerationIterator performs an exact three-way merge for a
// single keyed shard. Hot wins over checkpoint, checkpoint wins over base, and
// tombstones suppress every older copy. Its base cursor owns at most one small
// buffered FD; at the production default a global scan is therefore bounded by
// 1024 sidecar FDs, 16 MiB of base read buffers, one heap item per shard, and
// one compact key/value sort record per captured physical hot entry.
// Checkpoint records remain mmap-backed and are never copied into a second Go
// map.
type productionShardEnumerationIterator struct {
	shardID uint32
	router  PersistentIndexShardRouter

	hot    []productionHotEnumerationRecord
	hotPos int

	checkpoint *productionDeltaEnumerationCursor
	base       *productionBaseEnumerationCursor
}

func newProductionShardEnumerationIterator(
	ctx context.Context,
	shardID uint32,
	hot []productionHotEnumerationRecord,
	handle *ShardedDeltaCheckpointHandle,
	base *ShardedStreamBaseShard,
	lower solana.PublicKey,
	upper solana.PublicKey,
) (_ *productionShardEnumerationIterator, retErr error) {
	if base == nil {
		return nil, fmt.Errorf("immutable account-index shard %d has no base", shardID)
	}
	router := base.router
	if router.Shard(lower) >= uint32(router.Count()) { // also rejects a zero router
		return nil, errors.New("accountsdb: invalid immutable enumeration router")
	}
	iterator := &productionShardEnumerationIterator{
		shardID: shardID,
		router:  router,
		hot:     hot,
	}
	for position, record := range hot {
		if routed := router.Shard(record.key); routed != shardID {
			return nil, fmt.Errorf(
				"mutable enumeration key %x routes to shard %d, stored in shard %d",
				record.key, routed, shardID,
			)
		}
		if position != 0 && bytes.Compare(hot[position-1].key[:], record.key[:]) >= 0 {
			return nil, errors.New("accountsdb: mutable enumeration records are not strictly sorted")
		}
	}

	checkpoint, err := newProductionDeltaEnumerationCursor(
		ctx, handle, router, shardID, lower, upper,
	)
	if err != nil {
		return nil, err
	}
	iterator.checkpoint = checkpoint
	baseCursor, err := newProductionBaseEnumerationCursor(ctx, base, lower, upper)
	if err != nil {
		return nil, err
	}
	iterator.base = baseCursor
	return iterator, nil
}

func (iterator *productionShardEnumerationIterator) Close() error {
	if iterator == nil || iterator.base == nil {
		return nil
	}
	err := iterator.base.Close()
	iterator.base = nil
	return err
}

func (iterator *productionShardEnumerationIterator) Next(
	ctx context.Context,
) (solana.PublicKey, AccountIndexEntry, bool, error) {
	if iterator == nil {
		return solana.PublicKey{}, AccountIndexEntry{}, false, errors.New("accountsdb: nil shard enumeration iterator")
	}
	for {
		if err := ctx.Err(); err != nil {
			return solana.PublicKey{}, AccountIndexEntry{}, false, err
		}
		var minimum solana.PublicKey
		haveMinimum := false
		consider := func(key solana.PublicKey, valid bool) {
			if valid && (!haveMinimum || bytes.Compare(key[:], minimum[:]) < 0) {
				minimum = key
				haveMinimum = true
			}
		}
		consider(iterator.hotKey())
		if iterator.checkpoint != nil {
			consider(iterator.checkpoint.key, iterator.checkpoint.valid)
		}
		if iterator.base != nil {
			consider(iterator.base.key, iterator.base.valid)
		}
		if !haveMinimum {
			return solana.PublicKey{}, AccountIndexEntry{}, false, nil
		}

		var winner deltaIndexValue
		if key, valid := iterator.hotKey(); valid && key == minimum {
			winner = iterator.hot[iterator.hotPos].value
		} else if iterator.checkpoint != nil && iterator.checkpoint.valid && iterator.checkpoint.key == minimum {
			winner = iterator.checkpoint.value
		} else {
			winner = deltaIndexValue{Entry: iterator.base.entry}
		}

		if key, valid := iterator.hotKey(); valid && key == minimum {
			iterator.hotPos++
		}
		if iterator.checkpoint != nil && iterator.checkpoint.valid && iterator.checkpoint.key == minimum {
			if err := iterator.checkpoint.advance(ctx); err != nil {
				return solana.PublicKey{}, AccountIndexEntry{}, false, err
			}
		}
		if iterator.base != nil && iterator.base.valid && iterator.base.key == minimum {
			if err := iterator.base.advance(ctx); err != nil {
				return solana.PublicKey{}, AccountIndexEntry{}, false, err
			}
		}
		if winner.Tombstone {
			continue
		}
		return minimum, winner.Entry, true, nil
	}
}

func (iterator *productionShardEnumerationIterator) hotKey() (solana.PublicKey, bool) {
	if iterator.hotPos >= len(iterator.hot) {
		return solana.PublicKey{}, false
	}
	return iterator.hot[iterator.hotPos].key, true
}

type productionDeltaEnumerationCursor struct {
	records  []byte
	count    uint64
	ordinal  uint64
	upper    solana.PublicKey
	router   PersistentIndexShardRouter
	shardID  uint32
	key      solana.PublicKey
	value    deltaIndexValue
	previous solana.PublicKey
	havePrev bool
	valid    bool
}

func newProductionDeltaEnumerationCursor(
	ctx context.Context,
	handle *ShardedDeltaCheckpointHandle,
	router PersistentIndexShardRouter,
	shardID uint32,
	lower solana.PublicKey,
	upper solana.PublicKey,
) (*productionDeltaEnumerationCursor, error) {
	if handle == nil {
		return nil, nil
	}
	checkpoint := handle.Checkpoint()
	if checkpoint == nil {
		return nil, errors.New("accountsdb: retained enumeration checkpoint was released")
	}
	checkpoint.mu.RLock()
	if checkpoint.closed || checkpoint.records == nil {
		checkpoint.mu.RUnlock()
		return nil, errors.New("accountsdb: retained enumeration checkpoint is closed")
	}
	ordinal, err := checkpoint.lowerBoundPinned(ctx, lower)
	records := []byte(checkpoint.records)
	count := checkpoint.recordCount
	checkpoint.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	cursor := &productionDeltaEnumerationCursor{
		records: records, count: count, ordinal: ordinal, upper: upper,
		router: router, shardID: shardID,
	}
	if err := cursor.advance(ctx); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (cursor *productionDeltaEnumerationCursor) advance(ctx context.Context) error {
	cursor.valid = false
	if cursor.ordinal >= cursor.count {
		return nil
	}
	if cursor.ordinal&4095 == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	ordinal := cursor.ordinal
	key, value, err := decodeDeltaCheckpointRecord(cursor.records, ordinal)
	if err != nil {
		return err
	}
	cursor.ordinal++
	if bytes.Compare(key[:], cursor.upper[:]) > 0 {
		return nil
	}
	if cursor.havePrev && bytes.Compare(cursor.previous[:], key[:]) >= 0 {
		return fmt.Errorf("accountsdb: delta checkpoint records are not strictly sorted at ordinal %d", ordinal)
	}
	if routed := cursor.router.Shard(key); routed != cursor.shardID {
		return fmt.Errorf(
			"accountsdb: delta checkpoint key %x routes to shard %d, selected by shard %d",
			key, routed, cursor.shardID,
		)
	}
	cursor.key = key
	cursor.value = value
	cursor.previous = key
	cursor.havePrev = true
	cursor.valid = true
	return nil
}

type productionBaseEnumerationCursor struct {
	file     *os.File
	path     string
	fileInfo os.FileInfo
	seal     shardedBaseScanSeal
	reader   *bufio.Reader
	metadata shardedBaseMetadata
	catalog  *PersistentExtentCatalog
	router   PersistentIndexShardRouter
	ordinal  uint64
	upper    solana.PublicKey
	key      solana.PublicKey
	entry    AccountIndexEntry
	previous solana.PublicKey
	havePrev bool
	valid    bool
	closed   bool
}

// newProductionBaseEnumerationCursor opens under the shard lifecycle lock but
// releases it before the first caller-controlled visit. The snapshot's root
// generation pin owns the base resource for the cursor's full lifetime.
func newProductionBaseEnumerationCursor(
	ctx context.Context,
	shard *ShardedStreamBaseShard,
	lower solana.PublicKey,
	upper solana.PublicKey,
) (_ *productionBaseEnumerationCursor, retErr error) {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	catalog := shard.catalog.Load()
	if shard.closed || catalog == nil {
		return nil, fmt.Errorf("%w: closed shard", ErrInvalidShardedStreamBase)
	}
	file, err := shard.openScanLockedContext(ctx)
	if err != nil {
		return nil, err
	}
	keepFile := false
	defer func() {
		if !keepFile {
			retErr = errors.Join(retErr, file.Close())
		}
	}()
	var encodedHeader [shardedBaseMetadataSize]byte
	if _, err := file.ReadAt(encodedHeader[:], 0); err != nil {
		return nil, fmt.Errorf("%w: reread range-scan header: %v", ErrInvalidShardedStreamBase, err)
	}
	metadata, err := decodeShardedBaseMetadata(
		encodedHeader[:], shardedBaseScanMagic, shardedBaseScanRecordSize,
	)
	if err != nil {
		return nil, err
	}
	if metadata != shard.scanMetadata {
		return nil, fmt.Errorf("%w: range-scan header changed", ErrInvalidShardedStreamBase)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("accountsdb: stat sharded base range scan %s: %w", shard.scanPath, err)
	}
	wantSize := int64(shardedBaseMetadataSize) + int64(metadata.KeyCount)*shardedBaseScanRecordSize
	if !info.Mode().IsRegular() || info.Size() != wantSize {
		return nil, fmt.Errorf("%w: range-scan file size changed", ErrInvalidShardedStreamBase)
	}
	start, err := shardedBaseLowerBound(ctx, file, metadata.KeyCount, lower)
	if err != nil {
		return nil, err
	}
	offset := int64(shardedBaseMetadataSize) + int64(start)*shardedBaseScanRecordSize
	cursor := &productionBaseEnumerationCursor{
		file: file, path: shard.scanPath, fileInfo: info,
		seal:     shard.scanSeal,
		reader:   bufio.NewReaderSize(io.NewSectionReader(file, offset, wantSize-offset), productionEnumerationBaseBufferBytes),
		metadata: metadata, catalog: catalog, router: shard.router,
		ordinal: start, upper: upper,
	}
	if err := cursor.advance(ctx); err != nil {
		return nil, err
	}
	keepFile = true
	return cursor, nil
}

func (cursor *productionBaseEnumerationCursor) advance(ctx context.Context) error {
	cursor.valid = false
	if cursor.ordinal >= cursor.metadata.KeyCount {
		return nil
	}
	if cursor.ordinal&4095 == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	ordinal := cursor.ordinal
	var encoded [shardedBaseScanRecordSize]byte
	if _, err := io.ReadFull(cursor.reader, encoded[:]); err != nil {
		return fmt.Errorf(
			"%w: read shard %d range record %d: %v",
			ErrInvalidShardedStreamBase, cursor.metadata.ShardID, ordinal, err,
		)
	}
	cursor.ordinal++
	var key solana.PublicKey
	copy(key[:], encoded[:32])
	if bytes.Compare(key[:], cursor.upper[:]) > 0 {
		return nil
	}
	if cursor.havePrev && bytes.Compare(cursor.previous[:], key[:]) >= 0 {
		return fmt.Errorf(
			"%w: range records are not strictly sorted at ordinal %d",
			ErrInvalidShardedStreamBase, ordinal,
		)
	}
	if routed := cursor.router.Shard(key); routed != cursor.metadata.ShardID {
		return fmt.Errorf(
			"%w: range key %x routes to %d, file is shard %d",
			ErrInvalidShardedStreamBase, key, routed, cursor.metadata.ShardID,
		)
	}
	entry, err := cursor.catalog.UnpackAccountIndexEntry(uint48(encoded[32:]))
	if err != nil {
		return fmt.Errorf("%w: range record %d locator: %v", ErrInvalidShardedStreamBase, ordinal, err)
	}
	cursor.key = key
	cursor.entry = entry
	cursor.previous = key
	cursor.havePrev = true
	cursor.valid = true
	return nil
}

func (cursor *productionBaseEnumerationCursor) Close() error {
	if cursor == nil || cursor.closed {
		return nil
	}
	cursor.closed = true
	if cursor.file == nil {
		return nil
	}
	stableErr := validateStableRegularFile(cursor.file, cursor.path, cursor.fileInfo)
	if stableErr == nil && !cursor.seal.isZero() {
		current, err := shardedBaseScanSealFromFile(cursor.file, cursor.seal.SHA256)
		if err != nil {
			stableErr = err
		} else if current != cursor.seal {
			stableErr = fmt.Errorf("exact sidecar strong identity changed while enumerating")
		}
	}
	closeErr := cursor.file.Close()
	cursor.file = nil
	if stableErr != nil {
		stableErr = fmt.Errorf("%w: base range sidecar changed while enumerating: %v", ErrInvalidShardedStreamBase, stableErr)
	}
	return errors.Join(stableErr, closeErr)
}

// scanRange enumerates exact sorted checkpoint records within inclusive key
// bounds. Every selected record's CRC is verified while the checkpoint's mmap
// lifetime is pinned by its read lock.
func (checkpoint *DeltaCheckpoint) scanRange(
	ctx context.Context,
	lower solana.PublicKey,
	upper solana.PublicKey,
	visit func(solana.PublicKey, deltaIndexValue) error,
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil delta checkpoint scan context")
	}
	if visit == nil {
		return errors.New("accountsdb: nil delta checkpoint scan visitor")
	}
	if bytes.Compare(lower[:], upper[:]) > 0 {
		return errors.New("accountsdb: invalid delta checkpoint scan range")
	}
	if checkpoint == nil {
		return nil
	}
	checkpoint.mu.RLock()
	defer checkpoint.mu.RUnlock()
	if checkpoint.closed || checkpoint.records == nil {
		return errors.New("accountsdb: delta checkpoint is closed")
	}
	ordinal, err := checkpoint.lowerBoundPinned(ctx, lower)
	if err != nil {
		return err
	}
	var previous solana.PublicKey
	havePrevious := false
	for ; ordinal < checkpoint.recordCount; ordinal++ {
		if ordinal&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		key, value, err := decodeDeltaCheckpointRecord(checkpoint.records, ordinal)
		if err != nil {
			return err
		}
		if bytes.Compare(key[:], upper[:]) > 0 {
			break
		}
		if bytes.Compare(key[:], lower[:]) < 0 {
			return fmt.Errorf("accountsdb: delta checkpoint lower-bound search returned an earlier key at ordinal %d", ordinal)
		}
		if havePrevious && bytes.Compare(previous[:], key[:]) >= 0 {
			return fmt.Errorf("accountsdb: delta checkpoint records are not strictly sorted at ordinal %d", ordinal)
		}
		if err := visit(key, value); err != nil {
			return err
		}
		previous = key
		havePrevious = true
	}
	return ctx.Err()
}

func (checkpoint *DeltaCheckpoint) lowerBoundPinned(
	ctx context.Context,
	target solana.PublicKey,
) (uint64, error) {
	low, high := uint64(0), checkpoint.recordCount
	for low < high {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		middle := low + (high-low)/2
		key, _, err := decodeDeltaCheckpointRecord(checkpoint.records, middle)
		if err != nil {
			return 0, err
		}
		if bytes.Compare(key[:], target[:]) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low, nil
}

// scanRange uses the exact sorted base sidecar and touches only records in the
// inclusive key interval. Complete artifact SHA-256 and body CRC verification
// happens when the immutable generation is opened; a range scan revalidates
// the cached header and file size, then validates ordering/routing/locators for
// every record it consumes. Recomputing the whole-file CRC here would turn a
// narrow rent partition into a billion-key scan.
func (shard *ShardedStreamBaseShard) scanRange(
	ctx context.Context,
	lower solana.PublicKey,
	upper solana.PublicKey,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) (retErr error) {
	if ctx == nil {
		return errors.New("accountsdb: nil sharded base range context")
	}
	if visit == nil {
		return errors.New("accountsdb: nil sharded base range visitor")
	}
	if bytes.Compare(lower[:], upper[:]) > 0 {
		return errors.New("accountsdb: invalid sharded base scan range")
	}
	if shard == nil {
		return fmt.Errorf("%w: closed shard", ErrInvalidShardedStreamBase)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	catalog := shard.catalog.Load()
	if catalog == nil {
		return fmt.Errorf("%w: closed shard", ErrInvalidShardedStreamBase)
	}
	scan, err := shard.openScanLockedContext(ctx)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, shard.validateOpenScanIdentity(scan), scan.Close())
	}()
	var encodedHeader [shardedBaseMetadataSize]byte
	if _, err := scan.ReadAt(encodedHeader[:], 0); err != nil {
		return fmt.Errorf("%w: reread range-scan header: %v", ErrInvalidShardedStreamBase, err)
	}
	metadata, err := decodeShardedBaseMetadata(
		encodedHeader[:], shardedBaseScanMagic, shardedBaseScanRecordSize,
	)
	if err != nil {
		return err
	}
	if metadata != shard.scanMetadata {
		return fmt.Errorf("%w: range-scan header changed", ErrInvalidShardedStreamBase)
	}
	info, err := scan.Stat()
	if err != nil {
		return fmt.Errorf("accountsdb: stat sharded base range scan %s: %w", shard.scanPath, err)
	}
	wantSize := int64(shardedBaseMetadataSize) + int64(metadata.KeyCount)*shardedBaseScanRecordSize
	if !info.Mode().IsRegular() || info.Size() != wantSize {
		return fmt.Errorf("%w: range-scan file size changed", ErrInvalidShardedStreamBase)
	}

	start, err := shardedBaseLowerBound(ctx, scan, metadata.KeyCount, lower)
	if err != nil {
		return err
	}
	offset := int64(shardedBaseMetadataSize) + int64(start)*shardedBaseScanRecordSize
	reader := bufio.NewReaderSize(io.NewSectionReader(scan, offset, wantSize-offset), 1<<20)
	var encoded [shardedBaseScanRecordSize]byte
	var previous solana.PublicKey
	havePrevious := false
	for ordinal := start; ordinal < metadata.KeyCount; ordinal++ {
		if ordinal&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if _, err := io.ReadFull(reader, encoded[:]); err != nil {
			return fmt.Errorf("%w: read shard %d range record %d: %v", ErrInvalidShardedStreamBase, metadata.ShardID, ordinal, err)
		}
		var key solana.PublicKey
		copy(key[:], encoded[:32])
		if bytes.Compare(key[:], upper[:]) > 0 {
			break
		}
		if bytes.Compare(key[:], lower[:]) < 0 {
			return fmt.Errorf("%w: range lower-bound search returned an earlier key", ErrInvalidShardedStreamBase)
		}
		if havePrevious && bytes.Compare(previous[:], key[:]) >= 0 {
			return fmt.Errorf("%w: range records are not strictly sorted at ordinal %d", ErrInvalidShardedStreamBase, ordinal)
		}
		if routed := shard.router.Shard(key); routed != metadata.ShardID {
			return fmt.Errorf(
				"%w: range key %x routes to %d, file is shard %d",
				ErrInvalidShardedStreamBase, key, routed, metadata.ShardID,
			)
		}
		entry, err := catalog.UnpackAccountIndexEntry(uint48(encoded[32:]))
		if err != nil {
			return fmt.Errorf("%w: range record %d locator: %v", ErrInvalidShardedStreamBase, ordinal, err)
		}
		if err := visit(key, entry); err != nil {
			return err
		}
		previous = key
		havePrevious = true
	}
	return ctx.Err()
}

func shardedBaseLowerBound(
	ctx context.Context,
	file interface {
		ReadAt([]byte, int64) (int, error)
	},
	count uint64,
	target solana.PublicKey,
) (uint64, error) {
	low, high := uint64(0), count
	var encoded [shardedBaseScanRecordSize]byte
	for low < high {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		middle := low + (high-low)/2
		offset := int64(shardedBaseMetadataSize) + int64(middle)*shardedBaseScanRecordSize
		if _, err := file.ReadAt(encoded[:], offset); err != nil {
			return 0, fmt.Errorf("%w: read range-search record %d: %v", ErrInvalidShardedStreamBase, middle, err)
		}
		if bytes.Compare(encoded[:32], target[:]) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low, nil
}

// ScanKeysBetweenPrefixes is the AccountsDb-level streaming API for exact
// range enumeration. Prefer it to either materializing compatibility method.
func (accountsDb *AccountsDb) ScanKeysBetweenPrefixes(
	ctx context.Context,
	startPrefix uint64,
	endPrefix uint64,
	visit func(solana.PublicKey) error,
) error {
	if accountsDb == nil || accountsDb.ProductionIndex == nil {
		return errors.New("accountsdb: exact range enumeration requires the V2 production account index")
	}
	if visit == nil {
		return errors.New("accountsdb: nil account-key range visitor")
	}
	release, err := accountsDb.ProductionIndex.acquireEnumerationPermit(ctx)
	if err != nil {
		return err
	}
	defer release()
	// A rare oversized live fold may use several physical WAL frames while
	// pendingFold supplies point/batch readers with one logical epoch. Capture
	// the range snapshot under the complete index-write transaction fence so it
	// cannot land between frames. The retained snapshot is self-contained, so
	// release the fence before any disk I/O or caller-controlled visitor work.
	// Ordinary point and batch account loading remains lock-free with respect to
	// this mutex.
	accountsDb.accountIndexWriteMu.Lock()
	snapshot, err := accountsDb.ProductionIndex.captureEnumerationSnapshot(ctx, startPrefix, endPrefix)
	accountsDb.accountIndexWriteMu.Unlock()
	if err != nil {
		return err
	}
	defer snapshot.Close()
	return snapshot.scan(
		ctx,
		func(key solana.PublicKey, _ AccountIndexEntry) error { return visit(key) },
	)
}

// KeysBetweenPrefixesContext is the materializing counterpart of
// ScanKeysBetweenPrefixes. Prefer the streaming method when a wide range can
// be selected, because the returned slice is necessarily proportional to the
// number of live accounts.
func (accountsDb *AccountsDb) KeysBetweenPrefixesContext(
	ctx context.Context,
	startPrefix uint64,
	endPrefix uint64,
) ([]solana.PublicKey, error) {
	keys := make([]solana.PublicKey, 0)
	err := accountsDb.ScanKeysBetweenPrefixes(
		ctx, startPrefix, endPrefix,
		func(key solana.PublicKey) error {
			keys = append(keys, key)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// AllKeysContext materializes every public key. At mainnet scale this can use
// tens of gigabytes merely for the result; it exists for compatibility and
// diagnostics, while production callers should stream ScanKeysBetweenPrefixes.
func (accountsDb *AccountsDb) AllKeysContext(ctx context.Context) ([][]byte, error) {
	keys := make([][]byte, 0)
	err := accountsDb.ScanKeysBetweenPrefixes(
		ctx, 0, ^uint64(0),
		func(key solana.PublicKey) error {
			encoded := make([]byte, len(key))
			copy(encoded, key[:])
			keys = append(keys, encoded)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return keys, nil
}
