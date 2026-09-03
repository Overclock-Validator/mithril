package snapshot

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var MaxConcurrentFlushers int = DefaultSnapshotMaxConcurrentFlushers

const (
	// DefaultSnapshotShardSortChunkMB bounds the account-index records held
	// by one shard flusher. Eight default flushers therefore use at most roughly
	// 512 MiB for their primary sort arrays, independent of snapshot size or key
	// distribution.
	DefaultSnapshotShardSortChunkMB = 64
	shardMergeFanIn                 = 64
	shardMergeReadBufferBytes       = 64 << 10
)

// SnapshotShardSortChunkMB is snapshotted by each shard flush. It is exported
// for deployment tuning and tests; non-positive values use the default.
var SnapshotShardSortChunkMB = DefaultSnapshotShardSortChunkMB

type shardRequest struct {
	k solana.PublicKey
	v accountsdb.AccountIndexEntry
}

// ShardProgressCallback is called with (bytesDone, totalBytes) to report shard flush progress
type ShardProgressCallback func(bytesDone, totalBytes int64)

// ShardLogger manages multiple sharded log files
type ShardLogger struct {
	shards     []*shard
	filePrefix string
	wg         *sync.WaitGroup
	flushSem   *semaphore.Weighted

	// Progress tracking
	totalBytes atomic.Int64 // total bytes written to shard logs
	bytesDone  atomic.Int64 // bytes flushed to cache
	onProgress ShardProgressCallback

	// closed flag to prevent sends after Close is called (defensive)
	closed atomic.Bool
	sendMu sync.RWMutex
	stop   sync.Once

	finishMu  sync.Mutex
	finalized bool
	aborted   bool
	finishErr error
}

// shard represents a single log shard
type shard struct {
	id       int
	writer   *bufio.Writer
	file     *os.File
	requests chan shardRequest
	logSize  int
	flushSem *semaphore.Weighted
	parent   *ShardLogger // parent for progress reporting
	writeErr error
}

// NewShardLogger creates a new ShardLogger with the specified number of
// shards for logging entries. Entries are flushed to shardedSetter
// when log reaches a certain size or on shard closure.
func NewShardLogger(numShards int, filePrefix string) (*ShardLogger, error) {
	if numShards <= 0 {
		return nil, fmt.Errorf("snapshot shard count must be positive, got %d", numShards)
	}
	if numShards > 1000 {
		return nil, fmt.Errorf("snapshot shard count %d exceeds maximum 1000", numShards)
	}
	directoryInfo, err := os.Lstat(filePrefix)
	if err != nil {
		return nil, fmt.Errorf("inspect snapshot shard directory %s: %w", filePrefix, err)
	}
	if !directoryInfo.IsDir() {
		return nil, fmt.Errorf("snapshot shard directory %s is not a real directory", filePrefix)
	}
	flushers := snapshotMaxConcurrentFlushers()
	sl := &ShardLogger{
		shards:     make([]*shard, numShards),
		filePrefix: filePrefix,
		wg:         &sync.WaitGroup{},
		flushSem:   semaphore.NewWeighted(int64(flushers)),
	}

	for i := range numShards {
		opened, err := newShard(i, filePrefix, sl.flushSem, sl)
		if err != nil {
			var cleanupErr error
			for _, shard := range sl.shards[:i] {
				cleanupErr = errors.Join(cleanupErr, shard.file.Close())
				cleanupErr = errors.Join(cleanupErr, os.Remove(shard.file.Name()))
			}
			return nil, errors.Join(err, cleanupErr)
		}
		sl.shards[i] = opened
	}

	// Do not start a request goroutine until every shard file exists. A setup
	// failure can therefore unwind synchronously without leaking goroutines or
	// leaving a partially live logger behind.
	sl.wg.Add(numShards)
	for i := range numShards {
		go sl.shards[i].processRequests(sl.wg)
	}

	return sl, nil
}

// SetProgressCallback sets a callback to receive progress updates during shard flushes.
// The callback receives (bytesDone, totalBytes) and is called as bytes are flushed to cache.
func (sl *ShardLogger) SetProgressCallback(cb ShardProgressCallback) {
	sl.onProgress = cb
}

// TotalBytes returns the total bytes written to shard logs
func (sl *ShardLogger) TotalBytes() int64 {
	return sl.totalBytes.Load()
}

// BytesDone returns the bytes that have been flushed to cache
func (sl *ShardLogger) BytesDone() int64 {
	return sl.bytesDone.Load()
}

// newShard creates a new shard with the given ID
func newShard(id int, filePrefix string, flushSem *semaphore.Weighted, parent *ShardLogger) (*shard, error) {
	filename := filepath.Join(filePrefix, fmt.Sprintf("%03d", id))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create snapshot shard %d at %s: %w", id, filename, err)
	}

	s := &shard{
		id:       id,
		writer:   bufio.NewWriter(file),
		file:     file,
		requests: make(chan shardRequest, 100),
		flushSem: flushSem,
		parent:   parent,
	}

	return s, nil
}

const (
	vlen                   = /*Slot*/ 8 + /*FileId*/ 8 + /*Offset*/ 8
	shardContextCheckEvery = 4096
)

// processRequests handles incoming requests for a shard
func (s *shard) processRequests(wg *sync.WaitGroup) {
	defer wg.Done()
	var kBytes [32]byte
	var vBytes [vlen]byte
	for req := range s.requests {
		if s.writeErr != nil {
			continue
		}
		kBytes = [32]byte(req.k)
		if _, err := s.writer.Write(kBytes[:]); err != nil {
			s.writeErr = fmt.Errorf("write shard %d public key: %w", s.id, err)
			continue
		}
		req.v.Marshal(&vBytes)
		if _, err := s.writer.Write(vBytes[:]); err != nil {
			s.writeErr = fmt.Errorf("write shard %d account-index entry: %w", s.id, err)
			continue
		}

		bytesWritten := int64(len(req.k) + vlen)
		s.logSize += int(bytesWritten)

		// Track total bytes for progress reporting and notify callback
		if s.parent != nil {
			total := s.parent.totalBytes.Add(bytesWritten)
			if s.parent.onProgress != nil {
				// Notify with bytesDone=0 during streaming (before flush)
				// The callback can use totalBytes to show indexing progress
				s.parent.onProgress(0, total)
			}
		}
	}
}

func (s *shard) logToRun(ctx context.Context) error {
	err := s.flushSem.Acquire(ctx, 1)
	if err != nil {
		_ = s.file.Close()
		return fmt.Errorf("acquiring flush semaphore: %w", err)
	}
	defer s.flushSem.Release(1)
	closeRaw := func(primary error) error {
		if closeErr := s.file.Close(); closeErr != nil {
			if primary == nil {
				return closeErr
			}
			return fmt.Errorf("%w (also closing shard log: %v)", primary, closeErr)
		}
		return primary
	}
	if s.writeErr != nil {
		return closeRaw(s.writeErr)
	}
	// Flush and close the append-only raw log before sorting it. Staging files
	// are disposable and are rebuilt after a crash, so they deliberately avoid
	// forcing the full snapshot through fsync.
	if err := s.writer.Flush(); err != nil {
		return closeRaw(fmt.Errorf("failed to flush writer: %w", err))
	}
	filename := s.file.Name()
	if err := closeRaw(nil); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	// Read the raw log in bounded chunks. Each chunk is independently sorted and
	// deduplicated, then a bounded-fan-in merge produces the shard's final run.
	// This keeps memory independent of both snapshot size and key-prefix skew.
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to reopen file for reading: %w", err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", filename, err)
	}
	size := fileInfo.Size()

	const recordSize = int64(accountsdb.StreamIndexRunRecordSize)
	if rem := size % recordSize; rem != 0 {
		return fmt.Errorf("filename=%s had (size=%d) %% (recordSize=%d) = %d", filename, size, recordSize, rem)
	}
	runFilename := filename + ".run"
	partialFilename := runFilename + ".partial"
	defer cleanupShardSortTemps(filename, partialFilename)
	chunks, err := writeShardSortChunks(ctx, file, filename, size, s.parent)
	if err != nil {
		return err
	}
	// Once every input record is represented in a sorted chunk, retaining the
	// raw log would add a third full copy during merge passes. The entire work
	// directory is disposable on failure, so close and remove it now.
	if err := file.Close(); err != nil {
		return fmt.Errorf("close sorted raw shard log %s: %w", filename, err)
	}
	if err := os.Remove(filename); err != nil {
		return fmt.Errorf("remove sorted raw shard log %s: %w", filename, err)
	}
	if err := mergeShardSortChunks(ctx, filename, partialFilename, chunks); err != nil {
		return err
	}
	if err := os.Rename(partialFilename, runFilename); err != nil {
		return fmt.Errorf("publish run %s: %w", runFilename, err)
	}
	s.logSize = 0

	return nil
}

func shardSortChunkBytes() int64 {
	megabytes := SnapshotShardSortChunkMB
	if megabytes <= 0 {
		megabytes = DefaultSnapshotShardSortChunkMB
	}
	maxMiB := int64(^uint64(0)>>1) >> 20
	if int64(megabytes) > maxMiB {
		return int64(^uint64(0) >> 1)
	}
	budget := int64(megabytes) << 20
	if budget < accountsdb.StreamIndexRunRecordSize {
		budget = accountsdb.StreamIndexRunRecordSize
	}
	return budget
}

func compareShardRequests(a, b shardRequest) int {
	if c := bytes.Compare(a.k[:], b.k[:]); c != 0 {
		return c
	}
	// Newest first within one public key. File IDs increase as appendvecs are
	// created, and offsets increase within an appendvec, making equal-slot ties
	// deterministic too.
	if a.v.Slot != b.v.Slot {
		if a.v.Slot > b.v.Slot {
			return -1
		}
		return 1
	}
	if a.v.FileId != b.v.FileId {
		if a.v.FileId > b.v.FileId {
			return -1
		}
		return 1
	}
	if a.v.Offset > b.v.Offset {
		return -1
	}
	if a.v.Offset < b.v.Offset {
		return 1
	}
	return 0
}

func writeShardSortChunks(
	ctx context.Context,
	file *os.File,
	rawPath string,
	size int64,
	parent *ShardLogger,
) ([]string, error) {
	const recordSize = int64(accountsdb.StreamIndexRunRecordSize)
	totalRecords := size / recordSize
	chunkRecords := shardSortChunkBytes() / recordSize
	if chunkRecords < 1 {
		chunkRecords = 1
	}
	if chunkRecords > int64(^uint(0)>>1) {
		chunkRecords = int64(^uint(0) >> 1)
	}
	chunkRecords = min(chunkRecords, max(totalRecords, int64(1)))
	pairs := make([]shardRequest, int(chunkRecords))
	reader := bufio.NewReaderSize(file, 1<<20)
	chunkCount := (totalRecords + chunkRecords - 1) / chunkRecords
	if chunkCount > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("shard log %s requires too many sort chunks: %d", rawPath, chunkCount)
	}
	chunks := make([]string, 0, int(chunkCount))
	cleanup := true
	defer func() {
		if cleanup {
			for _, path := range chunks {
				_ = os.Remove(path)
			}
		}
	}()

	var encoded [accountsdb.StreamIndexRunRecordSize]byte
	for chunkOrdinal, remaining := 0, totalRecords; remaining > 0; chunkOrdinal++ {
		count := int(min(remaining, chunkRecords))
		for i := 0; i < count; i++ {
			if i%shardContextCheckEvery == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if _, err := io.ReadFull(reader, encoded[:]); err != nil {
				return nil, fmt.Errorf("reading shard log %s: %w", rawPath, err)
			}
			copy(pairs[i].k[:], encoded[:32])
			pairs[i].v.Unmarshal((*[24]byte)(encoded[32:56]))
			if parent != nil {
				done := parent.bytesDone.Add(recordSize)
				if parent.onProgress != nil {
					parent.onProgress(done, parent.totalBytes.Load())
				}
			}
		}
		chunk := pairs[:count]
		slices.SortFunc(chunk, compareShardRequests)
		path := fmt.Sprintf("%s.sort-%06d.partial", rawPath, chunkOrdinal)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove stale shard sort chunk %s: %w", path, err)
		}
		if err := writeSortedShardRequests(ctx, path, chunk); err != nil {
			return nil, err
		}
		chunks = append(chunks, path)
		remaining -= int64(count)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cleanup = false
	return chunks, nil
}

func writeSortedShardRequests(ctx context.Context, path string, pairs []shardRequest) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create shard sort run %s: %w", path, err)
	}
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(path)
		}
	}()
	writer := bufio.NewWriterSize(file, 1<<20)
	var encoded [accountsdb.StreamIndexRunRecordSize]byte
	var previous solana.PublicKey
	havePrevious := false
	for i := range pairs {
		if i%shardContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if havePrevious && pairs[i].k == previous {
			continue
		}
		encodeShardRequest(&encoded, pairs[i])
		if _, err := writer.Write(encoded[:]); err != nil {
			return fmt.Errorf("write shard sort run %s: %w", path, err)
		}
		previous = pairs[i].k
		havePrevious = true
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush shard sort run %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close shard sort run %s: %w", path, err)
	}
	keep = true
	return nil
}

func encodeShardRequest(encoded *[accountsdb.StreamIndexRunRecordSize]byte, request shardRequest) {
	copy(encoded[:32], request.k[:])
	request.v.Marshal((*[24]byte)(encoded[32:56]))
}

type shardMergeReader struct {
	file   *os.File
	reader *bufio.Reader
}

func (reader *shardMergeReader) next() (shardRequest, bool, error) {
	var request shardRequest
	var encoded [accountsdb.StreamIndexRunRecordSize]byte
	_, err := io.ReadFull(reader.reader, encoded[:])
	if err == io.EOF {
		return request, false, nil
	}
	if err != nil {
		return request, false, err
	}
	copy(request.k[:], encoded[:32])
	request.v.Unmarshal((*[24]byte)(encoded[32:56]))
	return request, true, nil
}

type shardMergeItem struct {
	request shardRequest
	reader  int
}

type shardMergeQueue []shardMergeItem

func (queue shardMergeQueue) Len() int { return len(queue) }
func (queue shardMergeQueue) Less(i, j int) bool {
	return compareShardRequests(queue[i].request, queue[j].request) < 0
}
func (queue shardMergeQueue) Swap(i, j int) { queue[i], queue[j] = queue[j], queue[i] }
func (queue *shardMergeQueue) Push(value any) {
	*queue = append(*queue, value.(shardMergeItem))
}
func (queue *shardMergeQueue) Pop() any {
	old := *queue
	last := old[len(old)-1]
	*queue = old[:len(old)-1]
	return last
}

func mergeShardSortChunks(ctx context.Context, rawPath, finalPartial string, chunks []string) error {
	if err := os.Remove(finalPartial); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale run partial %s: %w", finalPartial, err)
	}
	if len(chunks) == 0 {
		file, err := os.OpenFile(finalPartial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("create empty shard run %s: %w", finalPartial, err)
		}
		return file.Close()
	}

	runs := append([]string(nil), chunks...)
	for pass := 0; len(runs) > 1; pass++ {
		next := make([]string, 0, (len(runs)+shardMergeFanIn-1)/shardMergeFanIn)
		for start := 0; start < len(runs); start += shardMergeFanIn {
			end := min(start+shardMergeFanIn, len(runs))
			group := runs[start:end]
			if len(group) == 1 {
				next = append(next, group[0])
				continue
			}
			output := fmt.Sprintf("%s.merge-%03d-%06d.partial", rawPath, pass, len(next))
			if err := os.Remove(output); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove stale shard merge run %s: %w", output, err)
			}
			if err := mergeShardRunGroup(ctx, group, output); err != nil {
				return err
			}
			next = append(next, output)
			for _, input := range group {
				if err := os.Remove(input); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("remove merged shard run %s: %w", input, err)
				}
			}
		}
		runs = next
	}
	if err := os.Rename(runs[0], finalPartial); err != nil {
		return fmt.Errorf("stage final shard run %s: %w", finalPartial, err)
	}
	return nil
}

func mergeShardRunGroup(ctx context.Context, inputs []string, output string) error {
	readers := make([]shardMergeReader, len(inputs))
	defer func() {
		for i := range readers {
			if readers[i].file != nil {
				_ = readers[i].file.Close()
			}
		}
	}()
	queue := make(shardMergeQueue, 0, len(inputs))
	for i, path := range inputs {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open shard merge input %s: %w", path, err)
		}
		readers[i] = shardMergeReader{file: file, reader: bufio.NewReaderSize(file, shardMergeReadBufferBytes)}
		request, ok, err := readers[i].next()
		if err != nil {
			return fmt.Errorf("read shard merge input %s: %w", path, err)
		}
		if ok {
			heap.Push(&queue, shardMergeItem{request: request, reader: i})
		}
	}

	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create shard merge output %s: %w", output, err)
	}
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(output)
		}
	}()
	writer := bufio.NewWriterSize(file, 1<<20)
	var encoded [accountsdb.StreamIndexRunRecordSize]byte
	written := 0
	advance := func(readerIndex int) error {
		request, ok, err := readers[readerIndex].next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&queue, shardMergeItem{request: request, reader: readerIndex})
		}
		return nil
	}
	for queue.Len() > 0 {
		if written%shardContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		item := heap.Pop(&queue).(shardMergeItem)
		best := item.request
		if err := advance(item.reader); err != nil {
			return fmt.Errorf("advance shard merge input %s: %w", inputs[item.reader], err)
		}
		for queue.Len() > 0 && queue[0].request.k == best.k {
			item = heap.Pop(&queue).(shardMergeItem)
			if compareShardRequests(item.request, best) < 0 {
				best = item.request
			}
			if err := advance(item.reader); err != nil {
				return fmt.Errorf("advance shard merge input %s: %w", inputs[item.reader], err)
			}
		}
		encodeShardRequest(&encoded, best)
		if _, err := writer.Write(encoded[:]); err != nil {
			return fmt.Errorf("write shard merge output %s: %w", output, err)
		}
		written++
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush shard merge output %s: %w", output, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close shard merge output %s: %w", output, err)
	}
	keep = true
	return nil
}

func cleanupShardSortTemps(rawPath, finalPartial string) {
	_ = os.Remove(finalPartial)
	for _, pattern := range []string{rawPath + ".sort-*.partial", rawPath + ".merge-*.partial"} {
		paths, _ := filepath.Glob(pattern)
		for _, path := range paths {
			_ = os.Remove(path)
		}
	}
}

// EnqueueRequest adds a request to the appropriate shard
func (sl *ShardLogger) EnqueueRequest(k solana.PublicKey, v accountsdb.AccountIndexEntry) {
	if sl == nil {
		mlog.Log.Errorf("unexpected request sent to a nil shard logger")
		return
	}
	sl.sendMu.RLock()
	defer sl.sendMu.RUnlock()
	if sl.closed.Load() {
		mlog.Log.Errorf("unexpectedly still receiving requests after shard logger is closed!")
		return // Already closed
	}
	keyPrefix := binary.BigEndian.Uint64(k[:8])
	shardSize := math.MaxUint64 / uint64(len(sl.shards))
	shardIdx := int(keyPrefix / shardSize)
	if shardIdx >= len(sl.shards) {
		shardIdx = len(sl.shards) - 1
	}
	sl.shards[shardIdx].requests <- shardRequest{k, v}
}

// Close closes all shards and their files
func (sl *ShardLogger) Close(ctx context.Context) error {
	return sl.CloseWithProgress(ctx, nil)
}

func (sl *ShardLogger) stopRequests() {
	sl.stop.Do(func() {
		sl.sendMu.Lock()
		sl.closed.Store(true)
		for _, shard := range sl.shards {
			close(shard.requests)
		}
		sl.sendMu.Unlock()
	})
	sl.wg.Wait()
}

// Abort drains request goroutines and discards unsorted staging logs. Snapshot
// builders defer it immediately after construction so every early return
// releases descriptors and goroutines without paying to sort unusable input.
// It is a no-op after Close or another Abort has finalized the logger.
func (sl *ShardLogger) Abort() error {
	if sl == nil {
		return nil
	}
	sl.stopRequests()
	sl.finishMu.Lock()
	defer sl.finishMu.Unlock()
	if sl.finalized {
		return nil
	}
	var abortErr error
	for _, shard := range sl.shards {
		abortErr = errors.Join(abortErr, shard.file.Close())
		if err := os.Remove(shard.file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			abortErr = errors.Join(abortErr, err)
		}
	}
	sl.finalized = true
	sl.aborted = true
	sl.finishErr = abortErr
	return abortErr
}

// CloseWithProgress closes all shards with optional progress callback.
// The callback is called after each shard flush completes with (completed, total) counts.
func (sl *ShardLogger) CloseWithProgress(ctx context.Context, onProgress func(completed, total int)) error {
	if sl == nil {
		return errors.New("nil snapshot shard logger")
	}
	sl.stopRequests()
	sl.finishMu.Lock()
	defer sl.finishMu.Unlock()
	if sl.finalized {
		if sl.aborted && sl.finishErr == nil {
			return errors.New("snapshot shard logger was aborted")
		}
		return sl.finishErr
	}

	total := len(sl.shards)
	var completed atomic.Int32

	flushWg := &errgroup.Group{}
	for _, s := range sl.shards {
		flushWg.Go(func() error {
			err := s.logToRun(ctx)
			if onProgress != nil {
				onProgress(int(completed.Add(1)), total)
			}
			return err
		})
	}
	sl.finishErr = flushWg.Wait()
	sl.finalized = true
	return sl.finishErr
}
