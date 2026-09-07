package snapshot

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

const (
	dioAlign      = 4096
	shardBufSize  = 8 << 20 // staging buffer flushed to disk as one write
	shardBufCount = 4       // buffers per writer: 1 filling + up to 3 draining
)

func alignUp(n int) int { return (n + dioAlign - 1) &^ (dioAlign - 1) }

// alignedBuf returns a slice of length size whose backing array starts on a
// dioAlign boundary, as required for O_DIRECT.
func alignedBuf(size int) []byte {
	b := make([]byte, size+dioAlign)
	if off := int(uintptr(unsafe.Pointer(&b[0])) % dioAlign); off != 0 {
		b = b[dioAlign-off:]
	}
	return b[:size]
}

// syncBuildMetadata fsyncs the small files a completed build writes next to the
// index, and their directory, before the build is reported complete.
func syncBuildMetadata(accountsDbDir string) error {
	for _, name := range []string{"num_shards", "largest_file_id", "bootstrap_high_file_id", "bank_hash", "stake_pubkeys.idx"} {
		f, err := os.Open(filepath.Join(accountsDbDir, name))
		if os.IsNotExist(err) {
			continue // num_shards is absent for legacy single-directory builds
		}
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return fmt.Errorf("sync %s: %w", name, err)
		}
	}
	return syncDir(accountsDbDir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// writeShardMetadata records the shard count the DB was built with. Its presence
// also marks a complete build; OpenDb refuses to open a DB without it.
func writeShardMetadata(metaDir string, numShards int) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(numShards))
	return os.WriteFile(filepath.Join(metaDir, "num_shards"), buf[:], 0644)
}

type shardWriter struct {
	f      *os.File
	direct bool

	mu     sync.Mutex // guards cur/buflen/offset and enqueue ordering
	cur    []byte
	buflen int
	offset uint64

	free   chan []byte
	queue  chan wItem
	wg     sync.WaitGroup
	queued atomic.Int64 // bytes accepted but not yet written (backpressure signal)

	errMu     sync.Mutex
	err       error
	closed    bool // guarded by mu; close is terminal
	closeOnce sync.Once
	closeErr  error
}

type wItem struct {
	b      []byte
	n      int
	pooled bool // return b to the free pool after writing
}

func (s *shardWriter) newBuf() []byte {
	if s.direct {
		return alignedBuf(shardBufSize)
	}
	return make([]byte, shardBufSize)
}

func newShardWriter(path string, direct bool) (*shardWriter, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if direct {
		flag, err := directIOFlag()
		if err != nil {
			return nil, err
		}
		flags |= flag
	}
	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, err
	}
	s := &shardWriter{
		f:      f,
		direct: direct,
		free:   make(chan []byte, shardBufCount),
		queue:  make(chan wItem, shardBufCount+4),
	}
	s.cur = s.newBuf()
	for i := 0; i < shardBufCount-1; i++ {
		s.free <- s.newBuf()
	}
	s.wg.Add(1)
	go s.writeLoop()
	return s, nil
}

func (s *shardWriter) writeLoop() {
	defer s.wg.Done()
	for it := range s.queue {
		if it.n > 0 && s.loadErr() == nil {
			if _, err := s.f.Write(it.b[:it.n]); err != nil {
				s.storeErr(err)
			}
		}
		s.queued.Add(int64(-it.n))
		if it.pooled {
			s.free <- it.b
		}
	}
}

// append copies blob into the staging buffer and returns the base offset where its
// bytes will land in the file. Safe for concurrent callers; it serializes internally.
func (s *shardWriter) append(blob []byte) (uint64, error) {
	n := len(blob)
	padded := n
	if s.direct {
		padded = alignUp(n)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, os.ErrClosed
	}
	base := s.offset

	// len(s.cur), not cap: alignedBuf over-allocates, so cap can exceed the logical
	// buffer size and let an oversized record slip into the in-buffer path.
	if padded > len(s.cur) {
		// Record larger than a staging buffer: flush current, write it standalone.
		// blob is the caller's pooled tar buffer, which the index builder recycles as
		// soon as it finishes parsing — before this async write is guaranteed to run.
		// So we MUST queue a private copy; aliasing blob let the buffer be reused and
		// overwritten before the writer flushed it, corrupting every account in this
		// append-vec (~wrong pubkey on read-back).
		s.flushLocked()
		var b []byte
		if s.direct {
			b = alignedBuf(padded)
			copy(b, blob)
			clear(b[n:])
		} else {
			b = make([]byte, n)
			copy(b, blob)
		}
		s.offset += uint64(padded)
		s.queued.Add(int64(padded))
		s.queue <- wItem{b: b, n: padded, pooled: false}
		s.mu.Unlock()
		return base, s.loadErr()
	}

	if s.buflen+padded > len(s.cur) {
		s.flushLocked()
	}
	copy(s.cur[s.buflen:], blob)
	if s.direct {
		clear(s.cur[s.buflen+n : s.buflen+padded])
	}
	s.buflen += padded
	s.offset += uint64(padded)
	s.queued.Add(int64(padded))
	s.mu.Unlock()
	return base, s.loadErr()
}

// flushLocked hands the current buffer to the writer goroutine and swaps in a fresh
// one. Called with s.mu held; blocks (backpressure) when no free buffer is available.
func (s *shardWriter) flushLocked() {
	if s.buflen == 0 {
		return
	}
	s.queue <- wItem{b: s.cur, n: s.buflen, pooled: true}
	s.cur = <-s.free
	s.buflen = 0
}

func (s *shardWriter) close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.flushLocked()
		s.closed = true
		close(s.queue)
		s.mu.Unlock()
		s.wg.Wait()
		// The build is recorded as durable once indexing finishes, so the data
		// file and its directory entry must be on disk before that.
		s.closeErr = s.f.Sync()
		if err := s.f.Close(); s.closeErr == nil {
			s.closeErr = err
		}
		if s.closeErr == nil {
			s.closeErr = syncDir(filepath.Dir(s.f.Name()))
		}
		if werr := s.loadErr(); werr != nil {
			s.closeErr = werr
		}
		// The writer may remain reachable through worker closures until bootstrap
		// returns. Its 32 MiB of staging buffers are no longer needed during sorting.
		s.mu.Lock()
		s.cur = nil
		close(s.free)
		for range s.free {
		}
		s.mu.Unlock()
	})
	return s.closeErr
}

func (s *shardWriter) loadErr() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *shardWriter) storeErr(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

// shardBigFiles owns one coalesced big file ("data") per shard and picks a shard
// per record by fewest in-flight (queued) bytes, so records flow to whichever disk
// is draining fastest. With several shards, a shard whose filesystem is down to
// SnapshotShardMinFreeBytes stops receiving records, so disks of different sizes
// fill to their capacity instead of failing the unpack when the smallest is full.
type shardBigFiles struct {
	data    []*shardWriter
	written []atomic.Int64

	dirs      []string
	reserve   int64
	headroom  []atomic.Int64 // free minus queued minus reserve at last refresh, less writes since
	stop      chan struct{}
	done      chan struct{}
	warnOnce  sync.Once
	closeOnce sync.Once
}

var (
	// Test hooks; production reads statfs every refresh interval.
	shardAvailableBytes      = accountsdb.AvailableDiskBytes
	shardFreeRefreshInterval = 250 * time.Millisecond
)

func openShardBigFiles(shardDirs []string) (*shardBigFiles, error) {
	if err := accountsdb.ValidateStorageDirectories(shardDirs); err != nil {
		return nil, err
	}
	direct := SnapshotDirectIO
	mlog.Log.Infof("snapshot shard writers: %d shard(s), O_DIRECT=%v", len(shardDirs), direct)
	sb := &shardBigFiles{
		data:     make([]*shardWriter, len(shardDirs)),
		written:  make([]atomic.Int64, len(shardDirs)),
		dirs:     shardDirs,
		reserve:  int64(min(SnapshotShardMinFreeBytes, math.MaxInt64)),
		headroom: make([]atomic.Int64, len(shardDirs)),
	}
	for i, dir := range shardDirs {
		var err error
		if sb.data[i], err = newShardWriter(filepath.Join(dir, "data"), direct); err != nil {
			sb.close()
			return nil, err
		}
	}
	if len(shardDirs) > 1 {
		// One shard always takes every record; only multi-shard placement
		// needs to know how much room each filesystem has left.
		sb.refreshHeadroom()
		for i, dir := range shardDirs {
			mlog.Log.Infof("snapshot shard %d: %s, %d MiB usable above the %d MiB reserve", i, dir, max(sb.headroom[i].Load(), 0)>>20, sb.reserve>>20)
		}
		sb.stop, sb.done = make(chan struct{}), make(chan struct{})
		go sb.refreshLoop()
	}
	return sb, nil
}

func (sb *shardBigFiles) refreshLoop() {
	defer close(sb.done)
	ticker := time.NewTicker(shardFreeRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sb.stop:
			return
		case <-ticker.C:
			sb.refreshHeadroom()
		}
	}
}

// refreshHeadroom re-reads free space. Bytes accepted but not yet written are
// not visible to statfs, so they are subtracted explicitly. Other writers on
// the same filesystem (index logs and SSTs on the primary) are picked up here.
func (sb *shardBigFiles) refreshHeadroom() {
	for i, dir := range sb.dirs {
		free, err := shardAvailableBytes(dir)
		if err != nil {
			// Unknown capacity must not stop the unpack; ENOSPC still reports a
			// genuinely full disk.
			sb.headroom[i].Store(math.MaxInt64)
			continue
		}
		sb.headroom[i].Store(int64(min(free, math.MaxInt64)) - sb.data[i].queued.Load() - sb.reserve)
	}
}

func (sb *shardBigFiles) n() uint64 { return uint64(len(sb.data)) }

func (sb *shardBigFiles) choose() int {
	if len(sb.data) == 1 {
		return 0
	}
	// Prefer the disk with the least queued bytes among those with room left
	best, bestQ := -1, int64(math.MaxInt64)
	anyQueued := false
	for i := range sb.data {
		if sb.headroom[i].Load() <= 0 {
			continue
		}
		q := sb.data[i].queued.Load()
		if q > 0 {
			anyQueued = true
		}
		if q < bestQ {
			best, bestQ = i, q
		}
	}
	if best < 0 {
		// Every disk is at its reserve: keep writing where the most room is left,
		// and let the filesystem decide whether it actually fits.
		sb.warnOnce.Do(func() {
			mlog.Log.Warnf("snapshot shard writers: every accounts directory is below its %d MiB free-space reserve", sb.reserve>>20)
		})
		best, bestH := 0, int64(math.MinInt64)
		for i := range sb.data {
			if h := sb.headroom[i].Load(); h > bestH {
				best, bestH = i, h
			}
		}
		return best
	}
	if anyQueued {
		return best
	}
	// No queued bytes so spread evenly by cumulative bytes instead.
	best, bestW := -1, int64(math.MaxInt64)
	for i := range sb.data {
		if sb.headroom[i].Load() <= 0 {
			continue
		}
		if w := sb.written[i].Load(); w < bestW {
			best, bestW = i, w
		}
	}
	return best
}

// write appends blob to the chosen shard's big file and returns the shard-encoded
// fileId (segment 0) and the offset within that file.
func (sb *shardBigFiles) write(blob []byte) (fileId, base uint64, err error) {
	shard := sb.choose()
	sb.written[shard].Add(int64(len(blob)))
	sb.headroom[shard].Add(-int64(len(blob)))
	base, err = sb.data[shard].append(blob)
	if err != nil {
		return 0, 0, err
	}
	return uint64(shard), base, nil
}

func (sb *shardBigFiles) close() error {
	sb.closeOnce.Do(func() {
		if sb.stop != nil {
			close(sb.stop)
			<-sb.done
		}
	})
	var firstErr error
	for _, w := range sb.data {
		if w == nil {
			continue
		}
		if err := w.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
