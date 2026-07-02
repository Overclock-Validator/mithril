package snapshot

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

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

	errMu sync.Mutex
	err   error
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
		flags |= syscall.O_DIRECT
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
	s.mu.Lock()
	s.flushLocked()
	s.mu.Unlock()
	close(s.queue)
	s.wg.Wait()
	cerr := s.f.Close()
	if werr := s.loadErr(); werr != nil {
		return werr
	}
	return cerr
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
// is draining fastest.
type shardBigFiles struct {
	data    []*shardWriter
	written []atomic.Int64
}

func openShardBigFiles(shardDirs []string) (*shardBigFiles, error) {
	direct := SnapshotDirectIO
	mlog.Log.Infof("snapshot shard writers: %d shard(s), O_DIRECT=%v", len(shardDirs), direct)
	sb := &shardBigFiles{
		data:    make([]*shardWriter, len(shardDirs)),
		written: make([]atomic.Int64, len(shardDirs)),
	}
	for i, dir := range shardDirs {
		var err error
		if sb.data[i], err = newShardWriter(filepath.Join(dir, "data"), direct); err != nil {
			sb.close()
			return nil, err
		}
	}
	return sb, nil
}

func (sb *shardBigFiles) n() uint64 { return uint64(len(sb.data)) }

func (sb *shardBigFiles) choose() int {
	// Prefer the disk with the least queued bytes
	best, bestQ := 0, int64(math.MaxInt64)
	anyQueued := false
	for i := range sb.data {
		q := sb.data[i].queued.Load()
		if q > 0 {
			anyQueued = true
		}
		if q < bestQ {
			best, bestQ = i, q
		}
	}
	if anyQueued {
		return best
	}
	// No queued bytes so spread evenly by cumulative bytes instead.
	best, bestW := 0, int64(math.MaxInt64)
	for i := range sb.data {
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
	base, err = sb.data[shard].append(blob)
	if err != nil {
		return 0, 0, err
	}
	return uint64(shard), base, nil
}

func (sb *shardBigFiles) close() error {
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
