package history

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// A chunk file holds one fold's history, records sorted by account key and
// compressed in blocks:
//
//	magic "MHCHUNK1"
//	blocks: one zstd frame each, records in key order
//	index: per block {first key [32], offset u64, compressed len u32, raw len u32}
//	footer: index offset u64, block count u32, index crc32 u32,
//	        from slot u64, to slot u64, magic "MHCHUNK1"
//
// A record is: key [32], flags byte (bit 0: a full copy follows; bit 1: the
// account did not exist before the chunk), the full copy (AppendWhole), then a uvarint count of differences, each a uvarint slot
// offset from the chunk's first slot, a uvarint length and the XOR bytes.
const (
	chunkMagic     = "MHCHUNK1"
	chunkFooterLen = 8 + 4 + 4 + 8 + 8 + 8
	indexEntryLen  = 32 + 8 + 4 + 4
	recordHasFull  = 1
	recordAbsent   = 2
)

// Diff is one slot's change to an account.
type Diff struct {
	Slot  uint64
	Bytes []byte
}

// Record is one account's history within a chunk.
type Record struct {
	Key  [32]byte
	Full *Version // version before the chunk's first change; nil if not in this chunk
	// AbsentBefore: the account did not exist before the chunk's first
	// change, so a read needs no older chunk. Chunks written before the flag
	// existed never set it.
	AbsentBefore bool
	Diffs        []Diff // ascending by slot
}

// targetBlockSize is the raw size at which a chunk block is closed; a read
// decompresses one block. On a mainnet corpus 16 KiB blocks made
// whole-block queries' account reads about twice as fast as 64 KiB ones,
// for 4% more disk and 48 bytes of index in memory per block. A variable
// so tests can compare sizes.
var targetBlockSize = 16 << 10

var (
	encOnce sync.Once
	enc     *zstd.Encoder
	dec     *zstd.Decoder
)

func codecs() (*zstd.Encoder, *zstd.Decoder) {
	encOnce.Do(func() {
		enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		dec, _ = zstd.NewReader(nil)
	})
	return enc, dec
}

func appendRecord(dst []byte, r *Record, from uint64) []byte {
	dst = append(dst, r.Key[:]...)
	switch {
	case r.Full != nil:
		dst = append(dst, recordHasFull)
		dst = AppendWhole(dst, r.Full)
	case r.AbsentBefore:
		dst = append(dst, recordAbsent)
	default:
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, uint64(len(r.Diffs)))
	for _, d := range r.Diffs {
		dst = binary.AppendUvarint(dst, d.Slot-from)
		dst = binary.AppendUvarint(dst, uint64(len(d.Bytes)))
		dst = append(dst, d.Bytes...)
	}
	return dst
}

func readRecord(b []byte, from uint64) (*Record, []byte, error) {
	if len(b) < 33 {
		return nil, nil, errChunk
	}
	r := &Record{}
	copy(r.Key[:], b[:32])
	flags := b[32]
	b = b[33:]
	r.AbsentBefore = flags&recordAbsent != 0
	if flags&recordHasFull != 0 {
		v, rest, err := ReadWhole(b)
		if err != nil {
			return nil, nil, err
		}
		r.Full, b = v, rest
	}
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, nil, errChunk
	}
	b = b[k:]
	if n > uint64(len(b))/2 { // each difference takes at least two bytes
		return nil, nil, errChunk
	}
	r.Diffs = make([]Diff, 0, n)
	for i := uint64(0); i < n; i++ {
		off, k1 := binary.Uvarint(b)
		if k1 <= 0 {
			return nil, nil, errChunk
		}
		b = b[k1:]
		l, k2 := binary.Uvarint(b)
		if k2 <= 0 || l > uint64(len(b)-k2) {
			return nil, nil, errChunk
		}
		b = b[k2:]
		r.Diffs = append(r.Diffs, Diff{Slot: from + off, Bytes: b[:l:l]})
		b = b[l:]
	}
	return r, b, nil
}

// skipRecord returns what follows the record at the start of b, checking its
// lengths as readRecord does but allocating nothing.
func skipRecord(b []byte) ([]byte, error) {
	if len(b) < 33 {
		return nil, errChunk
	}
	flags := b[32]
	b = b[33:]
	if flags&recordHasFull != 0 {
		if len(b) < 49 {
			return nil, errChunk
		}
		n, k := binary.Uvarint(b[49:])
		if k <= 0 || n > MaxDataLen || uint64(len(b)-49-k) < n {
			return nil, errChunk
		}
		b = b[49+k+int(n):]
	}
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, errChunk
	}
	b = b[k:]
	if n > uint64(len(b))/2 {
		return nil, errChunk
	}
	for i := uint64(0); i < n; i++ {
		_, k1 := binary.Uvarint(b)
		if k1 <= 0 {
			return nil, errChunk
		}
		b = b[k1:]
		l, k2 := binary.Uvarint(b)
		if k2 <= 0 || l > uint64(len(b)-k2) {
			return nil, errChunk
		}
		b = b[k2+int(l):]
	}
	return b, nil
}

var errChunk = errors.New("history: malformed chunk")

// writeChunk writes records (sorted by key) as a chunk file covering slots
// [from, to], through a temporary file renamed into place after fsync, and
// returns a reader for it built from what was written.
func writeChunk(path string, from, to uint64, records []*Record) (*chunkReader, error) {
	e, _ := codecs()
	rawTotal := 0
	for _, r := range records {
		rawTotal += 48
		if r.Full != nil {
			rawTotal += 64 + len(r.Full.Data)
		}
		for _, d := range r.Diffs {
			rawTotal += 8 + len(d.Bytes)
		}
	}
	out := make([]byte, 0, len(chunkMagic)+rawTotal/3+chunkFooterLen+4096)
	out = append(out, chunkMagic...)
	c := &chunkReader{path: path, from: from, to: to}
	var index []byte
	raw := make([]byte, 0, 2*targetBlockSize)
	var first [32]byte
	flush := func() {
		if len(raw) == 0 {
			return
		}
		offset := len(out)
		out = e.EncodeAll(raw, out)
		b := blockRef{first: first, offset: uint64(offset), clen: uint32(len(out) - offset), rlen: uint32(len(raw))}
		c.blocks = append(c.blocks, b)
		index = append(index, first[:]...)
		index = binary.LittleEndian.AppendUint64(index, b.offset)
		index = binary.LittleEndian.AppendUint32(index, b.clen)
		index = binary.LittleEndian.AppendUint32(index, b.rlen)
		raw = raw[:0]
	}
	for _, r := range records {
		if len(raw) == 0 {
			first = r.Key
		}
		raw = appendRecord(raw, r, from)
		if len(raw) >= targetBlockSize {
			flush()
		}
	}
	flush()
	indexOffset := uint64(len(out))
	out = append(out, index...)
	out = binary.LittleEndian.AppendUint64(out, indexOffset)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(c.blocks)))
	out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(index))
	out = binary.LittleEndian.AppendUint64(out, from)
	out = binary.LittleEndian.AppendUint64(out, to)
	out = append(out, chunkMagic...)
	if err := writeFileSync(path, out); err != nil {
		return nil, err
	}
	return c, nil
}

// syncDir makes a rename or create in dir durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// writeFileSync writes data through a temporary file, fsyncs it, renames it
// into place and fsyncs the directory.
func writeFileSync(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

type blockRef struct {
	first      [32]byte
	offset     uint64
	clen, rlen uint32
}

// chunkReader reads one chunk file. Its index is kept in memory; blocks are
// read and decompressed on demand.
type chunkReader struct {
	path     string
	from, to uint64
	blocks   []blockRef
}

func openChunk(path string) (*chunkReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size < int64(len(chunkMagic)+chunkFooterLen) {
		return nil, fmt.Errorf("%w: %s too short", errChunk, path)
	}
	foot := make([]byte, chunkFooterLen)
	if _, err := f.ReadAt(foot, size-chunkFooterLen); err != nil {
		return nil, err
	}
	head := make([]byte, len(chunkMagic))
	if _, err := f.ReadAt(head, 0); err != nil {
		return nil, err
	}
	if string(foot[32:]) != chunkMagic || string(head) != chunkMagic {
		return nil, fmt.Errorf("%w: %s bad magic", errChunk, path)
	}
	indexOffset := binary.LittleEndian.Uint64(foot)
	n := uint64(binary.LittleEndian.Uint32(foot[8:]))
	sum := binary.LittleEndian.Uint32(foot[12:])
	c := &chunkReader{path: path, from: binary.LittleEndian.Uint64(foot[16:]), to: binary.LittleEndian.Uint64(foot[24:])}
	body := uint64(size - chunkFooterLen)
	if indexOffset < uint64(len(chunkMagic)) || indexOffset > body || n != (body-indexOffset)/indexEntryLen || (body-indexOffset)%indexEntryLen != 0 {
		return nil, fmt.Errorf("%w: %s index bounds", errChunk, path)
	}
	index := make([]byte, n*indexEntryLen)
	if _, err := f.ReadAt(index, int64(indexOffset)); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(index) != sum {
		return nil, fmt.Errorf("%w: %s index checksum", errChunk, path)
	}
	c.blocks = make([]blockRef, n)
	for i := range c.blocks {
		e := index[i*indexEntryLen:]
		copy(c.blocks[i].first[:], e[:32])
		c.blocks[i].offset = binary.LittleEndian.Uint64(e[32:])
		c.blocks[i].clen = binary.LittleEndian.Uint32(e[40:])
		c.blocks[i].rlen = binary.LittleEndian.Uint32(e[44:])
		if b := c.blocks[i]; b.offset < uint64(len(chunkMagic)) || b.offset+uint64(b.clen) > indexOffset {
			return nil, fmt.Errorf("%w: %s block %d bounds", errChunk, path, i)
		}
	}
	return c, nil
}

func (c *chunkReader) close() error { return nil }

// block reads and decompresses block i. The file is opened per read so a
// store with thousands of chunks holds no descriptors.
func (c *chunkReader) block(i int) ([]byte, error) {
	b := c.blocks[i]
	f, err := os.Open(c.path)
	if err != nil {
		return nil, err
	}
	z := make([]byte, b.clen)
	_, err = f.ReadAt(z, int64(b.offset))
	f.Close()
	if err != nil {
		return nil, err
	}
	_, d := codecs()
	raw, err := d.DecodeAll(z, make([]byte, 0, min(b.rlen, 16<<20)))
	if err != nil {
		return nil, fmt.Errorf("history: %s block %d: %w", c.path, i, err)
	}
	return raw, nil
}

// blockCache keeps recently decompressed blocks for every reader of a store,
// so the reads of one query, and of queries near each other, decompress a
// block once. Entries are keyed by reader: a chunk recorded again after a
// truncation has a new reader and never sees the old one's blocks. Cached
// blocks are shared and must not be modified.
type blockCache struct {
	mu           sync.Mutex
	limit        int64
	size         int64
	m            map[blockKey]*list.Element
	lru          list.List // of *cachedBlock, newest first
	hits, misses uint64
}

type blockKey struct {
	c     *chunkReader
	block int
}

type cachedBlock struct {
	key blockKey
	raw []byte
}

func newBlockCache(limit int64) *blockCache {
	if limit <= 0 {
		return nil
	}
	return &blockCache{limit: limit, m: make(map[blockKey]*list.Element)}
}

func (bc *blockCache) get(k blockKey) []byte {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if e, ok := bc.m[k]; ok {
		bc.lru.MoveToFront(e)
		bc.hits++
		return e.Value.(*cachedBlock).raw
	}
	bc.misses++
	return nil
}

func (bc *blockCache) put(k blockKey, raw []byte) {
	n := int64(cap(raw))
	if n > bc.limit/4 {
		return
	}
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if _, ok := bc.m[k]; ok {
		return // another reader decompressed it at the same time
	}
	bc.m[k] = bc.lru.PushFront(&cachedBlock{key: k, raw: raw})
	bc.size += n
	for bc.size > bc.limit {
		e := bc.lru.Back()
		cb := e.Value.(*cachedBlock)
		bc.lru.Remove(e)
		delete(bc.m, cb.key)
		bc.size -= int64(cap(cb.raw))
	}
}

// lookup returns the record for key, or nil if the chunk does not hold it.
func (c *chunkReader) lookup(key [32]byte) (*Record, error) {
	return c.lookupCached(key, nil)
}

func (c *chunkReader) lookupCached(key [32]byte, cache *blockCache) (*Record, error) {
	i := sort.Search(len(c.blocks), func(i int) bool { return bytes.Compare(c.blocks[i].first[:], key[:]) > 0 }) - 1
	if i < 0 {
		return nil, nil
	}
	var raw []byte
	k := blockKey{c, i}
	if cache != nil {
		raw = cache.get(k)
	}
	if raw == nil {
		var err error
		if raw, err = c.block(i); err != nil {
			return nil, err
		}
		if cache != nil {
			cache.put(k, raw)
		}
	}
	// Records before the key are skipped without decoding them.
	for len(raw) > 0 {
		if len(raw) < 33 {
			return nil, fmt.Errorf("history: %s: %w", c.path, errChunk)
		}
		switch bytes.Compare(raw[:32], key[:]) {
		case 0:
			r, _, err := readRecord(raw, c.from)
			if err != nil {
				return nil, fmt.Errorf("history: %s: %w", c.path, err)
			}
			return r, nil
		case 1:
			return nil, nil
		}
		rest, err := skipRecord(raw)
		if err != nil {
			return nil, fmt.Errorf("history: %s: %w", c.path, err)
		}
		raw = rest
	}
	return nil, nil
}

// records calls fn with every record in the chunk, in key order. Used to
// rebuild the open segment's tables after a restart or truncation.
func (c *chunkReader) records(fn func(r *Record)) error {
	for i := range c.blocks {
		raw, err := c.block(i)
		if err != nil {
			return err
		}
		for len(raw) > 0 {
			r, rest, err := readRecord(raw, c.from)
			if err != nil {
				return err
			}
			fn(r)
			raw = rest
		}
	}
	return nil
}
