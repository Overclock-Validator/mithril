package history

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

// Signature index: every folded transaction's first signature, by its first
// 8 bytes, with the transaction's slot and position in its block. A lookup
// returns every (slot, position) whose prefix matches. Different signatures
// can share a prefix, so callers confirm each candidate against the full
// signature in the block before answering.
//
// Each fold writes one sorted file beside its chunk; when a segment closes,
// its chunks' files are merged into one, so a lookup reads one small block
// per closed segment and one per chunk of the open segment.
//
// File layout (big endian):
//
//	header  "MHSIGIX1", from u64, to u64, n u64
//	entries n x (prefix u64, slot-from u32, position u32), sorted
//	trailer per block of sigBlockEntries entries: first prefix u64, crc32 u32
//	crc32 of header and trailer
//
// The header and trailer are checked when the file is opened, each block's
// crc when it is read.

const (
	sigMagic        = "MHSIGIX1"
	sigHeaderSize   = 32
	sigEntrySize    = 16
	sigBlockEntries = 512
	sigBlockSize    = sigEntrySize * sigBlockEntries
	sigTrailerEntry = 12
)

// SlotSignatures are one folded block's transactions, in block order, by
// SignaturePrefix of each transaction's first signature.
type SlotSignatures struct {
	Slot     uint64
	Prefixes []uint64
}

// SignaturePrefix is the index key of a signature: its first 8 bytes.
func SignaturePrefix(sig []byte) uint64 { return binary.BigEndian.Uint64(sig[:8]) }

// TxLocation is a transaction's block slot and its position among the
// block's transactions.
type TxLocation struct {
	Slot     uint64
	Position uint32
}

type sigEntry struct {
	prefix uint64
	slot   uint64
	pos    uint32
}

func (a sigEntry) less(b sigEntry) bool {
	if a.prefix != b.prefix {
		return a.prefix < b.prefix
	}
	if a.slot != b.slot {
		return a.slot < b.slot
	}
	return a.pos < b.pos
}

var errSigFile = errors.New("history: malformed signature index")

// A chunk's own file is sig-<from>-<to>.hsg; a merged one, sigs-<from>-<to>.hsg.
// A merged file is never rewritten in place: readers may open it by name.
func sigChunkFileName(from, to uint64) string  { return fmt.Sprintf("sig-%020d-%020d.hsg", from, to) }
func sigMergedFileName(from, to uint64) string { return fmt.Sprintf("sigs-%020d-%020d.hsg", from, to) }

// sigFile is an opened signature index: its range, size and block trailer.
type sigFile struct {
	path     string
	from, to uint64
	n        uint64
	fences   []uint64 // first prefix of each block
	crcs     []uint32
}

func sigBlocks(n uint64) uint64 { return (n + sigBlockEntries - 1) / sigBlockEntries }

func openSigFile(path string) (*sigFile, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	st, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	var hdr [sigHeaderSize]byte
	if _, err := fh.ReadAt(hdr[:], 0); err != nil || string(hdr[:8]) != sigMagic {
		return nil, fmt.Errorf("%w: %s header", errSigFile, path)
	}
	f := &sigFile{path: path, from: binary.BigEndian.Uint64(hdr[8:]), to: binary.BigEndian.Uint64(hdr[16:]), n: binary.BigEndian.Uint64(hdr[24:])}
	if f.from > f.to || f.to-f.from > 1<<32-1 || f.n > uint64(st.Size())/sigEntrySize {
		return nil, fmt.Errorf("%w: %s header", errSigFile, path)
	}
	nb := sigBlocks(f.n)
	if uint64(st.Size()) != sigHeaderSize+sigEntrySize*f.n+sigTrailerEntry*nb+4 {
		return nil, fmt.Errorf("%w: %s has the wrong size", errSigFile, path)
	}
	tr := make([]byte, sigTrailerEntry*nb+4)
	if _, err := fh.ReadAt(tr, int64(sigHeaderSize+sigEntrySize*f.n)); err != nil {
		return nil, err
	}
	c := crc32.Update(crc32.ChecksumIEEE(hdr[:]), crc32.IEEETable, tr[:len(tr)-4])
	if c != binary.BigEndian.Uint32(tr[len(tr)-4:]) {
		return nil, fmt.Errorf("%w: %s checksum", errSigFile, path)
	}
	f.fences = make([]uint64, nb)
	f.crcs = make([]uint32, nb)
	for i := range f.fences {
		f.fences[i] = binary.BigEndian.Uint64(tr[sigTrailerEntry*i:])
		f.crcs[i] = binary.BigEndian.Uint32(tr[sigTrailerEntry*i+8:])
		if i > 0 && f.fences[i] < f.fences[i-1] {
			return nil, fmt.Errorf("%w: %s blocks out of order", errSigFile, path)
		}
	}
	return f, nil
}

// readBlock reads and checks block b into buf, returning its entries' bytes.
func (f *sigFile) readBlock(fh *os.File, b uint64, buf []byte) ([]byte, error) {
	cnt := min(uint64(sigBlockEntries), f.n-b*sigBlockEntries)
	buf = buf[:sigEntrySize*cnt]
	if _, err := fh.ReadAt(buf, int64(sigHeaderSize+b*sigBlockSize)); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(buf) != f.crcs[b] {
		return nil, fmt.Errorf("%w: %s block %d checksum", errSigFile, f.path, b)
	}
	return buf, nil
}

func (f *sigFile) decode(e []byte) (sigEntry, error) {
	off := uint64(binary.BigEndian.Uint32(e[8:]))
	if off > f.to-f.from {
		return sigEntry{}, fmt.Errorf("%w: %s slot outside its range", errSigFile, f.path)
	}
	return sigEntry{prefix: binary.BigEndian.Uint64(e), slot: f.from + off, pos: binary.BigEndian.Uint32(e[12:])}, nil
}

// lookup appends every entry with the prefix, in slot order.
func (f *sigFile) lookup(prefix uint64, out []TxLocation) ([]TxLocation, error) {
	if f.n == 0 {
		return out, nil
	}
	// Entries with the prefix can begin in the block before the first block
	// whose first prefix is at least it.
	i := uint64(sort.Search(len(f.fences), func(i int) bool { return f.fences[i] >= prefix }))
	if i > 0 {
		i--
	}
	fh, err := os.Open(f.path)
	if err != nil {
		return out, err
	}
	defer fh.Close()
	buf := make([]byte, sigBlockSize)
	for b := i; b < uint64(len(f.fences)); b++ {
		if b > i && f.fences[b] > prefix {
			break
		}
		blk, err := f.readBlock(fh, b, buf)
		if err != nil {
			return out, err
		}
		for k := 0; k < len(blk); k += sigEntrySize {
			p := binary.BigEndian.Uint64(blk[k:])
			if p < prefix {
				continue
			}
			if p > prefix {
				return out, nil
			}
			e, err := f.decode(blk[k:])
			if err != nil {
				return out, err
			}
			out = append(out, TxLocation{Slot: e.slot, Position: e.pos})
		}
	}
	return out, nil
}

// sigIterBlocks is how many blocks an iterator reads at once.
const sigIterBlocks = 32

// sigIter reads a file's entries in order, checking every block.
type sigIter struct {
	f    *sigFile
	fh   *os.File
	b    uint64 // next block to read
	blk  []byte // rest of the current block
	rest []byte // blocks read but not yet started
	buf  []byte
	cur  sigEntry
	err  error
}

func (f *sigFile) iter() (*sigIter, error) {
	fh, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}
	return &sigIter{f: f, fh: fh, buf: make([]byte, sigIterBlocks*sigBlockSize)}, nil
}

// fill reads the next blocks; the block boundaries come from n.
func (it *sigIter) fill() bool {
	nb := uint64(len(it.f.fences))
	if it.b >= nb {
		return false
	}
	first := it.b
	last := min(nb, first+sigIterBlocks)
	end := min(it.f.n, last*sigBlockEntries)
	buf := it.buf[:sigEntrySize*(end-first*sigBlockEntries)]
	if _, err := it.fh.ReadAt(buf, int64(sigHeaderSize+first*sigBlockSize)); err != nil {
		it.err = err
		return false
	}
	it.rest, it.b = buf, last
	return true
}

func (it *sigIter) next() bool {
	if it.err != nil {
		return false
	}
	if len(it.blk) == 0 {
		if len(it.rest) == 0 && !it.fill() {
			return false
		}
		b := it.b - uint64((len(it.rest)+sigBlockSize-1)/sigBlockSize)
		n := min(len(it.rest), sigBlockSize)
		it.blk, it.rest = it.rest[:n], it.rest[n:]
		if crc32.ChecksumIEEE(it.blk) != it.f.crcs[b] {
			it.err = fmt.Errorf("%w: %s block %d checksum", errSigFile, it.f.path, b)
			return false
		}
	}
	it.cur, it.err = it.f.decode(it.blk)
	it.blk = it.blk[sigEntrySize:]
	return it.err == nil
}

func (it *sigIter) close() { it.fh.Close() }

// sigWriter streams sorted entries into a new file, replacing path when done.
type sigWriter struct {
	path     string
	tmp      *os.File
	w        *bufio.Writer
	from, to uint64
	n        uint64
	blk      []byte
	trailer  []byte
	last     sigEntry
}

func newSigWriter(path string, from, to uint64) (*sigWriter, error) {
	if from > to || to-from > 1<<32-1 {
		return nil, fmt.Errorf("history: signature index range %d..%d", from, to)
	}
	tmp, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w := &sigWriter{path: path, tmp: tmp, w: bufio.NewWriterSize(tmp, 1<<20), from: from, to: to, blk: make([]byte, 0, sigBlockSize)}
	if _, err := w.w.Write(make([]byte, sigHeaderSize)); err != nil {
		w.abort()
		return nil, err
	}
	return w, nil
}

func (w *sigWriter) add(e sigEntry) error {
	if e.slot < w.from || e.slot > w.to {
		return fmt.Errorf("history: signature at slot %d outside %d..%d", e.slot, w.from, w.to)
	}
	if w.n > 0 && !w.last.less(e) {
		return fmt.Errorf("history: signature index entries out of order at slot %d", e.slot)
	}
	w.last = e
	w.blk = binary.BigEndian.AppendUint64(w.blk, e.prefix)
	w.blk = binary.BigEndian.AppendUint32(w.blk, uint32(e.slot-w.from))
	w.blk = binary.BigEndian.AppendUint32(w.blk, e.pos)
	w.n++
	if len(w.blk) == sigBlockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *sigWriter) flushBlock() error {
	if len(w.blk) == 0 {
		return nil
	}
	w.trailer = append(w.trailer, w.blk[:8]...)
	w.trailer = binary.BigEndian.AppendUint32(w.trailer, crc32.ChecksumIEEE(w.blk))
	_, err := w.w.Write(w.blk)
	w.blk = w.blk[:0]
	return err
}

func (w *sigWriter) finish() (*sigFile, error) {
	hdr := make([]byte, 0, sigHeaderSize)
	hdr = append(hdr, sigMagic...)
	hdr = binary.BigEndian.AppendUint64(hdr, w.from)
	hdr = binary.BigEndian.AppendUint64(hdr, w.to)
	hdr = binary.BigEndian.AppendUint64(hdr, w.n)
	err := w.flushBlock()
	if err == nil {
		c := crc32.Update(crc32.ChecksumIEEE(hdr), crc32.IEEETable, w.trailer)
		_, err = w.w.Write(binary.BigEndian.AppendUint32(w.trailer, c))
	}
	if err == nil {
		err = w.w.Flush()
	}
	if err == nil {
		_, err = w.tmp.WriteAt(hdr, 0)
	}
	if err == nil {
		err = w.tmp.Sync()
	}
	if cerr := w.tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(w.tmp.Name(), w.path)
	}
	if err == nil {
		err = syncDir(filepath.Dir(w.path))
	}
	if err != nil {
		_ = os.Remove(w.tmp.Name())
		return nil, err
	}
	return openSigFile(w.path)
}

func (w *sigWriter) abort() {
	w.tmp.Close()
	_ = os.Remove(w.tmp.Name())
}

// writeChunkSigs writes one fold's signature file.
func writeChunkSigs(path string, from, to uint64, slots []SlotSignatures) (*sigFile, error) {
	n := 0
	for i, s := range slots {
		if s.Slot < from || s.Slot > to || (i > 0 && s.Slot <= slots[i-1].Slot) {
			return nil, fmt.Errorf("history: signatures for slot %d out of order in %d..%d", s.Slot, from, to)
		}
		if uint64(len(s.Prefixes)) > 1<<32-1 {
			return nil, fmt.Errorf("history: too many transactions in slot %d", s.Slot)
		}
		n += len(s.Prefixes)
	}
	entries := make([]sigEntry, 0, n)
	for _, s := range slots {
		for pos, p := range s.Prefixes {
			entries = append(entries, sigEntry{prefix: p, slot: s.Slot, pos: uint32(pos)})
		}
	}
	entries = radixSortSigs(entries)
	w, err := newSigWriter(path, from, to)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if err := w.add(e); err != nil {
			w.abort()
			return nil, err
		}
	}
	return w.finish()
}

// sigHeap is a min-heap of iterators by their current entry.
type sigHeap []*sigIter

func (h sigHeap) down(i int) {
	for {
		l := 2*i + 1
		if l >= len(h) {
			return
		}
		m := l
		if r := l + 1; r < len(h) && h[r].cur.less(h[l].cur) {
			m = r
		}
		if !h[m].cur.less(h[i].cur) {
			return
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
}

// mergeSigFiles writes the entries of files within from..to into one file
// covering that range. The output may replace one of the inputs.
func mergeSigFiles(path string, from, to uint64, files []*sigFile) (*sigFile, error) {
	w, err := newSigWriter(path, from, to)
	if err != nil {
		return nil, err
	}
	var h sigHeap
	var iters []*sigIter
	defer func() {
		for _, it := range iters {
			it.close()
		}
	}()
	fail := func(err error) (*sigFile, error) {
		w.abort()
		return nil, err
	}
	for _, f := range files {
		it, err := f.iter()
		if err != nil {
			return fail(err)
		}
		iters = append(iters, it)
		if it.next() {
			h = append(h, it)
		} else if it.err != nil {
			return fail(it.err)
		}
	}
	for i := len(h)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
	for len(h) > 0 {
		it := h[0]
		if it.cur.slot >= from && it.cur.slot <= to {
			if err := w.add(it.cur); err != nil {
				return fail(err)
			}
		}
		if !it.next() {
			if it.err != nil {
				return fail(it.err)
			}
			h[0] = h[len(h)-1]
			h = h[:len(h)-1]
		}
		h.down(0)
	}
	return w.finish()
}

// loadSigs opens the segment's signature files after its chunks are known.
// A file that is damaged, does not match the chunks, or is covered by a
// merged file is removed; its chunks are then not indexed.
func (seg *segment) loadSigs(names []string) {
	byRange := make(map[[2]uint64]*chunkMeta, len(seg.chunks))
	ends := make(map[uint64]bool, len(seg.chunks))
	for _, c := range seg.chunks {
		byRange[[2]uint64{c.from, c.to}] = c
		ends[c.to] = true
	}
	remove := func(name string) { _ = os.Remove(filepath.Join(seg.dir, name)) }
	// The merged file reaching furthest wins; it must start the segment and
	// end on a chunk boundary.
	var merged *sigFile
	var chunkNames []string
	for _, name := range names {
		from, to, ok := parseRange(name, "sigs-", ".hsg")
		if !ok {
			chunkNames = append(chunkNames, name)
			continue
		}
		if from != seg.from() || !ends[to] || (merged != nil && merged.to >= to) {
			remove(name)
			continue
		}
		f, err := openSigFile(filepath.Join(seg.dir, name))
		if err != nil || f.from != from || f.to != to {
			remove(name)
			continue
		}
		if merged != nil {
			_ = os.Remove(merged.path)
		}
		merged = f
	}
	if merged != nil {
		seg.sigFiles = append(seg.sigFiles, merged)
		for _, c := range seg.chunks {
			if c.to <= merged.to {
				c.sigs = true
			}
		}
	}
	for _, name := range chunkNames {
		from, to, ok := parseRange(name, "sig-", ".hsg")
		c := byRange[[2]uint64{from, to}]
		if !ok || c == nil || c.sigs {
			remove(name)
			continue
		}
		f, err := openSigFile(filepath.Join(seg.dir, name))
		if err != nil || f.from != from || f.to != to {
			remove(name)
			continue
		}
		c.sigs = true
		seg.sigFiles = append(seg.sigFiles, f)
	}
	sort.Slice(seg.sigFiles, func(i, j int) bool { return seg.sigFiles[i].from < seg.sigFiles[j].from })
}

// mergeSigs writes one signature file for the whole segment when every chunk
// is indexed and more than one file holds them; otherwise it returns nil.
func (seg *segment) mergeSigs() (*sigFile, error) {
	if len(seg.sigFiles) < 2 {
		return nil, nil
	}
	for _, c := range seg.chunks {
		if !c.sigs {
			return nil, nil
		}
	}
	return mergeSigFiles(filepath.Join(seg.dir, sigMergedFileName(seg.from(), seg.to())), seg.from(), seg.to(), seg.sigFiles)
}

// useMergedSigs replaces the segment's signature files with m (writer, with
// the store locked).
func (seg *segment) useMergedSigs(m *sigFile) {
	old := seg.sigFiles
	seg.sigFiles = []*sigFile{m}
	for _, f := range old {
		if f.path != m.path {
			_ = os.Remove(f.path)
		}
	}
}

// truncateSigs keeps the signature index of the first keep chunks only
// (writer, with the store locked). A merged file reaching past them is
// rewritten under a new name first, so a crash part way leaves either file.
func (seg *segment) truncateSigs(keep int) error {
	var cut uint64
	if keep > 0 {
		cut = seg.chunks[keep-1].to
	}
	var kept []*sigFile
	for _, f := range seg.sigFiles {
		switch {
		case keep > 0 && f.to <= cut:
			kept = append(kept, f)
		case keep > 0 && f.from <= cut:
			nf, err := mergeSigFiles(filepath.Join(seg.dir, sigMergedFileName(f.from, cut)), f.from, cut, []*sigFile{f})
			if err != nil {
				return err
			}
			kept = append(kept, nf)
			if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
				return err
			}
		default:
			if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	seg.sigFiles = kept
	return nil
}

// signatureCoverage returns the first slot from which every recorded chunk
// through the newest is in the signature index (s.mu held).
func (s *Store) signatureCoverage() (from, through uint64, ok bool) {
	for i := len(s.segments) - 1; i >= 0; i-- {
		seg := s.segments[i]
		for j := len(seg.chunks) - 1; j >= 0; j-- {
			c := seg.chunks[j]
			if !c.sigs {
				return from, through, ok
			}
			if !ok {
				through, ok = c.to, true
			}
			from = c.from
		}
	}
	return from, through, ok
}

// TransactionsBySignature returns every indexed transaction whose first
// signature begins with the same 8 bytes as sig, oldest first. Each is a
// candidate only: the caller must compare the full signature in the block.
//
// from and through are the slots the index covers without a gap: a
// signature with no candidate there was not the first signature of any
// folded transaction from slot from through slot through. ok is false when
// the newest recorded chunk is not indexed (no such statement can be made);
// candidates from older indexed chunks are still returned.
func (s *Store) TransactionsBySignature(sig []byte) (cands []TxLocation, from, through uint64, ok bool, err error) {
	if len(sig) < 8 {
		return nil, 0, 0, false, fmt.Errorf("history: signature of %d bytes", len(sig))
	}
	prefix := SignaturePrefix(sig)
	s.mu.RLock()
	defer s.mu.RUnlock()
	from, through, ok = s.signatureCoverage()
	for _, seg := range s.segments {
		for _, f := range seg.sigFiles {
			if cands, err = f.lookup(prefix, cands); err != nil {
				return nil, from, through, ok, err
			}
		}
	}
	return cands, from, through, ok, nil
}

// radixSortSigs sorts entries by prefix, keeping the order of entries with
// the same prefix (so entries built in slot and position order end up fully
// sorted). It returns the sorted slice, which may be a new one.
func radixSortSigs(a []sigEntry) []sigEntry {
	if len(a) < 256 {
		sort.SliceStable(a, func(i, j int) bool { return a[i].prefix < a[j].prefix })
		return a
	}
	const bits = 16
	tmp := make([]sigEntry, len(a))
	var count [1 << bits]int
	for shift := uint(0); shift < 64; shift += bits {
		clear(count[:])
		for i := range a {
			count[(a[i].prefix>>shift)&(1<<bits-1)]++
		}
		sum := 0
		for d := range count {
			c := count[d]
			count[d] = sum
			sum += c
		}
		for i := range a {
			d := (a[i].prefix >> shift) & (1<<bits - 1)
			tmp[count[d]] = a[i]
			count[d]++
		}
		a, tmp = tmp, a
	}
	return a
}
