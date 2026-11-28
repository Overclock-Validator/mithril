package history

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Bank context per recorded slot, so a past block can be replayed from history
// without first rebuilding a bank by replay. Account state (sysvars included)
// comes from AccountAt; these records hold the bank state that is not an
// account. Records exist only for folded blocks, so a block that lost a fork
// never supplies one; callers still check Blockhash against the block they
// replay.

// SlotBank is one folded block's bank context.
type SlotBank struct {
	Slot                     uint64
	Parent                   uint64 // the previous folded block's slot (0 when unknown)
	BlockHeight              uint64
	Bankhash                 [32]byte
	Blockhash                [32]byte
	LamportsPerSignature     uint64
	PrevLamportsPerSignature uint64
	NumSignatures            uint64
}

// BlockhashFee is one entry of the recent blockhash queue.
type BlockhashFee struct {
	Blockhash            [32]byte
	LamportsPerSignature uint64
}

// ChunkBank is the heavier context kept once per chunk, as of its last block:
// what a whole-block replay from the chunk boundary needs.
type ChunkBank struct {
	Slot              uint64
	AccountsLtHash    []byte
	RecentBlockhashes []BlockhashFee
	EvictedBlockhash  [32]byte
}

// BaseBank is the bank as of the block before a fold. It is kept with the
// first chunk of each segment, so the blocks at the start of history, whose
// parent no chunk covers, can be replayed like any other: BankAt and
// ChunkBankAtOrBefore return it, and accounts are read as of its slot.
type BaseBank struct {
	Slot  SlotBank  // Parent is 0
	Chunk ChunkBank // Chunk.Slot == Slot.Slot
}

// BankData is the bank context of one fold.
type BankData struct {
	Slots []SlotBank // ascending
	Top   *ChunkBank
	// Base is the bank before the fold's first block; the store keeps it only
	// when the fold starts a segment.
	Base *BaseBank
	// Sidecars are other per-fold records kept with the chunk, by kind (lower
	// case letters and digits), such as replay hints. Each is stored in its
	// own file, removed with the chunk, and read back with SidecarAt.
	Sidecars map[string][]byte
	// Signatures index the fold's transactions by signature, one entry per
	// folded block in slot order; nil when not recorded for this fold.
	Signatures []SlotSignatures
}

// ErrNoSidecar: no record of that kind for the slot's chunk.
var ErrNoSidecar = errors.New("history: no sidecar record for the slot")

const sidecarMagic = "MHSIDE01"

func sidecarFileName(kind string, from, to uint64) string {
	return fmt.Sprintf("%s-%020d-%020d.hsc", kind, from, to)
}

func validSidecarKind(kind string) bool {
	if kind == "" || len(kind) > 32 {
		return false
	}
	for _, c := range kind {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func encodeSidecar(data []byte) []byte {
	out := make([]byte, 0, len(sidecarMagic)+len(data)+4)
	out = append(out, sidecarMagic...)
	out = append(out, data...)
	return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
}

func decodeSidecar(b []byte) ([]byte, error) {
	if len(b) < len(sidecarMagic)+4 || string(b[:len(sidecarMagic)]) != sidecarMagic ||
		crc32.ChecksumIEEE(b[:len(b)-4]) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, errors.New("history: malformed sidecar file")
	}
	return b[len(sidecarMagic) : len(b)-4], nil
}

// parseSidecarName returns the chunk range of a sidecar file name.
func parseSidecarName(name string) (from, to uint64, ok bool) {
	if !strings.HasSuffix(name, ".hsc") {
		return 0, 0, false
	}
	base := strings.TrimSuffix(name, ".hsc")
	i := strings.LastIndexByte(base, '-')
	if i < 0 {
		return 0, 0, false
	}
	j := strings.LastIndexByte(base[:i], '-')
	if j <= 0 {
		return 0, 0, false
	}
	return parseRange(base[j+1:]+".x", "", ".x")
}

func (c *chunkMeta) sidecarPaths() []string {
	m, _ := filepath.Glob(filepath.Join(filepath.Dir(c.path), fmt.Sprintf("*-%020d-%020d.hsc", c.from, c.to)))
	return m
}

// SidecarAt returns the record of kind kept with the chunk covering slot,
// and that chunk's slot range.
func (s *Store) SidecarAt(slot uint64, kind string) (data []byte, from, to uint64, err error) {
	if !validSidecarKind(kind) {
		return nil, 0, 0, fmt.Errorf("history: invalid sidecar kind %q", kind)
	}
	s.mu.RLock()
	i, j := s.chunkFor(slot)
	var path string
	if i >= 0 {
		c := s.segments[i].chunks[j]
		from, to = c.from, c.to
		path = filepath.Join(filepath.Dir(c.path), sidecarFileName(kind, c.from, c.to))
	}
	s.mu.RUnlock()
	if path == "" {
		return nil, 0, 0, ErrNoSidecar
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, from, to, ErrNoSidecar
	}
	if err != nil {
		return nil, from, to, err
	}
	data, err = decodeSidecar(b)
	return data, from, to, err
}

// ErrNoBank: no bank record for that slot (skipped, not folded, or outside history).
var ErrNoBank = errors.New("history: no bank record for the slot")

const (
	bankMagic     = "MHBANK01"
	slotBankBytes = 6*8 + 64
)

func bankFileName(from, to uint64) string { return fmt.Sprintf("bank-%020d-%020d.hbk", from, to) }

func appendSlotBank(out []byte, r *SlotBank) []byte {
	out = binary.LittleEndian.AppendUint64(out, r.Slot)
	out = binary.LittleEndian.AppendUint64(out, r.Parent)
	out = binary.LittleEndian.AppendUint64(out, r.BlockHeight)
	out = binary.LittleEndian.AppendUint64(out, r.LamportsPerSignature)
	out = binary.LittleEndian.AppendUint64(out, r.PrevLamportsPerSignature)
	out = binary.LittleEndian.AppendUint64(out, r.NumSignatures)
	out = append(out, r.Bankhash[:]...)
	return append(out, r.Blockhash[:]...)
}

func appendChunkBank(out []byte, t *ChunkBank) []byte {
	out = binary.LittleEndian.AppendUint64(out, t.Slot)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(t.AccountsLtHash)))
	out = append(out, t.AccountsLtHash...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(t.RecentBlockhashes)))
	for _, e := range t.RecentBlockhashes {
		out = append(out, e.Blockhash[:]...)
		out = binary.LittleEndian.AppendUint64(out, e.LamportsPerSignature)
	}
	return append(out, t.EvictedBlockhash[:]...)
}

// encodeBank writes the slot records, then the chunk record and the base
// record, each behind a presence byte. Files written before base records
// existed end after the chunk record.
func encodeBank(b *BankData) []byte {
	out := []byte(bankMagic)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(b.Slots)))
	for i := range b.Slots {
		out = appendSlotBank(out, &b.Slots[i])
	}
	if t := b.Top; t != nil {
		out = append(out, 1)
		out = appendChunkBank(out, t)
	} else {
		out = append(out, 0)
	}
	if base := b.Base; base != nil {
		out = append(out, 1)
		out = appendSlotBank(out, &base.Slot)
		out = appendChunkBank(out, &base.Chunk)
	}
	return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
}

var errBank = errors.New("history: malformed bank file")

func decodeSlotBank(p []byte, r *SlotBank) {
	r.Slot = binary.LittleEndian.Uint64(p)
	r.Parent = binary.LittleEndian.Uint64(p[8:])
	r.BlockHeight = binary.LittleEndian.Uint64(p[16:])
	r.LamportsPerSignature = binary.LittleEndian.Uint64(p[24:])
	r.PrevLamportsPerSignature = binary.LittleEndian.Uint64(p[32:])
	r.NumSignatures = binary.LittleEndian.Uint64(p[40:])
	copy(r.Bankhash[:], p[48:80])
	copy(r.Blockhash[:], p[80:112])
}

// decodeChunkBank reads one chunk record and returns the bytes after it.
func decodeChunkBank(p []byte, t *ChunkBank) ([]byte, error) {
	if len(p) < 12 {
		return nil, errBank
	}
	t.Slot = binary.LittleEndian.Uint64(p)
	l := int(binary.LittleEndian.Uint32(p[8:]))
	p = p[12:]
	if len(p) < l+4 {
		return nil, errBank
	}
	t.AccountsLtHash = append([]byte(nil), p[:l]...)
	p = p[l:]
	m := int(binary.LittleEndian.Uint32(p))
	p = p[4:]
	if m < 0 || len(p) < m*40+32 {
		return nil, errBank
	}
	t.RecentBlockhashes = make([]BlockhashFee, m)
	for i := range t.RecentBlockhashes {
		copy(t.RecentBlockhashes[i].Blockhash[:], p[:32])
		t.RecentBlockhashes[i].LamportsPerSignature = binary.LittleEndian.Uint64(p[32:])
		p = p[40:]
	}
	copy(t.EvictedBlockhash[:], p[:32])
	return p[32:], nil
}

func decodeBank(b []byte) (*BankData, error) {
	if len(b) < len(bankMagic)+4+1+4 || string(b[:len(bankMagic)]) != bankMagic {
		return nil, errBank
	}
	body := b[:len(b)-4]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, fmt.Errorf("%w: checksum", errBank)
	}
	p := body[len(bankMagic):]
	n := int(binary.LittleEndian.Uint32(p))
	p = p[4:]
	if n < 0 || len(p) < n*slotBankBytes+1 {
		return nil, errBank
	}
	d := &BankData{Slots: make([]SlotBank, n)}
	for i := range d.Slots {
		decodeSlotBank(p, &d.Slots[i])
		p = p[slotBankBytes:]
	}
	hasTop := p[0]
	p = p[1:]
	var err error
	if hasTop == 1 {
		d.Top = &ChunkBank{}
		if p, err = decodeChunkBank(p, d.Top); err != nil {
			return nil, err
		}
	}
	if len(p) > 0 && p[0] == 1 {
		p = p[1:]
		if len(p) < slotBankBytes {
			return nil, errBank
		}
		d.Base = &BaseBank{}
		decodeSlotBank(p, &d.Base.Slot)
		if _, err = decodeChunkBank(p[slotBankBytes:], &d.Base.Chunk); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func readBankFile(path string) (*BankData, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeBank(b)
}

// bankFiles keeps the last few parsed bank files: a query walking parent
// links reads the same one or two files many times. Entries are dropped when
// history is truncated or reset, since a re-recorded range reuses its name.
type bankFiles struct {
	mu    sync.Mutex
	paths [4]string
	data  [4]*BankData
	next  int
}

func (c *bankFiles) get(path string) (*BankData, error) {
	c.mu.Lock()
	for i, p := range c.paths {
		if p == path {
			d := c.data[i]
			c.mu.Unlock()
			return d, nil
		}
	}
	c.mu.Unlock()
	d, err := readBankFile(path)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.paths[c.next], c.data[c.next] = path, d
	c.next = (c.next + 1) % len(c.paths)
	c.mu.Unlock()
	return d, nil
}

func (c *bankFiles) clear() {
	c.mu.Lock()
	c.paths, c.data = [4]string{}, [4]*BankData{}
	c.mu.Unlock()
}

func (c *chunkMeta) bankPath() string {
	return filepath.Join(filepath.Dir(c.path), bankFileName(c.from, c.to))
}

// chunkFor returns the chunk covering slot, or nil. Caller holds s.mu.
func (s *Store) chunkFor(slot uint64) (int, int) {
	for i := len(s.segments) - 1; i >= 0; i-- {
		seg := s.segments[i]
		if slot < seg.from() || slot > seg.to() {
			continue
		}
		j := sort.Search(len(seg.chunks), func(j int) bool { return seg.chunks[j].to >= slot })
		if j < len(seg.chunks) && seg.chunks[j].from <= slot {
			return i, j
		}
	}
	return -1, -1
}

// BankAt returns the bank context of the folded block at slot. The returned
// records are shared: callers must not modify them.
func (s *Store) BankAt(slot uint64) (SlotBank, error) {
	s.mu.RLock()
	i, j := s.chunkFor(slot)
	var path string
	if i >= 0 {
		path = s.segments[i].chunks[j].bankPath()
	} else if len(s.segments) > 0 {
		// The block before the oldest chunk: its segment's base record.
		if base := s.segments[0].base; base != nil && base.Slot == slot {
			r := *base
			s.mu.RUnlock()
			return r, nil
		}
	}
	s.mu.RUnlock()
	if path == "" {
		return SlotBank{}, ErrNoBank
	}
	d, err := s.banks.get(path)
	if errors.Is(err, os.ErrNotExist) {
		return SlotBank{}, ErrNoBank
	}
	if err != nil {
		return SlotBank{}, err
	}
	k := sort.Search(len(d.Slots), func(k int) bool { return d.Slots[k].Slot >= slot })
	if k == len(d.Slots) || d.Slots[k].Slot != slot {
		return SlotBank{}, ErrNoBank
	}
	return d.Slots[k], nil
}

// ChunkBankAtOrBefore returns the newest chunk record whose slot is at or
// before slot. The caller owns the returned copy.
func (s *Store) ChunkBankAtOrBefore(slot uint64) (*ChunkBank, error) {
	s.mu.RLock()
	// Newest first: the first chunk record at or before slot wins. Chunks
	// written without bank context are skipped.
	var paths []string
	for i := len(s.segments) - 1; i >= 0; i-- {
		seg := s.segments[i]
		for j := len(seg.chunks) - 1; j >= 0; j-- {
			// A segment's first chunk also holds the base record before it.
			if seg.chunks[j].from <= slot || (j == 0 && seg.base != nil && seg.base.Slot <= slot) {
				paths = append(paths, seg.chunks[j].bankPath())
			}
		}
	}
	s.mu.RUnlock()
	for _, p := range paths {
		d, err := s.banks.get(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		// A chunk's own record is newer than the base kept before it.
		for _, t := range []*ChunkBank{d.Top, baseChunk(d)} {
			if t != nil && t.Slot <= slot {
				c := *t // a copy: the parsed file is cached
				c.AccountsLtHash = append([]byte(nil), t.AccountsLtHash...)
				c.RecentBlockhashes = append([]BlockhashFee(nil), t.RecentBlockhashes...)
				return &c, nil
			}
		}
	}
	return nil, ErrNoBank
}

func baseChunk(d *BankData) *ChunkBank {
	if d.Base == nil {
		return nil
	}
	return &d.Base.Chunk
}

// readBase returns the base record kept with a segment's first chunk, or nil.
func readBase(c *chunkMeta) *SlotBank {
	d, err := readBankFile(c.bankPath())
	if err != nil || d.Base == nil || d.Base.Slot.Slot >= c.from {
		return nil
	}
	r := d.Base.Slot
	return &r
}

// lastBankSlot returns the slot of the newest bank record, or 0. Caller holds s.mu.
func (s *Store) lastBankSlot() uint64 {
	for i := len(s.segments) - 1; i >= 0; i-- {
		seg := s.segments[i]
		for j := len(seg.chunks) - 1; j >= 0; j-- {
			d, err := readBankFile(seg.chunks[j].bankPath())
			if err != nil || len(d.Slots) == 0 {
				continue
			}
			return d.Slots[len(d.Slots)-1].Slot
		}
	}
	return 0
}
