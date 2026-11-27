package history

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
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

// BankData is the bank context of one fold.
type BankData struct {
	Slots []SlotBank // ascending
	Top   *ChunkBank
}

// ErrNoBank: no bank record for that slot (skipped, not folded, or outside history).
var ErrNoBank = errors.New("history: no bank record for the slot")

const (
	bankMagic     = "MHBANK01"
	slotBankBytes = 6*8 + 64
)

func bankFileName(from, to uint64) string { return fmt.Sprintf("bank-%020d-%020d.hbk", from, to) }

func encodeBank(b *BankData) []byte {
	out := []byte(bankMagic)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(b.Slots)))
	for _, r := range b.Slots {
		out = binary.LittleEndian.AppendUint64(out, r.Slot)
		out = binary.LittleEndian.AppendUint64(out, r.Parent)
		out = binary.LittleEndian.AppendUint64(out, r.BlockHeight)
		out = binary.LittleEndian.AppendUint64(out, r.LamportsPerSignature)
		out = binary.LittleEndian.AppendUint64(out, r.PrevLamportsPerSignature)
		out = binary.LittleEndian.AppendUint64(out, r.NumSignatures)
		out = append(out, r.Bankhash[:]...)
		out = append(out, r.Blockhash[:]...)
	}
	if t := b.Top; t != nil {
		out = append(out, 1)
		out = binary.LittleEndian.AppendUint64(out, t.Slot)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(t.AccountsLtHash)))
		out = append(out, t.AccountsLtHash...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(t.RecentBlockhashes)))
		for _, e := range t.RecentBlockhashes {
			out = append(out, e.Blockhash[:]...)
			out = binary.LittleEndian.AppendUint64(out, e.LamportsPerSignature)
		}
		out = append(out, t.EvictedBlockhash[:]...)
	} else {
		out = append(out, 0)
	}
	return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
}

var errBank = errors.New("history: malformed bank file")

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
	if len(p) < n*slotBankBytes+1 {
		return nil, errBank
	}
	d := &BankData{Slots: make([]SlotBank, n)}
	for i := range d.Slots {
		r := &d.Slots[i]
		r.Slot = binary.LittleEndian.Uint64(p)
		r.Parent = binary.LittleEndian.Uint64(p[8:])
		r.BlockHeight = binary.LittleEndian.Uint64(p[16:])
		r.LamportsPerSignature = binary.LittleEndian.Uint64(p[24:])
		r.PrevLamportsPerSignature = binary.LittleEndian.Uint64(p[32:])
		r.NumSignatures = binary.LittleEndian.Uint64(p[40:])
		copy(r.Bankhash[:], p[48:80])
		copy(r.Blockhash[:], p[80:112])
		p = p[slotBankBytes:]
	}
	if p[0] == 1 {
		p = p[1:]
		if len(p) < 12 {
			return nil, errBank
		}
		t := &ChunkBank{Slot: binary.LittleEndian.Uint64(p)}
		l := int(binary.LittleEndian.Uint32(p[8:]))
		p = p[12:]
		if len(p) < l+4 {
			return nil, errBank
		}
		t.AccountsLtHash = append([]byte(nil), p[:l]...)
		p = p[l:]
		m := int(binary.LittleEndian.Uint32(p))
		p = p[4:]
		if len(p) < m*40+32 {
			return nil, errBank
		}
		t.RecentBlockhashes = make([]BlockhashFee, m)
		for i := range t.RecentBlockhashes {
			copy(t.RecentBlockhashes[i].Blockhash[:], p[:32])
			t.RecentBlockhashes[i].LamportsPerSignature = binary.LittleEndian.Uint64(p[32:])
			p = p[40:]
		}
		copy(t.EvictedBlockhash[:], p[:32])
		d.Top = t
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
			if seg.chunks[j].from <= slot {
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
		if d.Top != nil && d.Top.Slot <= slot {
			c := *d.Top // a copy: the parsed file is cached
			c.AccountsLtHash = append([]byte(nil), d.Top.AccountsLtHash...)
			c.RecentBlockhashes = append([]BlockhashFee(nil), d.Top.RecentBlockhashes...)
			return &c, nil
		}
	}
	return nil, ErrNoBank
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
