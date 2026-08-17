package turbine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ShredSpool is a disposable on-disk cache of VERIFIED raw shreds, one
// append-only file per slot. It exists so catchup never has to hold either
// far-future decoded blocks or far-future shred state in RAM: shreds outside
// the assembly windows are appended here at receive time, and the hydrator
// reads whole slots back in batches as replay approaches. Repaired shreds
// are written too, so a slot reset re-hydrates from disk instead of
// re-fetching one shred per round trip.
//
// Durability is explicitly NOT a goal: no fsync. Each record is checksummed,
// though, and an interrupted tail is truncated before either hydration or a
// later append. That distinction matters: appending after a torn record and
// merely stopping at it on read would make all later repairs unreachable and
// could feed a packet assembled across the crash boundary back into replay.
// Files below the floor are deleted as replay advances; when the byte cap is
// exceeded the HIGHEST slots are dropped first — the live edge is cheap to
// re-fetch near the tip, the low end borders replay and is what repair would
// otherwise pay for dearly.
type ShredSpool struct {
	mu          sync.Mutex
	dir         string
	open        map[uint64]*spoolFile
	sizes       map[uint64]int64                      // per-slot bytes on disk (open writers included)
	seen        map[uint64]map[spoolShredKey]struct{} // distinct shreds appended this run
	validated   map[uint64]bool                       // adopted files whose record tail was checked this run
	dataIndex   map[uint64]*spoolDataIndex            // lazily-built exact data-shred record offsets
	complete    map[uint64]SpoolSlotMeta
	journal     *os.File // append-only completeness journal (complete.idx)
	bytes       int64
	maxBytes    int64
	highestSlot uint64
	haveHighest bool
	floor       uint64
	closed      bool
}

// SpoolSlotMeta records a slot proven FULLY assembled: every data shred
// 0..LastIndex was held when the assembler completed it. The completeness
// index turns the spool from a byte cache into the seed of a repair-serving
// shredstore and defines its retention set. Serve-repair itself indexes only
// canonical raw data shreds present in the spool; FEC-reconstructed data is not
// persisted without a complete, independently verifiable wire payload.
type SpoolSlotMeta struct {
	LastIndex uint32
	Shreds    uint32
}

const spoolJournalName = "complete.idx"
const spoolJournalRecordSize = 8 + 4 + 4

var spoolFileMagic = [8]byte{'M', 'I', 'T', 'H', 'S', 'P', 2, 0}

const spoolRecordHeaderSize = 4 + 4 // packet length + CRC32

type spoolFile struct {
	f *os.File
	w *bufio.Writer
}

type spoolRecordRef struct {
	offset       int64
	storedLength uint32
	packetLength uint32
	crc          uint32
}

type spoolDataIndex struct {
	records     map[uint32]spoolRecordRef
	highest     uint32
	haveHighest bool
}

type spoolShredKey struct {
	type_       ShredType
	index       uint32
	fecSetIndex uint32
	position    uint16
}

const (
	spoolOpenFilesCap = 32
	// Slots this close to the shred edge always assemble in RAM — broadcast
	// plus FEC recovery completes them essentially free.
	spoolLiveAssemblyLag = uint64(64)
)

// OpenShredSpool opens (or creates) a spool directory, adopting any slot
// files a previous process left behind — after a restart they hydrate
// instead of re-repairing.
func OpenShredSpool(dir string, maxBytes int64) (*ShredSpool, error) {
	if dir == "" {
		return nil, fmt.Errorf("shred spool needs a directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &ShredSpool{
		dir:       dir,
		open:      make(map[uint64]*spoolFile),
		sizes:     make(map[uint64]int64),
		seen:      make(map[uint64]map[spoolShredKey]struct{}),
		validated: make(map[uint64]bool),
		dataIndex: make(map[uint64]*spoolDataIndex),
		complete:  make(map[uint64]SpoolSlotMeta),
		maxBytes:  maxBytes,
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		slot, ok := spoolSlotFromName(e.Name())
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		// The pre-v2 format had no integrity check. It cannot distinguish a
		// valid packet from one completed with bytes belonging to the next
		// post-crash append, so a disposable legacy cache must be re-repaired.
		f, err := os.Open(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var magic [len(spoolFileMagic)]byte
		_, readErr := io.ReadFull(f, magic[:])
		_ = f.Close()
		if readErr != nil || magic != spoolFileMagic {
			_ = os.Remove(filepath.Join(dir, e.Name()))
			continue
		}
		s.sizes[slot] = info.Size()
		s.bytes += info.Size()
		if !s.haveHighest || slot > s.highestSlot {
			s.highestSlot, s.haveHighest = slot, true
		}
	}
	s.loadJournal()
	return s, nil
}

// loadJournal restores completeness markers for slot files that still
// exist, then rewrites the journal compacted (dropping entries for deleted
// slots). A truncated tail record is discarded.
func (s *ShredSpool) loadJournal() {
	path := filepath.Join(s.dir, spoolJournalName)
	data, err := os.ReadFile(path)
	if err == nil {
		for off := 0; off+spoolJournalRecordSize <= len(data); off += spoolJournalRecordSize {
			slot := binary.LittleEndian.Uint64(data[off:])
			meta := SpoolSlotMeta{
				LastIndex: binary.LittleEndian.Uint32(data[off+8:]),
				Shreds:    binary.LittleEndian.Uint32(data[off+12:]),
			}
			// A zero-shred record is a tombstone. Process it even when a
			// newly-created partial file for this slot already exists: it
			// supersedes an older completion record for the discarded file.
			if meta.Shreds == 0 {
				delete(s.complete, slot)
				continue
			}
			if _, exists := s.sizes[slot]; !exists {
				continue // slot file gone: stale entry
			}
			s.complete[slot] = meta
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return // journal unavailable: completeness degrades to per-run only
	}
	for slot, meta := range s.complete {
		f.Write(spoolJournalRecord(slot, meta))
	}
	s.journal = f
}

func spoolJournalRecord(slot uint64, meta SpoolSlotMeta) []byte {
	rec := make([]byte, spoolJournalRecordSize)
	binary.LittleEndian.PutUint64(rec, slot)
	binary.LittleEndian.PutUint32(rec[8:], meta.LastIndex)
	binary.LittleEndian.PutUint32(rec[12:], meta.Shreds)
	return rec
}

// MarkComplete records that the slot fully assembled (data shreds
// 0..lastIndex all held). Called by the assembler's completion hook, so
// hydrating an adopted file re-marks it for free. Idempotent.
func (s *ShredSpool) MarkComplete(slot uint64, lastIndex uint32, shreds uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot < s.floor || shreds == 0 {
		return
	}
	if _, done := s.complete[slot]; done {
		return
	}
	meta := SpoolSlotMeta{LastIndex: lastIndex, Shreds: shreds}
	s.complete[slot] = meta
	if s.journal != nil {
		s.journal.Write(spoolJournalRecord(slot, meta))
	}
}

// IsComplete reports whether the slot was proven fully assembled, with its
// metadata.
func (s *ShredSpool) IsComplete(slot uint64) (SpoolSlotMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.complete[slot]
	return meta, ok
}

// CompleteSlots reports how many spooled slots are proven complete.
func (s *ShredSpool) CompleteSlots() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.complete)
}

func spoolSlotFromName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "s") || !strings.HasSuffix(name, ".shreds") {
		return 0, false
	}
	slot, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "s"), ".shreds"), 10, 64)
	return slot, err == nil
}

func (s *ShredSpool) pathFor(slot uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("s%d.shreds", slot))
}

// Append stores one verified shred packet under its slot. It consumes packet
// synchronously and does not retain the slice after returning.
func (s *ShredSpool) Append(slot uint64, packet []byte) {
	if len(packet) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.appendLocked(slot, packet)
}

// AppendShred stores a verified shred only once per process run. Network
// Turbine and serve-repair commonly deliver the same packet many times; those
// duplicates must not consume disk bandwidth or repeatedly exercise cap
// eviction before the assembler gets a chance to classify them. The packet is
// consumed synchronously and is not retained after this method returns.
func (s *ShredSpool) AppendShred(shred *Shred, packet []byte) bool {
	if shred == nil || len(packet) == 0 {
		return false
	}
	key := spoolShredKey{
		type_:       shred.Type,
		index:       shred.Index,
		fecSetIndex: shred.FECSetIndex,
		position:    shred.Position,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if slotSeen := s.seen[shred.Slot]; slotSeen != nil {
		if _, duplicate := slotSeen[key]; duplicate {
			return false
		}
	}
	// ParseShred exposes the canonical payload length. Repair responses carry a
	// trailing nonce which belongs to the response envelope, not the stored
	// shred; never persist it or a later serve-repair response would nest nonces.
	if len(shred.Payload) > 0 && len(shred.Payload) < len(packet) {
		packet = packet[:len(shred.Payload)]
	}
	record, ok := s.appendLocked(shred.Slot, packet)
	if !ok {
		return false
	}
	if s.seen[shred.Slot] == nil {
		s.seen[shred.Slot] = make(map[spoolShredKey]struct{})
	}
	s.seen[shred.Slot][key] = struct{}{}
	if shred.Type == ShredTypeData && isMerkleVariant(shred.Variant) {
		s.indexDataShredLocked(shred.Slot, shred.Index, record)
	}
	return true
}

func (s *ShredSpool) appendLocked(slot uint64, packet []byte) (spoolRecordRef, bool) {
	if s.closed || slot < s.floor {
		return spoolRecordRef{}, false
	}
	additional := int64(spoolRecordHeaderSize + len(packet))
	if s.sizes[slot] == 0 {
		additional += int64(len(spoolFileMagic))
	}
	if !s.ensureRoomLocked(slot, additional) {
		return spoolRecordRef{}, false
	}
	sf := s.open[slot]
	if sf == nil {
		if s.sizes[slot] > 0 && !s.validated[slot] {
			if _, err := s.readSlotLocked(slot); err != nil {
				return spoolRecordRef{}, false
			}
		}
		if len(s.open) >= spoolOpenFilesCap {
			s.closeOldestLocked()
		}
		f, err := os.OpenFile(s.pathFor(slot), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return spoolRecordRef{}, false // disposable cache: failures degrade to repair
		}
		sf = &spoolFile{f: f, w: bufio.NewWriterSize(f, 64<<10)}
		s.open[slot] = sf
		if s.sizes[slot] == 0 {
			if _, err := sf.w.Write(spoolFileMagic[:]); err != nil {
				s.closeSlotLocked(slot)
				_ = os.Remove(s.pathFor(slot))
				return spoolRecordRef{}, false
			}
			s.sizes[slot] = int64(len(spoolFileMagic))
			s.bytes += int64(len(spoolFileMagic))
			s.validated[slot] = true
			if !s.haveHighest || slot > s.highestSlot {
				s.highestSlot, s.haveHighest = slot, true
			}
		}
	}
	packetOffset := s.sizes[slot] + spoolRecordHeaderSize
	packetCRC := crc32.ChecksumIEEE(packet)
	var hdr [spoolRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(packet)))
	binary.LittleEndian.PutUint32(hdr[4:], packetCRC)
	if _, err := sf.w.Write(hdr[:]); err != nil {
		return spoolRecordRef{}, false
	}
	if _, err := sf.w.Write(packet); err != nil {
		return spoolRecordRef{}, false
	}
	written := int64(spoolRecordHeaderSize + len(packet))
	s.sizes[slot] += written
	s.bytes += written
	return spoolRecordRef{
		offset:       packetOffset,
		storedLength: uint32(len(packet)),
		packetLength: uint32(len(packet)),
		crc:          packetCRC,
	}, true
}

func (s *ShredSpool) indexDataShredLocked(slot uint64, index uint32, record spoolRecordRef) {
	data := s.dataIndex[slot]
	if data == nil {
		data = &spoolDataIndex{records: make(map[uint32]spoolRecordRef)}
		s.dataIndex[slot] = data
	}
	if _, exists := data.records[index]; exists {
		return
	}
	data.records[index] = record
	if !data.haveHighest || index > data.highest {
		data.highest = index
		data.haveHighest = true
	}
}

// closeOldestLocked flushes and closes one open handle (lowest slot: it is
// the most likely to be hydrated soon, so its buffered tail must be on disk).
func (s *ShredSpool) closeOldestLocked() {
	var victim uint64
	first := true
	for slot := range s.open {
		if first || slot < victim {
			victim = slot
			first = false
		}
	}
	if !first {
		s.closeSlotLocked(victim)
	}
}

func (s *ShredSpool) closeSlotLocked(slot uint64) {
	if sf := s.open[slot]; sf != nil {
		_ = sf.w.Flush()
		_ = sf.f.Close()
		delete(s.open, slot)
	}
}

// ensureRoomLocked applies the retention policy before writing. Once the
// spool is full, a new slot at/above the highest retained slot is rejected in
// O(1): writing and immediately deleting it was both useless and, formerly,
// performed an O(number-of-slots) highest-slot scan for every packet. A lower
// catch-up slot may evict whole future slots; recomputing the maximum once per
// evicted slot is rare and preserves the low-end-first retention contract.
func (s *ShredSpool) ensureRoomLocked(slot uint64, additional int64) bool {
	if s.maxBytes <= 0 || s.bytes+additional <= s.maxBytes {
		return true
	}
	for s.bytes+additional > s.maxBytes {
		if !s.haveHighest {
			return additional <= s.maxBytes
		}
		if slot >= s.highestSlot {
			return false
		}
		s.dropSlotLocked(s.highestSlot)
	}
	return true
}

func (s *ShredSpool) dropSlotLocked(slot uint64) {
	s.closeSlotLocked(slot)
	s.bytes -= s.sizes[slot]
	delete(s.sizes, slot)
	delete(s.seen, slot)
	delete(s.validated, slot)
	delete(s.dataIndex, slot)
	delete(s.complete, slot)
	_ = os.Remove(s.pathFor(slot))
	if s.journal != nil {
		// Supersede any older completion record if this slot number is later
		// re-created before the journal is compacted on restart.
		s.journal.Write(spoolJournalRecord(slot, SpoolSlotMeta{}))
	}
	if s.haveHighest && slot == s.highestSlot {
		s.recomputeHighestLocked()
	}
}

func (s *ShredSpool) recomputeHighestLocked() {
	s.haveHighest = false
	s.highestSlot = 0
	for slot := range s.sizes {
		if !s.haveHighest || slot > s.highestSlot {
			s.highestSlot, s.haveHighest = slot, true
		}
	}
}

// DiscardSlot removes both persisted packets and the completeness marker for
// a poisoned or rejected slot. The journal tombstone prevents an older
// completion record from being resurrected if repair immediately recreates a
// partial file with the same slot number.
func (s *ShredSpool) DiscardSlot(slot uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.dropSlotLocked(slot)
}

// HasSlot reports whether any shreds are spooled for slot.
func (s *ShredSpool) HasSlot(slot uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sizes[slot] > int64(len(spoolFileMagic))
}

// SlotsInRange returns spooled slots within [lo, hi], ascending.
func (s *ShredSpool) SlotsInRange(lo, hi uint64) []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []uint64
	for slot, size := range s.sizes {
		if size > int64(len(spoolFileMagic)) && slot >= lo && slot <= hi {
			out = append(out, slot)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ReadSlot returns every intact spooled shred packet for slot. A truncated or
// corrupt tail is removed atomically under the spool lock, so later repairs
// append at the last valid record rather than behind unreachable garbage.
func (s *ShredSpool) ReadSlot(slot uint64) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readSlotLocked(slot)
}

func (s *ShredSpool) readSlotLocked(slot uint64) ([][]byte, error) {
	s.closeSlotLocked(slot) // flush any buffered tail
	path := s.pathFor(slot)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < len(spoolFileMagic) || string(data[:len(spoolFileMagic)]) != string(spoolFileMagic[:]) {
		s.dropSlotLocked(slot)
		return nil, fmt.Errorf("invalid shred spool header for slot %d", slot)
	}
	var packets [][]byte
	validEnd := len(spoolFileMagic)
	for off := validEnd; off < len(data); {
		if off+spoolRecordHeaderSize > len(data) {
			break
		}
		n := int(binary.LittleEndian.Uint32(data[off : off+4]))
		wantCRC := binary.LittleEndian.Uint32(data[off+4 : off+spoolRecordHeaderSize])
		packetStart := off + spoolRecordHeaderSize
		packetEnd := packetStart + n
		if n <= 0 || packetEnd < packetStart || packetEnd > len(data) {
			break
		}
		packet := data[packetStart:packetEnd]
		if crc32.ChecksumIEEE(packet) != wantCRC {
			break
		}
		packets = append(packets, packet)
		off = packetEnd
		validEnd = packetEnd
	}
	if validEnd != len(data) {
		if err := os.Truncate(path, int64(validEnd)); err != nil {
			return nil, fmt.Errorf("truncate corrupt shred spool tail for slot %d: %w", slot, err)
		}
		oldSize := s.sizes[slot]
		s.sizes[slot] = int64(validEnd)
		s.bytes += int64(validEnd) - oldSize
		delete(s.complete, slot)
		delete(s.dataIndex, slot)
		if s.journal != nil {
			s.journal.Write(spoolJournalRecord(slot, SpoolSlotMeta{}))
		}
	}
	s.validated[slot] = true
	return packets, nil
}

// GetDataShred returns the canonical data-shred packet for an exact index.
// The on-disk record index is built lazily for adopted slot files and updated
// incrementally for new appends, so serving does not rescan a whole slot per
// request.
func (s *ShredSpool) GetDataShred(slot uint64, index uint64) ([]byte, bool, error) {
	if index > math.MaxUint32 {
		return nil, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, fmt.Errorf("shred spool is closed")
	}
	data, err := s.ensureDataIndexLocked(slot)
	if err != nil {
		return nil, false, err
	}
	record, ok := data.records[uint32(index)]
	if !ok {
		return nil, false, nil
	}
	packet, err := s.readRecordLocked(slot, record)
	return packet, err == nil, err
}

// GetHighestDataShredFrom returns the stored data shred with the highest
// index greater than or equal to minIndex.
func (s *ShredSpool) GetHighestDataShredFrom(slot uint64, minIndex uint64) ([]byte, bool, error) {
	if minIndex > math.MaxUint32 {
		return nil, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, fmt.Errorf("shred spool is closed")
	}
	data, err := s.ensureDataIndexLocked(slot)
	if err != nil {
		return nil, false, err
	}
	minimum := uint32(minIndex)
	if !data.haveHighest || data.highest < minimum {
		return nil, false, nil
	}
	record, ok := data.records[data.highest]
	if !ok {
		delete(s.dataIndex, slot)
		return nil, false, fmt.Errorf("shred spool data index lost highest shred %d for slot %d", data.highest, slot)
	}
	packet, err := s.readRecordLocked(slot, record)
	return packet, err == nil, err
}

func (s *ShredSpool) ensureDataIndexLocked(slot uint64) (*spoolDataIndex, error) {
	if data := s.dataIndex[slot]; data != nil {
		return data, nil
	}
	dataIndex := &spoolDataIndex{records: make(map[uint32]spoolRecordRef)}
	if s.sizes[slot] <= int64(len(spoolFileMagic)) {
		s.dataIndex[slot] = dataIndex
		return dataIndex, nil
	}

	s.closeSlotLocked(slot)
	data, err := os.ReadFile(s.pathFor(slot))
	if err != nil {
		return nil, err
	}
	if len(data) < len(spoolFileMagic) || string(data[:len(spoolFileMagic)]) != string(spoolFileMagic[:]) {
		s.dropSlotLocked(slot)
		return nil, fmt.Errorf("invalid shred spool header for slot %d", slot)
	}

	validEnd := len(spoolFileMagic)
	for off := validEnd; off < len(data); {
		if off+spoolRecordHeaderSize > len(data) {
			break
		}
		storedLength := int(binary.LittleEndian.Uint32(data[off : off+4]))
		wantCRC := binary.LittleEndian.Uint32(data[off+4 : off+spoolRecordHeaderSize])
		packetStart := off + spoolRecordHeaderSize
		packetEnd := packetStart + storedLength
		if storedLength <= 0 || packetEnd < packetStart || packetEnd > len(data) {
			break
		}
		packet := data[packetStart:packetEnd]
		if crc32.ChecksumIEEE(packet) != wantCRC {
			break
		}
		if len(packet) > shredFECSetIndexOffset+3 {
			shredType, classifyErr := classifyVariant(packet[shredVariantOffset])
			packetSlot := binary.LittleEndian.Uint64(packet[shredSlotOffset : shredSlotOffset+8])
			if classifyErr == nil && shredType == ShredTypeData && isMerkleVariant(packet[shredVariantOffset]) && packetSlot == slot {
				index := binary.LittleEndian.Uint32(packet[shredIndexOffset : shredIndexOffset+4])
				packetLength := storedLength
				// Merkle data shreds have a fixed canonical payload. Older spool
				// files may contain the four-byte repair nonce after it.
				if packetLength > dataPayloadSize {
					packetLength = dataPayloadSize
				}
				record := spoolRecordRef{
					offset:       int64(packetStart),
					storedLength: uint32(storedLength),
					packetLength: uint32(packetLength),
					crc:          wantCRC,
				}
				if _, exists := dataIndex.records[index]; !exists {
					dataIndex.records[index] = record
				}
				if !dataIndex.haveHighest || index > dataIndex.highest {
					dataIndex.highest = index
					dataIndex.haveHighest = true
				}
			}
		}
		off = packetEnd
		validEnd = packetEnd
	}
	if validEnd != len(data) {
		if err := os.Truncate(s.pathFor(slot), int64(validEnd)); err != nil {
			return nil, fmt.Errorf("truncate corrupt shred spool tail for slot %d: %w", slot, err)
		}
		oldSize := s.sizes[slot]
		s.sizes[slot] = int64(validEnd)
		s.bytes += int64(validEnd) - oldSize
		delete(s.complete, slot)
		if s.journal != nil {
			s.journal.Write(spoolJournalRecord(slot, SpoolSlotMeta{}))
		}
	}
	s.validated[slot] = true
	s.dataIndex[slot] = dataIndex
	return dataIndex, nil
}

func (s *ShredSpool) readRecordLocked(slot uint64, record spoolRecordRef) ([]byte, error) {
	// A just-appended record may still live in the per-slot bufio writer. Flush
	// it before opening a separate read handle; keep the append handle open so a
	// busy repair service does not turn every response into an open/close cycle.
	if sf := s.open[slot]; sf != nil {
		if err := sf.w.Flush(); err != nil {
			return nil, err
		}
	}
	f, err := os.Open(s.pathFor(slot))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stored := make([]byte, int(record.storedLength))
	if _, err := f.ReadAt(stored, record.offset); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(stored) != record.crc {
		delete(s.dataIndex, slot)
		return nil, fmt.Errorf("shred spool record checksum mismatch for slot %d", slot)
	}
	if record.packetLength > record.storedLength {
		return nil, fmt.Errorf("invalid shred spool record length %d/%d for slot %d", record.packetLength, record.storedLength, slot)
	}
	return stored[:record.packetLength], nil
}

// SetFloor advances the retention floor, deleting slot files strictly below
// it. Idempotent, monotonic.
func (s *ShredSpool) SetFloor(slot uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot <= s.floor {
		return
	}
	s.floor = slot
	for spooled := range s.sizes {
		if spooled < slot {
			s.dropSlotLocked(spooled)
		}
	}
}

// Stats reports spooled slot count and total bytes.
func (s *ShredSpool) Stats() (slots int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, size := range s.sizes {
		if size > int64(len(spoolFileMagic)) {
			slots++
		}
	}
	return slots, s.bytes
}

// Close flushes and closes all open handles (files remain for the next
// opener — a stream restart or the block source after a prewarm handoff).
// Buffered tails MUST reach disk here: the next opener sizes and reads the
// directory fresh and can only see flushed bytes. Later writes no-op.
func (s *ShredSpool) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for slot := range s.open {
		s.closeSlotLocked(slot)
	}
	if s.journal != nil {
		_ = s.journal.Close()
		s.journal = nil
	}
}
