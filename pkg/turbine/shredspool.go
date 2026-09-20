package turbine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
// exceeded catchup keeps the lowest slots needed by replay. After live handoff,
// recent slots take priority so old data cannot starve producer repair storage.
type ShredSpool struct {
	mu           sync.Mutex
	recovery     map[uint64]spoolRecoveryIndex
	dir          string
	open         map[uint64]*spoolFile
	sizes        map[uint64]int64                      // per-slot bytes on disk (open writers included)
	seen         map[uint64]map[spoolShredKey]struct{} // distinct shreds appended this run
	validated    map[uint64]bool                       // adopted files whose record tail was checked this run
	complete     map[uint64]SpoolSlotMeta
	journal      *os.File // append-only completeness journal (complete.idx)
	bytes        int64
	maxBytes     int64
	highestSlot  uint64
	lowestSlot   uint64
	haveHighest  bool
	preferRecent bool
	floor        uint64
	closed       bool

	receiverGeneration uint64
}

// beginReceiver starts a new owner of retention updates after the previous
// receiver has stopped. A replay rewind may need slots below its old floor.
func (s *ShredSpool) beginReceiver() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receiverGeneration++
	s.floor = 0
	s.preferRecent = false
	return s.receiverGeneration
}

func (s *ShredSpool) endReceiver(generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receiverGeneration == generation {
		s.receiverGeneration++
	}
}

// SpoolSlotMeta records a slot proven FULLY assembled: every data shred
// 0..LastIndex was held when the assembler completed it. The completeness
// index is what turns the spool from a byte cache into the seed of a
// repair-serving shredstore: complete slots need zero network on restart,
// answer HighestWindowIndex honestly, and define the serving/retention set.
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
		if !s.haveHighest || slot < s.lowestSlot {
			s.lowestSlot = slot
		}
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
	s.appendLocked(slot, packet)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendShredLocked(shred, packet)
}

func (s *ShredSpool) appendShredLocked(shred *Shred, packet []byte) bool {
	key := spoolShredKey{
		type_:       shred.Type,
		index:       shred.Index,
		fecSetIndex: shred.FECSetIndex,
		position:    shred.Position,
	}
	if slotSeen := s.seen[shred.Slot]; slotSeen != nil {
		if _, duplicate := slotSeen[key]; duplicate {
			return false
		}
	}
	if !s.appendLocked(shred.Slot, packet) {
		return false
	}
	if s.seen[shred.Slot] == nil {
		s.seen[shred.Slot] = make(map[spoolShredKey]struct{})
	}
	s.seen[shred.Slot][key] = struct{}{}
	return true
}

func (s *ShredSpool) appendLocked(slot uint64, packet []byte) bool {
	if s.closed || slot < s.floor {
		return false
	}
	additional := int64(spoolRecordHeaderSize + len(packet))
	if s.sizes[slot] == 0 {
		additional += int64(len(spoolFileMagic))
	}
	if !s.ensureRoomLocked(slot, additional) {
		return false
	}
	sf := s.open[slot]
	if sf == nil {
		if s.sizes[slot] > 0 && !s.validated[slot] {
			if _, err := s.readSlotLocked(slot); err != nil {
				return false
			}
		}
		if len(s.open) >= spoolOpenFilesCap {
			s.closeOldestLocked()
		}
		f, err := os.OpenFile(s.pathFor(slot), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return false // disposable cache: failures degrade to repair
		}
		sf = &spoolFile{f: f, w: bufio.NewWriterSize(f, 64<<10)}
		s.open[slot] = sf
		if s.sizes[slot] == 0 {
			if _, err := sf.w.Write(spoolFileMagic[:]); err != nil {
				s.closeSlotLocked(slot)
				_ = os.Remove(s.pathFor(slot))
				return false
			}
			s.sizes[slot] = int64(len(spoolFileMagic))
			s.bytes += int64(len(spoolFileMagic))
			s.validated[slot] = true
			if !s.haveHighest || slot < s.lowestSlot {
				s.lowestSlot = slot
			}
			if !s.haveHighest || slot > s.highestSlot {
				s.highestSlot, s.haveHighest = slot, true
			}
		}
	}
	var hdr [spoolRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(packet)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(packet))
	if _, err := sf.w.Write(hdr[:]); err != nil {
		return false
	}
	if _, err := sf.w.Write(packet); err != nil {
		return false
	}
	written := int64(spoolRecordHeaderSize + len(packet))
	s.sizes[slot] += written
	s.bytes += written
	if idx := s.recovery[slot]; idx != nil {
		if err := idx.add(slot, packet, s.sizes[slot]-written); err != nil {
			delete(s.recovery, slot)
		}
	}
	return true
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

// FlushSlot releases buffered packets and closes the slot's append handle.
// This is best-effort cache retention, not fsync or a power-loss guarantee.
func (s *ShredSpool) FlushSlot(slot uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeSlotLocked(slot)
}

func (s *ShredSpool) closeSlotLocked(slot uint64) {
	if sf := s.open[slot]; sf != nil {
		_ = sf.w.Flush()
		_ = sf.f.Close()
		delete(s.open, slot)
	}
}

// ensureRoomLocked rejects lower-priority packets in O(1) when full. Catchup
// prefers lower slots; live replay prefers recent slots. Bounds are recomputed
// only when a whole slot is evicted, not for every rejected packet.
func (s *ShredSpool) ensureRoomLocked(slot uint64, additional int64) bool {
	if s.maxBytes <= 0 || s.bytes+additional <= s.maxBytes {
		return true
	}
	for s.bytes+additional > s.maxBytes {
		if !s.haveHighest {
			return additional <= s.maxBytes
		}
		victim := s.highestSlot
		if s.preferRecent {
			if slot <= s.lowestSlot {
				return false
			}
			victim = s.lowestSlot
		} else if slot >= s.highestSlot {
			return false
		}
		s.dropSlotLocked(victim)
	}
	return true
}

func (s *ShredSpool) dropSlotLocked(slot uint64) {
	s.closeSlotLocked(slot)
	s.bytes -= s.sizes[slot]
	delete(s.sizes, slot)
	delete(s.seen, slot)
	delete(s.validated, slot)
	delete(s.recovery, slot)
	delete(s.complete, slot)
	_ = os.Remove(s.pathFor(slot))
	if s.journal != nil {
		// Supersede any older completion record if this slot number is later
		// re-created before the journal is compacted on restart.
		s.journal.Write(spoolJournalRecord(slot, SpoolSlotMeta{}))
	}
	if s.haveHighest && (slot == s.highestSlot || slot == s.lowestSlot) {
		s.recomputeBoundsLocked()
	}
}

func (s *ShredSpool) recomputeBoundsLocked() {
	s.haveHighest = false
	s.highestSlot = 0
	s.lowestSlot = 0
	for slot := range s.sizes {
		if !s.haveHighest || slot < s.lowestSlot {
			s.lowestSlot = slot
		}
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
	s.discardSlot(slot, 0)
}

func (s *ShredSpool) discardSlot(slot, generation uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || (generation != 0 && generation != s.receiverGeneration) {
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
		delete(s.recovery, slot)
		if err := os.Truncate(path, int64(validEnd)); err != nil {
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
	return packets, nil
}

// SetFloor advances the retention floor, deleting slot files strictly below
// it. Idempotent, monotonic.
func (s *ShredSpool) SetFloor(slot uint64) {
	s.setFloor(slot, 0)
}

func (s *ShredSpool) setFloor(slot, generation uint64) {
	s.setRetention(slot, generation, false)
}

func (s *ShredSpool) setReplayFloor(slot, generation uint64) {
	s.setRetention(slot, generation, true)
}

func (s *ShredSpool) setRetention(slot, generation uint64, recent bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || (generation != 0 && generation != s.receiverGeneration) {
		return
	}
	s.preferRecent = recent
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
