package turbine

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// Round-trip: appended packets come back whole; the floor deletes below;
// the byte cap drops the HIGHEST slots first (the live edge is cheap to
// re-fetch; the low end borders replay).
func TestShredSpoolRoundTrip(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()

	spool.Append(100, []byte("alpha"))
	spool.Append(100, []byte("beta"))
	spool.Append(101, []byte("gamma"))

	if !spool.HasSlot(100) || !spool.HasSlot(101) || spool.HasSlot(102) {
		t.Fatalf("HasSlot bookkeeping wrong")
	}
	got, err := spool.ReadSlot(100)
	if err != nil || len(got) != 2 || string(got[0]) != "alpha" || string(got[1]) != "beta" {
		t.Fatalf("ReadSlot(100) = %v, %v", got, err)
	}
	if slots := spool.SlotsInRange(100, 101); len(slots) != 2 || slots[0] != 100 || slots[1] != 101 {
		t.Fatalf("SlotsInRange = %v", slots)
	}

	spool.SetFloor(101)
	if spool.HasSlot(100) {
		t.Fatalf("floor must delete below")
	}
	spool.Append(99, []byte("stale"))
	if spool.HasSlot(99) {
		t.Fatalf("appends below the floor must be ignored")
	}
}

func TestShredSpoolDoesNotRetainAppendBuffer(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()

	packet := []byte("original")
	spool.Append(100, packet)
	copy(packet, "modified")

	got, err := spool.ReadSlot(100)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 || string(got[0]) != "original" {
		t.Fatalf("spool retained append buffer: %q", got)
	}
}

func TestShredSpoolCapDropsHighest(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 80)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()

	payload := make([]byte, 20)
	spool.Append(10, payload) // 36 bytes (file + record headers included)
	spool.Append(11, payload) // 72
	spool.Append(12, payload) // 108 > 80 -> drop highest (12)
	if spool.HasSlot(12) {
		t.Fatalf("cap must drop the highest slot")
	}
	if !spool.HasSlot(10) || !spool.HasSlot(11) {
		t.Fatalf("low slots must survive the cap")
	}
}

func TestShredSpoolDeduplicatesAndRejectsOverCapFutureWithoutChurn(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 80)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()

	payload := make([]byte, 20)
	low := &Shred{Slot: 10, Type: ShredTypeData, Index: 0}
	if !spool.AppendShred(low, payload) {
		t.Fatal("first distinct shred was not stored")
	}
	if spool.AppendShred(low, payload) {
		t.Fatal("duplicate shred was stored twice")
	}
	spool.Append(11, payload) // fill to 72/80 bytes
	_, before := spool.Stats()

	future := &Shred{Slot: 12, Type: ShredTypeData, Index: 0}
	for i := 0; i < 100; i++ {
		if spool.AppendShred(future, payload) {
			t.Fatal("over-cap future shred should be rejected")
		}
	}
	_, after := spool.Stats()
	if after != before {
		t.Fatalf("over-cap future traffic churned spool bytes: before=%d after=%d", before, after)
	}
	if !spool.HasSlot(10) || !spool.HasSlot(11) || spool.HasSlot(12) {
		t.Fatalf("cap retention changed: low10=%v low11=%v future12=%v", spool.HasSlot(10), spool.HasSlot(11), spool.HasSlot(12))
	}
}

// A restart adopts leftover slot files: they hydrate instead of re-repairing.
func TestShredSpoolAdoptsExistingFiles(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first.Append(500, []byte("persisted"))
	first.Close()

	second, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	if !second.HasSlot(500) {
		t.Fatalf("reopen must adopt existing slot files")
	}
	got, err := second.ReadSlot(500)
	if err != nil || len(got) != 1 || string(got[0]) != "persisted" {
		t.Fatalf("ReadSlot after reopen = %v, %v", got, err)
	}
}

// A crash can leave a record header plus only part of its payload. If a new
// process appends repairs behind that fragment, a length-only format can
// accidentally consume bytes from the new record as the old packet and feed
// poisoned shred data to replay. The checksum makes the boundary detectable,
// and validation before the first post-restart append preserves later data.
func TestShredSpoolRepairsTornTailBeforeAppending(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first.Append(500, []byte("intact"))
	first.Close()

	path := filepath.Join(dir, "s500.shreds")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open torn tail: %v", err)
	}
	intended := []byte("interrupted-packet")
	var hdr [spoolRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(intended)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(intended))
	if _, err := f.Write(append(hdr[:], intended[:3]...)); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	_ = f.Close()

	second, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	// Append is intentionally before ReadSlot: restart-time repair traffic
	// must not be hidden behind the torn record.
	second.Append(500, []byte("after-restart"))
	got, err := second.ReadSlot(500)
	if err != nil {
		t.Fatalf("read repaired file: %v", err)
	}
	if len(got) != 2 || string(got[0]) != "intact" || string(got[1]) != "after-restart" {
		t.Fatalf("ReadSlot after torn tail repair = %q, want intact + after-restart", got)
	}
	wantSize := len(spoolFileMagic) +
		spoolRecordHeaderSize + len("intact") +
		spoolRecordHeaderSize + len("after-restart")
	if info, err := os.Stat(path); err != nil || info.Size() != int64(wantSize) {
		t.Fatalf("repaired size = %v, %v; want %d", info, err, wantSize)
	}
}

func TestShredSpoolDiscardTombstonesOldCompletion(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first.Append(800, []byte("old-complete-block"))
	first.MarkComplete(800, 9, 10)
	first.DiscardSlot(800)
	first.Append(800, []byte("new-partial-block"))
	first.Close()

	second, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	if meta, ok := second.IsComplete(800); ok {
		t.Fatalf("discarded completion resurrected for replacement file: %+v", meta)
	}
	got, err := second.ReadSlot(800)
	if err != nil || len(got) != 1 || string(got[0]) != "new-partial-block" {
		t.Fatalf("replacement partial file = %q, %v", got, err)
	}
}

func TestShredSpoolDropsLegacyUncheckedFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s900.shreds")
	if err := os.WriteFile(path, []byte("legacy-unchecked-cache"), 0o644); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}
	spool, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()
	if spool.HasSlot(900) {
		t.Fatalf("legacy unchecked slot was adopted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("legacy file was not removed: %v", err)
	}
}

// The handoff contract: buffered slot-file tails and the completeness
// journal reach disk on Close, because the NEXT opener of the same directory
// (the block source after a prewarm handoff, or a turbine stream restart)
// sizes and reads it fresh and can only see flushed bytes. Writes after
// Close are dropped rather than silently reopening handles nobody closes.
func TestShredSpoolCloseFlushesBufferedTailsForNextOpener(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first.Append(700, []byte("buffered-tail"))
	first.MarkComplete(700, 3, 4)

	// Premise: the record is still in the bufio buffer — the on-disk file
	// exists but is empty, which is exactly what a second opener would
	// (wrongly) observe without the Close flush.
	info, err := os.Stat(filepath.Join(dir, "s700.shreds"))
	if err != nil {
		t.Fatalf("slot file should exist while buffered: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("premise broken: tail already flushed (size %d)", info.Size())
	}

	first.Close()
	first.Append(700, []byte("after-close"))

	second, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	got, err := second.ReadSlot(700)
	if err != nil || len(got) != 1 || string(got[0]) != "buffered-tail" {
		t.Fatalf("ReadSlot after handoff = %v, %v (want the buffered tail, no post-Close write)", got, err)
	}
	if meta, ok := second.IsComplete(700); !ok || meta.LastIndex != 3 || meta.Shreds != 4 {
		t.Fatalf("completeness must survive handoff (meta=%+v ok=%v)", meta, ok)
	}
}

// The completeness journal survives restarts and compacts away entries for
// deleted slot files — the index that lets a rebooted node know which slots
// need zero network, and the seed of repair-serving retention policy.
func TestShredSpoolCompletenessJournal(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first.Append(700, []byte("full"))
	first.Append(701, []byte("partial"))
	first.MarkComplete(700, 41, 42)
	first.Close()

	second, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	meta, ok := second.IsComplete(700)
	if !ok || meta.LastIndex != 41 || meta.Shreds != 42 {
		t.Fatalf("journal must survive reopen: %+v ok=%v", meta, ok)
	}
	if _, ok := second.IsComplete(701); ok {
		t.Fatalf("unmarked slot must not be complete")
	}
	// Deleting the slot (floor) drops the marker; the compacted journal on
	// the NEXT reopen must not resurrect it.
	second.SetFloor(701)
	if _, ok := second.IsComplete(700); ok {
		t.Fatalf("floor deletion must clear completeness")
	}
	second.Close()
	third, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("third open: %v", err)
	}
	defer third.Close()
	if _, ok := third.IsComplete(700); ok {
		t.Fatalf("stale journal entry must not resurrect a deleted slot")
	}
	if third.CompleteSlots() != 0 {
		t.Fatalf("no complete slots expected, got %d", third.CompleteSlots())
	}
}

// The catchup monitor starts from the run of complete spooled slots at the
// frontier; a gap or an incomplete slot ends the run.
func TestSpoolCompleteThrough(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()
	r := NewUDPReceiver("127.0.0.1:0")
	if _, ok := r.SpoolCompleteThrough(100, 10); ok {
		t.Fatalf("no spool attached must report nothing")
	}
	r.SetShredSpool(spool)
	for _, slot := range []uint64{100, 101, 102, 104} {
		spool.MarkComplete(slot, 9, 10)
	}
	spool.Append(103, []byte("partial")) // spooled but never proven complete

	if last, ok := r.SpoolCompleteThrough(100, 10); !ok || last != 102 {
		t.Fatalf("run from 100 = %d, %v; want 102, true", last, ok)
	}
	if last, ok := r.SpoolCompleteThrough(100, 2); !ok || last != 101 {
		t.Fatalf("limited run from 100 = %d, %v; want 101, true", last, ok)
	}
	if _, ok := r.SpoolCompleteThrough(103, 10); ok {
		t.Fatalf("an incomplete frontier slot must not start a run")
	}
	if last, ok := r.SpoolCompleteThrough(104, 10); !ok || last != 104 {
		t.Fatalf("run from 104 = %d, %v; want 104, true", last, ok)
	}
}

// spoolDataShredPacket is a minimal legacy data shred of slot whose header
// names parent: enough for ParseShred and the spool's parent lookup.
func spoolDataShredPacket(slot, parent uint64, index uint32) []byte {
	packet := make([]byte, dataHeaderSize+8)
	packet[shredVariantOffset] = legacyDataVariant
	binary.LittleEndian.PutUint64(packet[shredSlotOffset:], slot)
	binary.LittleEndian.PutUint32(packet[shredIndexOffset:], index)
	binary.LittleEndian.PutUint16(packet[dataParentOffsetOffset:], uint16(slot-parent))
	binary.LittleEndian.PutUint16(packet[dataSizeOffset:], uint16(len(packet)))
	return packet
}

func spoolCompleteBlock(t *testing.T, spool *ShredSpool, slot, parent uint64) {
	t.Helper()
	spool.Append(slot, spoolDataShredPacket(slot, parent, 0))
	spool.MarkComplete(slot, 0, 1)
}

// Mainnet halt: 919 (the last slot of a leader run) was received complete, the
// cluster skipped it, and 920 links to 918. Catch-up must neither claim 919
// as part of a replayable run nor serve it on restart.
func TestSpoolCatchupSkipsSlotALaterBlockLinksPast(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetShredSpool(spool)
	spoolCompleteBlock(t, spool, 918, 917)
	spoolCompleteBlock(t, spool, 919, 918)
	spoolCompleteBlock(t, spool, 920, 918)

	if got := spool.CompleteParents(918, 920); got[918] != 917 || got[919] != 918 || got[920] != 918 || len(got) != 3 {
		t.Fatalf("parents = %v", got)
	}
	if last, ok := r.SpoolCompleteThrough(918, 10); !ok || last != 918 {
		t.Fatalf("run from 918 = %d, %v; want 918, true (919 is skipped on 920's branch)", last, ok)
	}
	if _, ok := r.SpoolCompleteThrough(919, 10); ok {
		t.Fatalf("a run must not start at a slot a later spooled block links past")
	}
	if last, ok := r.SpoolCompleteThrough(918, 2); !ok || last != 919 {
		t.Fatalf("run from 918 limited to 2 = %d, %v; want 919, true (920 is outside the window)", last, ok)
	}

	if dropped := r.DiscardSpooledSkippedSlots(919, 10); len(dropped) != 1 || dropped[0] != 919 {
		t.Fatalf("discarded %v, want [919]", dropped)
	}
	if spool.HasSlot(919) {
		t.Fatalf("919 must be gone from the spool")
	}
	if _, complete := spool.IsComplete(919); complete {
		t.Fatalf("919 must not stay complete")
	}
	if _, complete := spool.IsComplete(920); !complete {
		t.Fatalf("920 must be kept")
	}
	if dropped := r.DiscardSpooledSkippedSlots(919, 10); len(dropped) != 0 {
		t.Fatalf("second pass discarded %v", dropped)
	}

	// A different block for 919 that 920 builds on is a normal chain again;
	// the cached parent of the discarded block must not survive.
	spoolCompleteBlock(t, spool, 919, 917)
	spool.DiscardSlot(920)
	spoolCompleteBlock(t, spool, 920, 919)
	if got := spool.CompleteParents(919, 920); got[919] != 917 || got[920] != 919 {
		t.Fatalf("parents after replacement = %v", got)
	}
	if last, ok := r.SpoolCompleteThrough(919, 10); !ok || last != 920 {
		t.Fatalf("run from 919 = %d, %v; want 920, true", last, ok)
	}
}

// 921 links past 919..920 while 920 builds on 919: two competing branches.
// Catch-up stops before the contested range and leaves the choice to the live
// drive; the spool keeps both branches whole (dropping only the dead end 920
// would replay 919 and halt the same way).
func TestSpoolCatchupKeepsContestedSlot(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetShredSpool(spool)
	spoolCompleteBlock(t, spool, 918, 917)
	spoolCompleteBlock(t, spool, 919, 918)
	spoolCompleteBlock(t, spool, 920, 919)
	spoolCompleteBlock(t, spool, 921, 918)

	if dropped := r.DiscardSpooledSkippedSlots(918, 10); len(dropped) != 0 {
		t.Fatalf("discarded %v, want none (920 builds on 919, so 919..920 is a contested branch)", dropped)
	}
	if last, ok := r.SpoolCompleteThrough(918, 10); !ok || last != 918 {
		t.Fatalf("run from 918 = %d, %v; want 918, true", last, ok)
	}
	if !spool.HasSlot(919) || !spool.HasSlot(920) || !spool.HasSlot(921) {
		t.Fatalf("both branches must stay spooled")
	}
	// Slots below from are never touched; with 920 rooted, 921 (built on 918)
	// skips the root and is the only one discarded.
	if dropped := r.DiscardSpooledSkippedSlots(921, 10); len(dropped) != 1 || dropped[0] != 921 {
		t.Fatalf("discarded %v, want [921]", dropped)
	}
	if !spool.HasSlot(919) || !spool.HasSlot(920) {
		t.Fatalf("slots below the frontier must stay spooled")
	}
}

// Blocks without a readable data shred (journal entries only) give no parent
// and leave the run as the journal states it.
func TestSpoolCompleteParentsIgnoresUnreadableSlots(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()
	spool.MarkComplete(500, 9, 10)
	spool.Append(501, []byte("not a shred"))
	spool.MarkComplete(501, 9, 10)
	if got := spool.CompleteParents(0, 1000); len(got) != 0 {
		t.Fatalf("parents = %v, want none", got)
	}
}

// A spooled block that skips the rooted slot (from-1) is abandoned: it is
// discarded, including at the frontier, and its link is not evidence against
// the canonical next block.
func TestSpoolCatchupIgnoresBlocksSkippingTheRoot(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetShredSpool(spool)
	// Rooted 918; canonical 919 builds on it; 920 links to 917, skipping the root.
	spoolCompleteBlock(t, spool, 919, 918)
	spoolCompleteBlock(t, spool, 920, 917)
	if dropped := r.DiscardSpooledSkippedSlots(919, 10); len(dropped) != 1 || dropped[0] != 920 {
		t.Fatalf("discarded %v, want [920]", dropped)
	}
	if _, complete := spool.IsComplete(919); !complete {
		t.Fatalf("the canonical 919 must be kept")
	}

	// The frontier block itself skipping the root is discarded too.
	spool2, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool2.Close()
	r2 := NewUDPReceiver("127.0.0.1:0")
	r2.SetShredSpool(spool2)
	spoolCompleteBlock(t, spool2, 919, 916)
	if dropped := r2.DiscardSpooledSkippedSlots(919, 10); len(dropped) != 1 || dropped[0] != 919 {
		t.Fatalf("discarded %v, want [919]", dropped)
	}
}

// A malformed parent offset (zero, or past the slot) gives no parent rather
// than parent 0, which would make the block "link past" every earlier slot.
func TestSpoolParentRejectsMalformedOffsets(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer spool.Close()
	bad := spoolDataShredPacket(50, 0, 0)
	binary.LittleEndian.PutUint16(bad[dataParentOffsetOffset:], 60) // offset beyond the slot
	spool.Append(50, bad)
	spool.MarkComplete(50, 0, 1)
	zero := spoolDataShredPacket(51, 51, 0) // offset 0
	spool.Append(51, zero)
	spool.MarkComplete(51, 0, 1)
	if got := spool.CompleteParents(0, 100); len(got) != 0 {
		t.Fatalf("parents = %v, want none", got)
	}
}
