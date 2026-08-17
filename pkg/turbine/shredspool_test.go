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

func TestShredSpoolIndexesCanonicalDataShreds(t *testing.T) {
	dir := t.TempDir()
	shreds := generatedAlpenglowDataShreds(t)
	if len(shreds) < 2 {
		t.Fatalf("generated %d data shreds, want at least two", len(shreds))
	}
	first := shreds[0]
	last := shreds[len(shreds)-1]

	spool, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i, shred := range shreds {
		packet := append([]byte(nil), shred.Payload...)
		if i == 0 {
			// A repair response appends its nonce to the canonical shred.
			packet = binary.LittleEndian.AppendUint32(packet, 0xdecafbad)
		}
		if !spool.AppendShred(shred, packet) {
			t.Fatalf("AppendShred(%d) returned false", shred.Index)
		}
	}

	got, ok, err := spool.GetDataShred(first.Slot, uint64(first.Index))
	if err != nil || !ok {
		t.Fatalf("GetDataShred = ok %v, err %v", ok, err)
	}
	if string(got) != string(first.Payload) {
		t.Fatalf("exact lookup retained a repair nonce: got %d bytes, want %d", len(got), len(first.Payload))
	}
	got, ok, err = spool.GetHighestDataShredFrom(first.Slot, 0)
	if err != nil || !ok || string(got) != string(last.Payload) {
		t.Fatalf("GetHighestDataShredFrom = %d bytes, ok %v, err %v; want index %d", len(got), ok, err, last.Index)
	}
	if _, ok, err := spool.GetDataShred(first.Slot, uint64(last.Index)+1); err != nil || ok {
		t.Fatalf("missing exact lookup = ok %v, err %v", ok, err)
	}
	spool.Close()

	// A fresh process has no in-memory index; the first request reconstructs it
	// from checksummed slot records.
	reopened, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, ok, err = reopened.GetDataShred(first.Slot, uint64(first.Index))
	if err != nil || !ok || string(got) != string(first.Payload) {
		t.Fatalf("adopted exact lookup = %d bytes, ok %v, err %v", len(got), ok, err)
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
