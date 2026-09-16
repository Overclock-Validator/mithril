package accountsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"
)

// Packed snapshot appendvecs may have unaligned starts, no final padding and
// O_DIRECT gaps. The canonical index, not a sequential parser, identifies live
// records. A bounded assessment pass measures live bytes; a second pass moves
// them in durable batches. Only then may the source be unlinked.
//
// The cursor is deliberately volatile. Restart/error simply repeats assessment
// of the remaining references. After each batch, either the old file is still
// authoritative or the new data+manifest+index are durable; both files coexist
// until evacuation finishes. Interrupted work therefore needs no new redo log.
// Rewind pins are checked every cycle, and invalidate any in-progress scan.
type coalescedScan struct {
	cursor    []byte
	liveBytes int64
	moving    bool
}

type coalescedCompactResult struct {
	scanned, moved, freed int64
	deleted               bool
}

const maxCoalescedBatchRecords = 8192 // bounds manifest and Pebble batch memory

// Coalesced compaction outputs keep each account's index Slot unchanged.
// Their physical filename is 0.<id>; the manifest identifies that addressing
// mode. Ordinary fold/compact segments retain their existing addressing.
func (db *AccountsDb) loadCoalescedFiles() error {
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segManifestSuffix) {
			continue
		}
		path := filepath.Join(db.AcctsDir, e.Name())
		hdr, err := readManifestHeader(path)
		if err != nil || hdr.Kind != ManifestKindCoalescedCompact {
			continue
		}
		m, err := readCoalescedManifest(path)
		if err != nil {
			return err
		}
		if m.ThroughSlot != 0 || e.Name() != SegmentDataName(0, m.FileId)+segManifestSuffix {
			return fmt.Errorf("invalid coalesced compact manifest %s", path)
		}
		db.coalescedFiles.Store(m.FileId, filepath.Join(db.AcctsDir, SegmentDataName(0, m.FileId)))
	}
	return nil
}

func (db *AccountsDb) compactCoalesced(c compactCandidate, scanBudget, moveBudget int64, minDead float64, allowOversizedRecord bool) (res coalescedCompactResult, err error) {
	if db.coalescedScans == nil {
		db.coalescedScans = make(map[uint64]*coalescedScan)
	}
	s := db.coalescedScans[c.fileId]
	if s == nil {
		s = &coalescedScan{}
		db.coalescedScans[c.fileId] = s
	}
	defer func() {
		if err != nil {
			delete(db.coalescedScans, c.fileId)
		}
	}()
	src, err := os.Open(c.path)
	if err != nil {
		return res, err
	}
	defer src.Close()
	var opts *pebble.IterOptions
	if c.manifestPath != "" {
		m, e := readCoalescedManifest(c.manifestPath)
		if e != nil {
			return res, e
		}
		// Each output contains a consecutive, sorted range of keys from the
		// source scan. Restrict future scans to that range, instead of scanning
		// the full account index once per small replacement segment.
		if len(m.Records) > 0 {
			opts = &pebble.IterOptions{LowerBound: m.Records[0].Pubkey[:], UpperBound: append(m.Records[len(m.Records)-1].Pubkey[:], 0)}
		}
	}
	it, err := db.Index.NewIter(opts)
	if err != nil {
		return res, err
	}
	defer it.Close()
	valid := it.First()
	if s.cursor != nil {
		valid = it.SeekGE(s.cursor)
		if valid && bytes.Equal(it.Key(), s.cursor) {
			valid = it.Next()
		}
	}
	var out *coalescedOutput
	defer func() {
		if out != nil {
			out.file.Close()
		}
	}()
	var header [hdrLen]byte
	copyBuf := make([]byte, 64<<10)
	for valid {
		cost := int64(len(it.Key()) + len(it.Value()))
		if res.scanned > 0 && res.scanned+cost > scanBudget {
			break
		}
		res.scanned += cost
		key := it.Key()
		if len(key) == 32 {
			entry, e := UnmarshalAcctIdxEntryValue(it.Value())
			if e != nil {
				return res, e
			}
			if entry.FileId == c.fileId {
				if res.scanned > cost && res.scanned+hdrLen > scanBudget {
					break
				}
				if entry.Offset > uint64(c.size) || uint64(c.size)-entry.Offset < hdrLen {
					return res, fmt.Errorf("indexed record outside %s at %d", c.path, entry.Offset)
				}
				if _, e = src.ReadAt(header[:], int64(entry.Offset)); e != nil {
					return res, e
				}
				res.scanned += hdrLen
				if !bytes.Equal(header[pubkeyOffset:pubkeyOffset+32], key) {
					return res, fmt.Errorf("indexed pubkey mismatch in %s at %d", c.path, entry.Offset)
				}
				dataLen := binary.LittleEndian.Uint64(header[dataLenOffset : dataLenOffset+8])
				if dataLen > uint64(c.size)-entry.Offset-hdrLen {
					return res, fmt.Errorf("truncated indexed record in %s at %d", c.path, entry.Offset)
				}
				if !s.moving {
					s.liveBytes += int64(hdrLen) + int64(dataLen)
				} else {
					padded := int64(hdrLen) + int64((dataLen+7)&^uint64(7))
					// Budgets stop between records. Like legacy compaction, one indivisible
					// record may exceed a very small configured budget; it never requires
					// an allocation proportional to account data size.
					if (res.moved+padded > moveBudget && (out != nil || !allowOversizedRecord)) || (out != nil && (res.scanned+int64(dataLen) > scanBudget || len(out.records) >= maxCoalescedBatchRecords)) {
						break
					}
					if out == nil {
						out, e = db.newCoalescedOutput()
						if e != nil {
							return res, e
						}
					}
					var pk [32]byte
					copy(pk[:], key)
					rec := ManifestRecord{Pubkey: pk, OwnerSlot: entry.Slot, Offset: uint64(res.moved)}
					if _, e = out.writer.Write(header[:]); e != nil {
						return res, e
					}
					if _, e = io.CopyBuffer(out.writer, io.NewSectionReader(src, int64(entry.Offset)+hdrLen, int64(dataLen)), copyBuf); e != nil {
						return res, e
					}
					var padding [7]byte
					if _, e = out.writer.Write(padding[:(8-dataLen%8)%8]); e != nil {
						return res, e
					}
					out.records = append(out.records, rec)
					res.moved += padded
					res.scanned += int64(dataLen)
				}
			}
		}
		s.cursor = append(s.cursor[:0], key...)
		valid = it.Next()
		if res.scanned >= scanBudget || (s.moving && res.moved >= moveBudget) {
			break
		}
	}
	if e := it.Error(); e != nil {
		return res, e
	}
	exhausted := !valid
	if out != nil {
		if e := db.commitCoalescedOutput(out, res.moved); e != nil {
			return res, e
		}
	}
	if !exhausted {
		return res, nil
	}
	if !s.moving && s.liveBytes > 0 {
		if 1-float64(s.liveBytes)/float64(c.size) < minDead {
			delete(db.coalescedScans, c.fileId)
			return res, nil
		}
		s.moving = true
		s.cursor = nil
		return res, nil
	}
	// No live references were found, or every remaining reference was moved.
	// A fold cannot reintroduce references to this immutable source. An allowed
	// rewind that could do so would pin the source and reset the scan next cycle.
	db.appendVecReadMu.Lock()
	defer db.appendVecReadMu.Unlock()
	// Also covers a previous ambiguous Commit/Sync error: never unlink using
	// an in-memory index view whose durability has not been re-established.
	if e := db.Index.Flush(); e != nil {
		return res, e
	}
	if e := db.coalescedCompactStage("before-unlink"); e != nil {
		return res, e
	}
	if e := removeSourceFiles(filepath.Dir(c.path), c.path, c.manifestPath); e != nil {
		return res, e
	}
	db.coalescedFiles.Delete(c.fileId)
	delete(db.coalescedScans, c.fileId)
	res.deleted = true
	res.freed = c.size
	return res, nil
}

type coalescedOutput struct {
	file    *os.File
	writer  io.Writer
	crc     hash.Hash32
	id      uint64
	records []ManifestRecord
}

func (db *AccountsDb) newCoalescedOutput() (*coalescedOutput, error) {
	id := db.nextFileId(0)
	if err := db.persistLargestFileId(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(db.AcctsDir, SegmentDataName(0, id)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	crc := crc32.NewIEEE()
	return &coalescedOutput{file: f, writer: io.MultiWriter(f, crc), crc: crc, id: id}, nil
}

func (db *AccountsDb) commitCoalescedOutput(out *coalescedOutput, size int64) error {
	if err := out.file.Sync(); err != nil {
		return err
	}
	if err := fsyncDir(db.AcctsDir); err != nil {
		return err
	}
	if err := db.coalescedCompactStage("after-data"); err != nil {
		return err
	}
	m := &SegmentManifest{Version: segManifestVersion, Kind: ManifestKindCoalescedCompact, FileId: out.id, DataLen: uint64(size), DataCRC: out.crc.Sum32(), Records: out.records}
	if err := WriteSegmentManifest(db.AcctsDir, m); err != nil {
		return err
	}
	if err := db.coalescedCompactStage("after-manifest"); err != nil {
		return err
	}
	db.appendVecReadMu.Lock()
	defer db.appendVecReadMu.Unlock()
	db.coalescedFiles.Store(out.id, out.file.Name())
	batch := db.Index.NewBatch()
	defer batch.Close()
	var b [24]byte
	for _, r := range out.records {
		(&AccountIndexEntry{Slot: r.OwnerSlot, FileId: out.id, Offset: r.Offset}).Marshal(&b)
		if err := batch.Set(r.Pubkey[:], b[:], nil); err != nil {
			return err
		}
	}
	opts := pebble.Sync
	if db.IndexWALDisabled {
		opts = pebble.NoSync
	}
	if err := batch.Commit(opts); err != nil {
		return err
	}
	if db.IndexWALDisabled {
		if err := db.Index.Flush(); err != nil {
			return err
		}
	}
	return db.coalescedCompactStage("after-index")
}

func (db *AccountsDb) coalescedCompactStage(stage string) error {
	if db.coalescedCompactHook != nil {
		return db.coalescedCompactHook(stage)
	}
	return nil
}

// This manifest kind is written only by the bounded coalesced compactor.
func readCoalescedManifest(path string) (*SegmentManifest, error) {
	m, err := ReadSegmentManifest(path)
	if err != nil {
		return nil, err
	}
	if m.Kind != ManifestKindCoalescedCompact || m.ThroughSlot != 0 || len(m.Records) == 0 || len(m.Records) > maxCoalescedBatchRecords {
		return nil, fmt.Errorf("invalid coalesced compact manifest %s", path)
	}
	for i := 1; i < len(m.Records); i++ {
		if bytes.Compare(m.Records[i-1].Pubkey[:], m.Records[i].Pubkey[:]) >= 0 {
			return nil, fmt.Errorf("unsorted coalesced compact manifest %s", path)
		}
	}
	return m, nil
}
