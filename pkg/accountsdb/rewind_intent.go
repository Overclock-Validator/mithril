package accountsdb

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const rewindIntentName = "rewind-intent.json"

// An intent is local to this store and pins both the exact pre-rewind meta and
// the target manifest identity. It survives forensic cleanup, unlike parked
// manifests. No forward replay is allowed until the caller's state is durable.
type rewindIntent struct {
	Version     int
	From        foldMeta
	Target      foldMeta
	ManifestSHA [32]byte
}
type rewindIntentFile struct {
	Intent rewindIntent
	SHA    [32]byte
}
type rewindTestHooks struct {
	beforeIndexFlush func()
	afterIntent      func()
	afterPark        func()
	afterIndex       func()
	afterState       func()
}

func (db *AccountsDb) readRewindIntentLocked() (*rewindIntent, error) {
	data, err := os.ReadFile(filepath.Join(db.AcctsDir, rewindIntentName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var disk rewindIntentFile
	if err := json.Unmarshal(data, &disk); err != nil {
		return nil, fmt.Errorf("rewind intent: %w", err)
	}
	encoded, err := json.Marshal(disk.Intent)
	if err != nil {
		return nil, err
	}
	in := &disk.Intent
	if sha256.Sum256(encoded) != disk.SHA || in.Version != 1 || in.Target.BatchSeq == 0 || in.From.BatchSeq < in.Target.BatchSeq || in.From.ThroughSlot < in.Target.ThroughSlot {
		return nil, fmt.Errorf("invalid rewind intent checksum/version/boundaries")
	}
	meta, have, err := db.readFoldMeta()
	if err != nil {
		return nil, err
	}
	if !have || (meta != in.From && meta != in.Target) {
		return nil, fmt.Errorf("rewind intent does not match committed store frontier")
	}
	path := segmentManifestPath(db.AcctsDir, in.Target.ThroughSlot, in.Target.FileId)
	data, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rewind intent target: %w", err)
	}
	if sha256.Sum256(data) != in.ManifestSHA {
		return nil, fmt.Errorf("rewind intent target manifest identity mismatch")
	}
	m, err := ReadSegmentManifest(path)
	if err != nil {
		return nil, err
	}
	if m.Kind != ManifestKindFold || (foldMeta{BatchSeq: m.BatchSeq, ThroughSlot: m.ThroughSlot, FileId: m.FileId}) != in.Target {
		return nil, fmt.Errorf("rewind intent target manifest boundary mismatch")
	}
	return in, nil
}

func (db *AccountsDb) beginRewindIntentLocked(target uint64) error {
	in, err := db.readRewindIntentLocked()
	if err != nil {
		return err
	}
	if in != nil {
		if in.Target.ThroughSlot != target {
			return fmt.Errorf("pending rewind to %d must finish before rewind to %d", in.Target.ThroughSlot, target)
		}
		// Also retry directory durability if an earlier rename succeeded but Sync failed.
		return fsyncDir(db.AcctsDir)
	}
	// Pin the source index frontier before parking its redo manifests. Without
	// this flush, a WAL-less crash could lose both the source memtable and the
	// ability of ordinary recovery to replay the now-parked suffix.
	if db.IndexWALDisabled {
		if err := db.Index.Flush(); err != nil {
			return err
		}
	}
	meta, have, err := db.readFoldMeta()
	if err != nil {
		return err
	}
	if !have {
		return fmt.Errorf("rewind intent: no committed fold frontier")
	}
	headers, err := ListFoldManifests(db.AcctsDir)
	if err != nil {
		return err
	}
	for _, h := range headers {
		if h.ThroughSlot != target || h.BatchSeq > meta.BatchSeq {
			continue
		}
		m, err := ReadSegmentManifest(h.Path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(h.Path)
		if err != nil {
			return err
		}
		in = &rewindIntent{Version: 1, From: meta, Target: foldMeta{BatchSeq: m.BatchSeq, ThroughSlot: m.ThroughSlot, FileId: m.FileId}, ManifestSHA: sha256.Sum256(data)}
		break
	}
	if in == nil {
		return fmt.Errorf("rewind intent: no retained target boundary %d", target)
	}
	floor, err := db.readCompactionHistoryFloor()
	if err != nil {
		return err
	}
	if in.Target.BatchSeq < floor.BatchSeq {
		return fmt.Errorf("rewind target %d is below expired-history boundary %d", in.Target.BatchSeq, floor.BatchSeq)
	}
	encoded, err := json.Marshal(in)
	if err != nil {
		return err
	}
	data, err := json.Marshal(rewindIntentFile{Intent: *in, SHA: sha256.Sum256(encoded)})
	if err != nil {
		return err
	}
	path := filepath.Join(db.AcctsDir, rewindIntentName)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return fsyncDir(db.AcctsDir)
}

// PendingRewindTarget returns an exact, validated intent target (zero if none).
// Legacy parked manifests without an intent are handled separately by startup.
func (db *AccountsDb) PendingRewindTarget() (uint64, error) {
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	in, err := db.readRewindIntentLocked()
	if err != nil || in == nil {
		return 0, err
	}
	return in.Target.ThroughSlot, nil
}

// RewindToBatchBoundaryAndPersist bridges the AccountsDB rollback and its
// caller's durable resume state. The store must be quiesced. persist must
// validate and durably save the returned checkpoint before returning nil; it
// must not reenter AccountsDB. On any error callers must halt (never fold ahead).
// The intent is cleared only after persist succeeds, including on a retry after
// index rollback/forensic cleanup. Ordinary recovery defers to this intent.
func (db *AccountsDb) RewindToBatchBoundaryAndPersist(target uint64, persist func(RewindResult) error) (RewindResult, error) {
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	db.appendVecReadMu.Lock()
	defer db.appendVecReadMu.Unlock()
	if persist == nil {
		return RewindResult{}, fmt.Errorf("rewind requires durable state callback")
	}
	db.rewindPending = true
	if err := db.beginRewindIntentLocked(target); err != nil {
		return RewindResult{}, err
	}
	fire(db.rewindHooks.afterIntent)
	res, err := db.rewindToBatchBoundaryLocked(target)
	if err != nil {
		return res, err
	}
	if err := persist(res); err != nil {
		return res, err
	}
	fire(db.rewindHooks.afterState)
	if err := os.Remove(filepath.Join(db.AcctsDir, rewindIntentName)); err != nil {
		return res, err
	}
	if err := fsyncDir(db.AcctsDir); err != nil {
		return res, err
	}
	db.rewindPending = false
	return res, nil
}
