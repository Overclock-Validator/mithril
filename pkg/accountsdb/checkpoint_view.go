package accountsdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
)

// A CheckpointView reads accounts as they were at an earlier fold boundary,
// while the store keeps folding forward. It changes nothing on disk.
//
// Every fold manifest records, for each account the fold wrote, where that
// account's previous version is. Walking the manifests after a boundary back
// to it therefore names, for every account changed since, the version it had
// at the boundary: the same pointers a rewind would write back into the index.
// The view keeps those pointers in memory. An account that no later fold
// touched is unchanged, and is read through the index as it stands.
//
// A view is safe for concurrent use. It stays valid as later folds commit: a
// read first folds in the manifests committed since the view last looked. It
// stops being valid once compaction lets the boundary expire, and then every
// read returns ErrCheckpointExpired.
type CheckpointView struct {
	db          *AccountsDb
	throughSlot uint64
	batchSeq    uint64
	resumeCtx   []byte
	bankhashes  []SlotBankhash

	mu sync.Mutex
	// headSeq is the newest fold whose manifest is reflected in changed.
	headSeq uint64
	// changed holds, for each account written by a fold in (batchSeq, headSeq],
	// its version at the boundary. present is false when it did not exist.
	changed map[solana.PublicKey]checkpointVersion
}

type checkpointVersion struct {
	entry   AccountIndexEntry
	present bool
}

// ErrCheckpointExpired means the boundary a view was opened at is no longer
// within the retained rewind horizon.
var ErrCheckpointExpired = errors.New("accountsdb: checkpoint is outside the retained rewind horizon")

// CheckpointLoadStats describes one LoadAccounts call.
type CheckpointLoadStats struct {
	Requested uint64
	// Changed counts keys whose boundary version came from a fold record,
	// with no index lookup. Unchanged counts keys read through the index.
	Changed, Unchanged uint64
	// Absent counts keys with no account at the boundary.
	Absent uint64
	// Files is the number of data files opened; DataBytes the account data read.
	Files     uint64
	DataBytes uint64
}

// OpenCheckpointView opens a view at the fold boundary whose last slot is
// throughSlot. ListRewindPoints names the available boundaries.
func (db *AccountsDb) OpenCheckpointView(throughSlot uint64) (*CheckpointView, error) {
	headers, err := db.foldManifests.headers(db.AcctsDir)
	if err != nil {
		return nil, err
	}
	var target *ManifestHeader
	for seq := range headers {
		if header := headers[seq]; header.ThroughSlot == throughSlot {
			target = &header
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("%w: no fold boundary at slot %d", ErrCheckpointExpired, throughSlot)
	}
	manifest, err := ReadSegmentManifest(target.Path)
	if errors.Is(err, os.ErrNotExist) {
		// Compaction removed it since the directory was listed.
		db.foldManifests.forget()
		return nil, fmt.Errorf("%w: no fold boundary at slot %d", ErrCheckpointExpired, throughSlot)
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint manifest at slot %d: %w", throughSlot, err)
	}
	view := &CheckpointView{
		db: db, throughSlot: throughSlot, batchSeq: manifest.BatchSeq, headSeq: manifest.BatchSeq,
		resumeCtx: manifest.ResumeCtx, bankhashes: manifest.Bankhashes,
		changed: make(map[solana.PublicKey]checkpointVersion),
	}
	// Establish validity and the first set of pointers now, so an unusable
	// boundary is reported at open and not at the first read.
	snapshot := db.Index.NewSnapshot()
	defer snapshot.Close()
	view.mu.Lock()
	defer view.mu.Unlock()
	if err := view.refreshLocked(snapshot); err != nil {
		return nil, err
	}
	return view, nil
}

// ThroughSlot is the last slot folded at the view's boundary.
func (view *CheckpointView) ThroughSlot() uint64 { return view.throughSlot }

// ResumeContext is the serialized replay state at the boundary, as the fold
// that ended there recorded it. Callers must not modify it.
func (view *CheckpointView) ResumeContext() []byte { return view.resumeCtx }

// refreshLocked brings changed up to the fold head visible in snapshot.
// Caller holds view.mu. Manifests are applied oldest first, and a key keeps
// the first pointer recorded for it: the earliest fold after the boundary to
// write an account is the one that replaced its boundary version.
func (view *CheckpointView) refreshLocked(snapshot *pebble.Snapshot) error {
	head, haveHead, err := readFoldMetaFrom(snapshot)
	if err != nil {
		return err
	}
	floor, err := readCompactionHistoryFloorFrom(snapshot)
	if err != nil {
		return err
	}
	if !haveHead || head.BatchSeq < view.batchSeq || floor.BatchSeq > view.batchSeq {
		// The head below the boundary means the store was rewound past it.
		return ErrCheckpointExpired
	}
	if head.BatchSeq == view.headSeq {
		return nil
	}
	headers, err := view.db.foldManifests.headers(view.db.AcctsDir)
	if err != nil {
		return err
	}
	for seq := view.headSeq + 1; seq <= head.BatchSeq; seq++ {
		header, ok := headers[seq]
		if !ok {
			// The remembered directory may predate this fold; list it afresh once.
			view.db.foldManifests.forget()
			if headers, err = view.db.foldManifests.headers(view.db.AcctsDir); err != nil {
				return err
			}
			if header, ok = headers[seq]; !ok {
				return fmt.Errorf("%w: fold manifest %d is missing", ErrCheckpointExpired, seq)
			}
		}
		manifest, err := ReadSegmentManifest(header.Path)
		if err != nil {
			return fmt.Errorf("read fold manifest %d: %w", seq, err)
		}
		if manifest.Kind != ManifestKindFold || manifest.BatchSeq != seq {
			return fmt.Errorf("fold manifest %s does not describe batch %d", header.Path, seq)
		}
		for i := range manifest.Records {
			record := &manifest.Records[i]
			key := solana.PublicKey(record.Pubkey)
			if _, seen := view.changed[key]; !seen {
				view.changed[key] = checkpointVersion{entry: record.Prev, present: record.PrevValid}
			}
		}
		view.headSeq = seq
	}
	return nil
}

// GetAccount returns the account as of the boundary, or ErrNoAccount.
func (view *CheckpointView) GetAccount(key solana.PublicKey) (*accounts.Account, error) {
	loaded, _, err := view.LoadAccounts(context.Background(), []solana.PublicKey{key})
	if err != nil {
		return nil, err
	}
	if loaded[0] == nil {
		return nil, ErrNoAccount
	}
	return loaded[0], nil
}

type checkpointRead struct {
	index int // position in the caller's key list
	entry AccountIndexEntry
}

// LoadAccounts returns every key's account as of the boundary, in the order
// given, with nil where no account existed. It is meant to be called once for
// everything a replay of the following fold will touch: locations are resolved
// first (from fold records where the account has changed since, and only
// otherwise through the index), then read file by file in offset order.
//
// The returned accounts are owned by the caller.
func (view *CheckpointView) LoadAccounts(ctx context.Context, keys []solana.PublicKey) ([]*accounts.Account, CheckpointLoadStats, error) {
	stats := CheckpointLoadStats{Requested: uint64(len(keys))}
	out := make([]*accounts.Account, len(keys))
	if len(keys) == 0 {
		return out, stats, nil
	}
	db := view.db
	// Compaction unlinks data files under the write side of this lock.
	db.appendVecReadMu.RLock()
	defer db.appendVecReadMu.RUnlock()
	// One index snapshot covers the fold head, the expiry check and every
	// unchanged key's lookup, so a fold committing meanwhile cannot be seen
	// in the index without also being seen as changed.
	snapshot := db.Index.NewSnapshot()
	defer snapshot.Close()

	reads := make([]checkpointRead, 0, len(keys))
	view.mu.Lock()
	if err := view.refreshLocked(snapshot); err != nil {
		view.mu.Unlock()
		return nil, stats, err
	}
	for i, key := range keys {
		if version, ok := view.changed[key]; ok {
			stats.Changed++
			if version.present {
				reads = append(reads, checkpointRead{index: i, entry: version.entry})
			} else {
				stats.Absent++
			}
			continue
		}
		reads = append(reads, checkpointRead{index: i, entry: AccountIndexEntry{FileId: ^uint64(0)}})
	}
	view.mu.Unlock()

	for i := range reads {
		if reads[i].entry.FileId != ^uint64(0) {
			continue
		}
		stats.Unchanged++
		key := keys[reads[i].index]
		value, closer, err := snapshot.Get(key[:])
		if errors.Is(err, pebble.ErrNotFound) {
			stats.Absent++
			reads[i].index = -1
			continue
		}
		if err != nil {
			return nil, stats, fmt.Errorf("index get %s: %w", key, err)
		}
		entry, decodeErr := UnmarshalAcctIdxEntryValue(value)
		closer.Close()
		if decodeErr != nil {
			return nil, stats, fmt.Errorf("index entry %s: %w", key, decodeErr)
		}
		reads[i].entry = entry
	}

	// Disk order: one pass over each file, front to back.
	sort.Slice(reads, func(a, b int) bool {
		x, y := reads[a].entry, reads[b].entry
		if x.Slot != y.Slot {
			return x.Slot < y.Slot
		}
		if x.FileId != y.FileId {
			return x.FileId < y.FileId
		}
		return x.Offset < y.Offset
	})
	var file *os.File
	var fileSlot, fileID uint64
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	for n, read := range reads {
		if read.index < 0 {
			continue
		}
		if n&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, stats, err
			}
		}
		if file == nil || read.entry.Slot != fileSlot || read.entry.FileId != fileID {
			if file != nil {
				file.Close()
				file = nil
			}
			path := db.appendVecPath(read.entry.Slot, read.entry.FileId)
			opened, err := os.Open(path)
			if err != nil {
				return nil, stats, fmt.Errorf("open checkpoint data %s: %w", path, err)
			}
			file, fileSlot, fileID = opened, read.entry.Slot, read.entry.FileId
			stats.Files++
		}
		key := keys[read.index]
		var record AppendVecAccount
		section := io.NewSectionReader(file, int64(read.entry.Offset), 1<<62)
		if err := record.unmarshal(section, ^uint64(0)); err != nil {
			return nil, stats, fmt.Errorf("read %s at %s@%d: %w", key, file.Name(), read.entry.Offset, err)
		}
		account := record.ToAccount()
		if account.Key != key {
			return nil, stats, fmt.Errorf("record at %s@%d holds %s, not %s", file.Name(), read.entry.Offset, account.Key, key)
		}
		account.Slot = read.entry.Slot
		stats.DataBytes += uint64(len(account.Data))
		out[read.index] = account
	}
	return out, stats, nil
}

// foldManifestDirectory remembers where each fold's manifest is, so finding
// one does not mean listing the accounts directory, which holds every data
// file in the store. It is filled by one listing on first use, kept current
// as folds commit, and dropped whenever a rewind, a recovery or compaction
// changes the directory outside those commits.
type foldManifestDirectory struct {
	mu    sync.Mutex
	bySeq map[uint64]ManifestHeader // nil until listed
}

// headers returns the manifests by batch sequence. The map is a copy.
func (dir *foldManifestDirectory) headers(acctsDir string) (map[uint64]ManifestHeader, error) {
	dir.mu.Lock()
	defer dir.mu.Unlock()
	if dir.bySeq == nil {
		listed, err := ListFoldManifests(acctsDir)
		if err != nil {
			return nil, err
		}
		dir.bySeq = make(map[uint64]ManifestHeader, len(listed))
		for _, header := range listed {
			if header.BatchSeq != 0 {
				dir.bySeq[header.BatchSeq] = header
			}
		}
	}
	out := make(map[uint64]ManifestHeader, len(dir.bySeq))
	for seq, header := range dir.bySeq {
		out[seq] = header
	}
	return out, nil
}

func (dir *foldManifestDirectory) note(header ManifestHeader) {
	dir.mu.Lock()
	if dir.bySeq != nil {
		dir.bySeq[header.BatchSeq] = header
	}
	dir.mu.Unlock()
}

func (dir *foldManifestDirectory) forget() {
	dir.mu.Lock()
	dir.bySeq = nil
	dir.mu.Unlock()
}
