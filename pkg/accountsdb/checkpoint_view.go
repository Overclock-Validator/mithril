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

// checkpointLoadWorkers is how many files LoadAccounts reads at once.
const checkpointLoadWorkers = 4

// LoadAccounts returns every key's account as of the boundary, in the order
// given, with nil where no account existed. It is meant to be called once for
// everything a replay of the following fold will touch: locations are resolved
// first (from fold records where the account has changed since, and only
// otherwise through the index), then read file by file in offset order, a few
// files at a time.
//
// The returned accounts are owned by the caller.
func (view *CheckpointView) LoadAccounts(ctx context.Context, keys []solana.PublicKey) ([]*accounts.Account, CheckpointLoadStats, error) {
	return view.LoadAccountsWith(ctx, keys, checkpointLoadWorkers)
}

// PendingCheckpointLoad is the second stage of LoadAccountsStaged.
type PendingCheckpointLoad struct {
	done     chan struct{}
	accounts []*accounts.Account
	stats    CheckpointLoadStats
	err      error
}

// Wait blocks until the stage has loaded and returns it, like LoadAccounts.
func (pending *PendingCheckpointLoad) Wait() ([]*accounts.Account, CheckpointLoadStats, error) {
	<-pending.done
	return pending.accounts, pending.stats, pending.err
}

// LoadAccountsStaged loads first and returns it, with the rest loading behind
// it. A replay can then begin on the first few blocks' accounts while the
// remainder of the fold's accounts arrive, instead of waiting for all of them.
// Both stages describe the same boundary. Cancelling ctx stops the second.
func (view *CheckpointView) LoadAccountsStaged(ctx context.Context, first, rest []solana.PublicKey, workers int) ([]*accounts.Account, CheckpointLoadStats, *PendingCheckpointLoad, error) {
	loaded, stats, err := view.LoadAccountsWith(ctx, first, workers)
	if err != nil {
		return nil, stats, nil, err
	}
	pending := &PendingCheckpointLoad{done: make(chan struct{})}
	go func() {
		defer close(pending.done)
		pending.accounts, pending.stats, pending.err = view.LoadAccountsWith(ctx, rest, workers)
	}()
	return loaded, stats, pending, nil
}

// LoadAccountsWith is LoadAccounts reading up to workers files at once.
func (view *CheckpointView) LoadAccountsWith(ctx context.Context, keys []solana.PublicKey, workers int) ([]*accounts.Account, CheckpointLoadStats, error) {
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

	// Drop keys with no account, then put the rest in disk order: one pass
	// over each file, front to back.
	present := reads[:0]
	for _, read := range reads {
		if read.index >= 0 {
			present = append(present, read)
		}
	}
	reads = present
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
	// Each worker takes a contiguous run of the ordered reads, cut where the
	// file changes, so no file is opened by two workers.
	workers = max(1, min(workers, len(reads)))
	parts := make([][]checkpointRead, 0, workers)
	for start := 0; start < len(reads); {
		end := min(len(reads), start+(len(reads)-start)/(workers-len(parts)))
		end = max(end, start+1)
		for end < len(reads) && len(parts) < workers-1 && reads[end].entry.Slot == reads[end-1].entry.Slot && reads[end].entry.FileId == reads[end-1].entry.FileId {
			end++
		}
		if len(parts) == workers-1 {
			end = len(reads)
		}
		parts = append(parts, reads[start:end])
		start = end
	}
	type partResult struct {
		files, dataBytes uint64
		err              error
	}
	results := make([]partResult, len(parts))
	var wait sync.WaitGroup
	for n, part := range parts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results[n].files, results[n].dataBytes, results[n].err = view.readCheckpointPart(ctx, keys, part, out)
		}()
	}
	wait.Wait()
	for _, result := range results {
		stats.Files += result.files
		stats.DataBytes += result.dataBytes
		if result.err != nil {
			return nil, stats, result.err
		}
	}
	return out, stats, nil
}

// readCheckpointPart reads one ordered run of records into out. Each record
// has its own position in out, so parts never write the same element.
func (view *CheckpointView) readCheckpointPart(ctx context.Context, keys []solana.PublicKey, reads []checkpointRead, out []*accounts.Account) (files, dataBytes uint64, err error) {
	var file *os.File
	var fileSlot, fileID uint64
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	for n, read := range reads {
		if n&255 == 0 {
			if err := ctx.Err(); err != nil {
				return files, dataBytes, err
			}
		}
		if file == nil || read.entry.Slot != fileSlot || read.entry.FileId != fileID {
			if file != nil {
				file.Close()
				file = nil
			}
			path := view.db.appendVecPath(read.entry.Slot, read.entry.FileId)
			opened, err := os.Open(path)
			if err != nil {
				return files, dataBytes, fmt.Errorf("open checkpoint data %s: %w", path, err)
			}
			file, fileSlot, fileID = opened, read.entry.Slot, read.entry.FileId
			files++
		}
		key := keys[read.index]
		var record AppendVecAccount
		section := io.NewSectionReader(file, int64(read.entry.Offset), 1<<62)
		if err := record.unmarshal(section, ^uint64(0)); err != nil {
			return files, dataBytes, fmt.Errorf("read %s at %s@%d: %w", key, file.Name(), read.entry.Offset, err)
		}
		account := record.ToAccount()
		if account.Key != key {
			return files, dataBytes, fmt.Errorf("record at %s@%d holds %s, not %s", file.Name(), read.entry.Offset, account.Key, key)
		}
		account.Slot = read.entry.Slot
		dataBytes += uint64(len(account.Data))
		out[read.index] = account
	}
	return files, dataBytes, nil
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

// CheckpointBoundaries returns the fold boundaries a view can be opened at,
// oldest first, without waiting on a fold in progress. A boundary may still
// expire before it is used.
func (db *AccountsDb) CheckpointBoundaries() ([]uint64, error) {
	snapshot := db.Index.NewSnapshot()
	defer snapshot.Close()
	head, haveHead, err := readFoldMetaFrom(snapshot)
	if err != nil || !haveHead {
		return nil, err
	}
	floor, err := readCompactionHistoryFloorFrom(snapshot)
	if err != nil {
		return nil, err
	}
	headers, err := db.foldManifests.headers(db.AcctsDir)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for seq, header := range headers {
		if seq >= floor.BatchSeq && seq <= head.BatchSeq {
			out = append(out, header.ThroughSlot)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out, nil
}

// FoldRecord describes the fold that follows a view's boundary.
type FoldRecord struct {
	// The fold covers slots (FromSlot, ThroughSlot].
	FromSlot, ThroughSlot uint64
	// Bankhashes holds the bank hash of every slot the fold contains, ascending.
	Bankhashes []SlotBankhash
	// Keys are the accounts the fold wrote: the accounts replaying it changes.
	Keys []solana.PublicKey
}

// FoldAfter returns the record of the fold that follows the view's boundary,
// or nil when the boundary is the head.
func (view *CheckpointView) FoldAfter() (*FoldRecord, error) {
	headers, err := view.db.foldManifests.headers(view.db.AcctsDir)
	if err != nil {
		return nil, err
	}
	header, ok := headers[view.batchSeq+1]
	if !ok {
		return nil, nil
	}
	manifest, err := ReadSegmentManifest(header.Path)
	if err != nil {
		return nil, err
	}
	record := &FoldRecord{
		FromSlot: manifest.FromSlot, ThroughSlot: manifest.ThroughSlot,
		Bankhashes: manifest.Bankhashes, Keys: make([]solana.PublicKey, len(manifest.Records)),
	}
	for i := range manifest.Records {
		record.Keys[i] = solana.PublicKey(manifest.Records[i].Pubkey)
	}
	return record, nil
}

// KeysWrittenAfter returns the accounts written by the fold that follows the
// view's boundary, or nil when the boundary is the head.
func (view *CheckpointView) KeysWrittenAfter() ([]solana.PublicKey, error) {
	record, err := view.FoldAfter()
	if err != nil || record == nil {
		return nil, err
	}
	return record.Keys, nil
}
