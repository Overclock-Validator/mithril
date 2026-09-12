package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
)

// RejectV2Artifacts prevents the legacy opener from creating an empty Pebble
// index beside an incompatible V2 store. No format conversion is implicit.
func RejectV2Artifacts(root string) error {
	for _, name := range []string{"accounts_index_v2.lock", "accounts_index.root", "accounts_delta_v2.journal", "accounts_index.stmh", "accounts_index.manifest"} {
		if _, err := os.Lstat(filepath.Join(root, name)); err == nil {
			return fmt.Errorf("accountsdb: incompatible V2 store (%s); initialize a new Pebble database from genesis or a snapshot", name)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func ValidatePebbleArtifacts(root string) error {
	if err := RejectV2Artifacts(root); err != nil {
		return err
	}
	for _, name := range []string{"mithril_db", "bankhash_db", "accounts"} {
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("accountsdb: %s is not a real directory", name)
		}
	}
	for _, name := range []string{"mithril_db", "bankhash_db"} {
		info, err := os.Lstat(filepath.Join(root, name, "CURRENT"))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() == 0 {
			return fmt.Errorf("accountsdb: invalid %s/CURRENT", name)
		}
	}
	return nil
}

func readUint64Sidecar(root, name string) (uint64, error) {
	path := filepath.Join(root, name)
	info, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() != 8 {
		return 0, fmt.Errorf("accountsdb: invalid %s: expected eight-byte regular file", name)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("accountsdb: changed %s", name)
	}
	return binary.LittleEndian.Uint64(raw), nil
}

func ValidateLargestFileID(root string) (uint64, error) {
	return readUint64Sidecar(root, "largest_file_id")
}
func ReadBootstrapHighFileID(root string) (uint64, error) {
	return readUint64Sidecar(root, "bootstrap_high_file_id")
}

// writeUint64Sidecar publishes only a complete, fsynced value. Callers hold the
// store guard; noReplace is used for the immutable bootstrap allocation bound.
func writeUint64Sidecar(root, name string, value uint64, noReplace bool) (retErr error) {
	f, err := os.CreateTemp(root, "."+name+"-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], value)
	_, err = f.Write(raw[:])
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	dest := filepath.Join(root, name)
	if noReplace {
		err = util.RenameNoReplace(tmp, dest)
	} else {
		err = os.Rename(tmp, dest)
	}
	if err != nil {
		return err
	}
	return fsyncDir(root)
}

func WriteLargestFileID(root string, value uint64) error {
	old, err := ValidateLargestFileID(root)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil && value < old {
		return fmt.Errorf("accountsdb: cannot decrease largest file ID")
	}
	return writeUint64Sidecar(root, "largest_file_id", value, false)
}
func WriteBootstrapHighFileID(root string, value uint64) error {
	return writeUint64Sidecar(root, "bootstrap_high_file_id", value, true)
}

// ValidateStakePubkeyIndex checks the existing Pebble branch's version-2 format
// with bounded memory. It intentionally does not introduce V2's framed format.
func ValidateStakePubkeyIndex(path string) (uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() < 8 || (info.Size()-8)%StakeIndexRecordSize != 0 {
		return 0, fmt.Errorf("accountsdb: malformed stake index")
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var header [8]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return 0, err
	}
	if !bytes.Equal(header[:4], StakeIndexMagic[:]) || binary.LittleEndian.Uint32(header[4:]) != StakeIndexVersion {
		return 0, fmt.Errorf("accountsdb: unsupported stake index")
	}
	return uint64((info.Size() - 8) / StakeIndexRecordSize), nil
}

// ScanKeysBetweenPrefixes streams keys in canonical byte order from a pinned
// Pebble snapshot. Prefix bounds are inclusive big-endian uint64 values. It
// skips fold metadata keys. Callers needing coherent account bytes must also
// serialize durable mutations for the scan's duration (as GenesisReplay does).
func (db *AccountsDb) ScanKeysBetweenPrefixes(ctx context.Context, start, end uint64, visit func(solana.PublicKey) error) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if visit == nil || start > end {
		return fmt.Errorf("accountsdb: invalid key scan")
	}
	snapshot := db.Index.NewSnapshot()
	defer func() { retErr = errors.Join(retErr, snapshot.Close()) }()
	var lower, upper [8]byte
	binary.BigEndian.PutUint64(lower[:], start)
	options := &pebble.IterOptions{LowerBound: lower[:]}
	if end != ^uint64(0) {
		binary.BigEndian.PutUint64(upper[:], end+1)
		options.UpperBound = upper[:]
	}
	iter, err := snapshot.NewIter(options)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, iter.Close()) }()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(iter.Key()) != 32 {
			continue
		}
		if _, err := UnmarshalAcctIdxEntryValue(iter.Value()); err != nil {
			return err
		}
		var key solana.PublicKey
		copy(key[:], iter.Key())
		if err := visit(key); err != nil {
			return err
		}
	}
	return iter.Error()
}

// RecoverGenesisFoldState validates every decided fold before ordinary recovery
// may remove any orphan. Offline genesis retains its whole linear manifest
// history; gaps, corruption, rewind and compaction are not valid in this mode.
// Ordinary snapshot-origin recovery retains its existing policy.
func (db *AccountsDb) RecoverGenesisFoldState() (RecoveryResult, error) {
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	root := filepath.Dir(db.AcctsDir)
	bootstrap, err := ReadBootstrapHighFileID(root)
	if err != nil {
		return RecoveryResult{}, err
	}
	largest, err := ValidateLargestFileID(root)
	if err != nil {
		return RecoveryResult{}, err
	}
	if bootstrap != 1 || largest < bootstrap {
		return RecoveryResult{}, fmt.Errorf("accountsdb: invalid genesis file-ID bounds")
	}
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return RecoveryResult{}, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) == ".rewound" {
			return RecoveryResult{}, fmt.Errorf("accountsdb: genesis rewind is unsupported")
		}
		if filepath.Ext(name) != ".manifest" {
			continue
		}
		h, err := readManifestHeader(filepath.Join(db.AcctsDir, name))
		if err != nil {
			return RecoveryResult{}, err
		}
		if h.Kind != ManifestKindFold {
			return RecoveryResult{}, fmt.Errorf("accountsdb: genesis compaction is unsupported")
		}
	}
	// ListFoldManifests supplies canonical sequence order after the full inventory
	// above has established that no unsupported manifest was silently skipped.
	manifests, err := ListFoldManifests(db.AcctsDir)
	if err != nil {
		return RecoveryResult{}, err
	}
	meta, haveMeta, err := db.readFoldMeta()
	if err != nil {
		return RecoveryResult{}, err
	}
	var through uint64
	seenFiles := map[uint64]bool{bootstrap: true}
	foundMeta := !haveMeta
	var bankhashes []SlotBankhash
	for i, h := range manifests {
		m, err := db.verifyAndReadManifest(h)
		if err != nil {
			return RecoveryResult{}, err
		}
		if m.BatchSeq != uint64(i)+1 || m.FromSlot != through || m.ThroughSlot <= through || m.FileId <= bootstrap || m.FileId > largest || seenFiles[m.FileId] || h.Path != segmentManifestPath(db.AcctsDir, m.ThroughSlot, m.FileId) {
			return RecoveryResult{}, fmt.Errorf("accountsdb: invalid genesis manifest lineage")
		}
		if uint64(len(m.Bankhashes)) != m.ThroughSlot-m.FromSlot {
			return RecoveryResult{}, fmt.Errorf("accountsdb: incomplete genesis manifest bank hashes")
		}
		for j, hash := range m.Bankhashes {
			if hash.Slot != m.FromSlot+uint64(j)+1 {
				return RecoveryResult{}, fmt.Errorf("accountsdb: invalid genesis bank-hash lineage")
			}
		}
		bankhashes = append(bankhashes, m.Bankhashes...)
		seenFiles[m.FileId] = true
		through = m.ThroughSlot
		if haveMeta && m.BatchSeq == meta.BatchSeq {
			if meta.ThroughSlot != m.ThroughSlot || meta.FileId != m.FileId {
				return RecoveryResult{}, fmt.Errorf("accountsdb: genesis index watermark mismatch")
			}
			foundMeta = true
		}
	}
	if !foundMeta {
		return RecoveryResult{}, fmt.Errorf("accountsdb: genesis index watermark has no manifest")
	}
	recovered, err := db.recoverFoldStateLocked()
	if err != nil {
		return recovered, err
	}
	// Pebble's account-index watermark can survive while the independently
	// written bankhash WAL does not. Rebuild hashes even for already-applied
	// manifests; the validated manifest is authoritative for both databases.
	batch := db.BankHashStore.NewBatch()
	for _, hash := range bankhashes {
		var key [8]byte
		binary.LittleEndian.PutUint64(key[:], hash.Slot)
		if err := batch.Set(key[:], hash.Bankhash[:], nil); err != nil {
			return recovered, errors.Join(err, batch.Close())
		}
	}
	if len(bankhashes) == 0 {
		return recovered, batch.Close()
	}
	return recovered, errors.Join(batch.Commit(pebble.Sync), batch.Close())
}
