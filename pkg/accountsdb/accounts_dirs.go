package accountsdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// An AccountsDB may spread its appendvec files over several accounts
// directories, one per physical disk, so a snapshot unpack writes to all of
// them at once. Files keep the ordinary "<slot>.<fileId>" format; a file lives
// in directory fileId % N. The snapshot unpack picks a directory per appendvec
// and mints a matching ID, runtime writes allocate IDs in the directory they
// write to, and fold segments and every manifest stay in the primary (first)
// directory. A single directory is exactly the legacy layout.

// AccountsDirCountFile records N in the primary AccountsDB directory. It is
// written only when N > 1; its absence means one directory.
const AccountsDirCountFile = "accounts_dir_count"

// WriteAccountsDirCount records how many accounts directories a build used.
func WriteAccountsDirCount(metaDir string, n int) error {
	path := filepath.Join(metaDir, AccountsDirCountFile)
	if n <= 1 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(n))
	return os.WriteFile(path, buf[:], 0o644)
}

func readAccountsDirCount(metaDir string) (int, error) {
	b, err := os.ReadFile(filepath.Join(metaDir, AccountsDirCountFile))
	if os.IsNotExist(err) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("%s: expected 8 bytes, got %d", AccountsDirCountFile, len(b))
	}
	return int(binary.LittleEndian.Uint64(b)), nil
}

func (db *AccountsDb) dirCount() uint64 {
	if len(db.AcctsDirs) <= 1 {
		return 1
	}
	return uint64(len(db.AcctsDirs))
}

// AccountsDirs returns every directory holding appendvec files, primary first.
func (db *AccountsDb) AccountsDirs() []string {
	if len(db.AcctsDirs) == 0 {
		return []string{db.AcctsDir}
	}
	return db.AcctsDirs
}

func (db *AccountsDb) appendVecDir(fileId uint64) string {
	if len(db.AcctsDirs) <= 1 {
		return db.AcctsDir
	}
	return db.AcctsDirs[fileId%uint64(len(db.AcctsDirs))]
}

func (db *AccountsDb) appendVecPath(slot, fileId uint64) string {
	return filepath.Join(db.appendVecDir(fileId), SegmentDataName(slot, fileId))
}

// chooseShard picks the directory for a legacy direct store: the primary.
func (db *AccountsDb) chooseShard() int { return 0 }

// nextFileId returns a fresh file ID stored in directory dir (0 = primary).
// It advances the shared high-water mark; callers persist it before writing
// the first data byte (I7).
func (db *AccountsDb) nextFileId(dir int) uint64 {
	n := db.dirCount()
	for {
		prev := db.LargestFileId.Load()
		next := prev + 1
		if n > 1 {
			next = (prev/n+1)*n + uint64(dir)%n
		}
		if db.LargestFileId.CompareAndSwap(prev, next) {
			return next
		}
	}
}
