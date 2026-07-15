package accountsdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// fsyncDir makes a directory entry change (create/rename/unlink) durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("accountsdb: open dir for fsync: %w", err)
	}
	defer d.Close()
	return d.Sync()
}

// persistLargestFileId durably records the appendvec/segment file-id high-water
// mark so a restart cannot rewind it and reuse a fileId (which would clobber a
// committed data file).
func (accountsDb *AccountsDb) persistLargestFileId() error {
	path := filepath.Join(filepath.Dir(accountsDb.AcctsDir), "largest_file_id")
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], accountsDb.LargestFileId.Load())
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(b[:], 0); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
