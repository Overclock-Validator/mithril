package accountsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
)

const (
	// Sentinel FileId values for the big snapshot files. Accounts loaded from the
	// full snapshot have FileId=SnapshotFileId and point into accounts/snapshot.dat;
	// accounts loaded from the incremental snapshot have FileId=IncrementalFileId and
	// point into accounts/incremental.dat. The Offset field of such index entries is
	// the global byte offset within that big file. All other FileIds (>= FirstReplayFileId)
	// are individual appendvec files (accounts/SLOT.FILEID) created during replay.
	SnapshotFileId    = uint64(0)
	IncrementalFileId = uint64(1)
	FirstReplayFileId = uint64(2)
)

// IsSnapshotFile returns true if the FileId refers to one of the big snapshot files.
func IsSnapshotFile(fileId uint64) bool {
	return fileId == SnapshotFileId || fileId == IncrementalFileId
}

type AccountIndexEntry struct {
	Slot   uint64
	FileId uint64
	Offset uint64
}

func (entry AccountIndexEntry) Marshal(out *[24]byte) {
	binary.LittleEndian.PutUint64(out[0:8], entry.Slot)
	binary.LittleEndian.PutUint64(out[8:16], entry.FileId)
	binary.LittleEndian.PutUint64(out[16:24], entry.Offset)
}

func (entry *AccountIndexEntry) Unmarshal(in *[24]byte) {
	entry.Slot = binary.LittleEndian.Uint64(in[0:8])
	entry.FileId = binary.LittleEndian.Uint64(in[8:16])
	entry.Offset = binary.LittleEndian.Uint64(in[16:24])
}

func UnmarshalAcctIdxEntry(data []byte) (*AccountIndexEntry, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("UnmarshalAcctIdxEntry: input had %d < 24 minimum bytes", len(data))
	}
	out := &AccountIndexEntry{}
	out.Unmarshal((*[24]byte)(data[:24]))
	return out, nil
}

// StakeIndexEntry stores a stake account pubkey with its appendvec location hint.
// The location (FileId, Offset) is used for sorting to achieve sequential I/O.
// It may be stale; actual reads still go through Pebble for the canonical location.
type StakeIndexEntry struct {
	Pubkey solana.PublicKey
	FileId uint64
	Offset uint64
}

// StakeIndexMagic is the magic header for stake pubkey index files.
var StakeIndexMagic = [4]byte{'S', 'T', 'K', 'I'}

const StakeIndexVersion = uint32(2)
const StakeIndexRecordSize = 48 // 32-byte pubkey + 8-byte fileId + 8-byte offset

// WriteStakePubkeyIndex writes stake index entries.
// Format: 8-byte header ("STKI" + version uint32 LE) + N × 48-byte records.
func WriteStakePubkeyIndex(path string, entries []StakeIndexEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewWriterSize(f, 1<<20)

	// Write header
	var header [8]byte
	copy(header[0:4], StakeIndexMagic[:])
	binary.LittleEndian.PutUint32(header[4:8], StakeIndexVersion)
	if _, err := buf.Write(header[:]); err != nil {
		return fmt.Errorf("writing stake index header: %w", err)
	}

	// Write records
	var record [StakeIndexRecordSize]byte
	for _, e := range entries {
		copy(record[0:32], e.Pubkey[:])
		binary.LittleEndian.PutUint64(record[32:40], e.FileId)
		binary.LittleEndian.PutUint64(record[40:48], e.Offset)
		if _, err := buf.Write(record[:]); err != nil {
			return fmt.Errorf("writing stake index record: %w", err)
		}
	}

	return buf.Flush()
}

// BuildIndexEntriesFromAppendVecs parses an appendvec and returns:
// - pubkeys: all account pubkeys
// - acctIdxEntries: index entries for each account
// - stakeEntries: stake account pubkeys with their appendvec location hints
//
// fileId is the FileId recorded in every produced index entry (a big-file
// sentinel such as SnapshotFileId/IncrementalFileId). baseOffset is added to
// each account's in-appendvec offset so the recorded Offset is the global byte
// offset within the big file that the appendvec was concatenated into.
func BuildIndexEntriesFromAppendVecs(data []byte, fileSize uint64, slot uint64, fileId uint64, baseOffset uint64) ([]solana.PublicKey, []AccountIndexEntry, []StakeIndexEntry, error) {
	pubkeys := make([]solana.PublicKey, 0, 20000)
	acctIdxEntries := make([]AccountIndexEntry, 0, 20000)
	stakeEntries := make([]StakeIndexEntry, 0, 1000)
	var err error

	parser := &appendVecParser{Buf: data, FileSize: fileSize, FileId: fileId, Slot: slot}

	var owner solana.PublicKey
	for {
		pubkeys = append(pubkeys, solana.PublicKey{})
		acctIdxEntries = append(acctIdxEntries, AccountIndexEntry{})
		idx := len(acctIdxEntries) - 1
		err = parser.ParseNextAcctWithOwner(&pubkeys[idx], &acctIdxEntries[idx], &owner)
		if err != nil {
			pubkeys = pubkeys[:idx]
			acctIdxEntries = acctIdxEntries[:idx]
			break
		}
		// Translate the in-appendvec offset to a global big-file offset.
		acctIdxEntries[idx].Offset += baseOffset

		// Collect stake account entries with appendvec location hints
		if bytes.Equal(owner[:], addresses.StakeProgramAddr[:]) {
			stakeEntries = append(stakeEntries, StakeIndexEntry{
				Pubkey: pubkeys[idx],
				FileId: acctIdxEntries[idx].FileId,
				Offset: acctIdxEntries[idx].Offset,
			})
		}
	}

	return pubkeys, acctIdxEntries, stakeEntries, nil
}
