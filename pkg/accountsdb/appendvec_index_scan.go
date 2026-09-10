package accountsdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
)

// ScanIndexEntriesFromAppendVecs parses an appendvec in bounded batches. Each
// batch is owned by consume after the call starts; the scanner allocates fresh
// backing arrays before producing another batch. This lets snapshot bootstrap
// mmap even a format-valid multi-gigabyte appendvec without allocating output
// proportional to the entire file.
func ScanIndexEntriesFromAppendVecs(
	data []byte,
	fileSize uint64,
	slot uint64,
	fileID uint64,
	batchSize int,
	consume func([]solana.PublicKey, []AccountIndexEntry, []StakeIndexEntry) error,
) error {
	if batchSize <= 0 {
		return fmt.Errorf("appendvec index scan batch size must be positive, got %d", batchSize)
	}
	if consume == nil {
		return errors.New("appendvec index scan consume callback is nil")
	}
	parser := &appendVecParser{Buf: data, FileSize: fileSize, FileId: fileID, Slot: slot}

	for {
		pubkeys := make([]solana.PublicKey, 0, batchSize)
		entries := make([]AccountIndexEntry, 0, batchSize)
		stakeEntries := make([]StakeIndexEntry, 0, min(batchSize, 1000))
		reachedEnd := false

		for len(pubkeys) < batchSize {
			var pubkey solana.PublicKey
			var entry AccountIndexEntry
			var owner solana.PublicKey
			err := parser.ParseNextAcctWithOwner(&pubkey, &entry, &owner)
			if errors.Is(err, io.EOF) {
				reachedEnd = true
				break
			}
			if err != nil {
				return fmt.Errorf("parse appendvec slot=%d file_id=%d: %w", slot, fileID, err)
			}
			pubkeys = append(pubkeys, pubkey)
			entries = append(entries, entry)
			if bytes.Equal(owner[:], addresses.StakeProgramAddr[:]) {
				stakeEntries = append(stakeEntries, StakeIndexEntry{
					Pubkey: pubkey,
					FileId: entry.FileId,
					Offset: entry.Offset,
				})
			}
		}

		if len(pubkeys) != 0 {
			if err := consume(pubkeys, entries, stakeEntries); err != nil {
				return err
			}
		}
		if reachedEnd {
			return nil
		}
	}
}
