package accountsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
)

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

func unmarshalAcctIdxEntry(data []byte) (*AccountIndexEntry, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("unmarshalAcctIdxEntry: input had %d < 24 minimum bytes", len(data))
	}
	out := &AccountIndexEntry{}
	out.Unmarshal((*[24]byte)(data[:24]))
	return out, nil
}

// BuildIndexEntriesFromAppendVecs parses an appendvec and returns:
// - pubkeys: all account pubkeys
// - acctIdxEntries: index entries for each account
// - stakePubkeys: pubkeys of accounts owned by the stake program
func BuildIndexEntriesFromAppendVecs(data []byte, fileSize uint64, slot uint64, fileId uint64) ([]solana.PublicKey, []AccountIndexEntry, []solana.PublicKey, error) {
	pubkeys := make([]solana.PublicKey, 0, 20000)
	acctIdxEntries := make([]AccountIndexEntry, 0, 20000)
	stakePubkeys := make([]solana.PublicKey, 0, 1000)
	var err error

	parser := &appendVecParser{Buf: data, FileSize: fileSize, FileId: fileId, Slot: slot}

	var owner solana.PublicKey
	for {
		pubkeys = append(pubkeys, solana.PublicKey{})
		acctIdxEntries = append(acctIdxEntries, AccountIndexEntry{})
		err = parser.ParseNextAcctWithOwner(&pubkeys[len(pubkeys)-1], &acctIdxEntries[len(acctIdxEntries)-1], &owner)
		if err != nil {
			pubkeys = pubkeys[:len(pubkeys)-1]
			acctIdxEntries = acctIdxEntries[:len(acctIdxEntries)-1]
			break
		}
		// Collect stake account pubkeys for building stake index
		if bytes.Equal(owner[:], addresses.StakeProgramAddr[:]) {
			stakePubkeys = append(stakePubkeys, pubkeys[len(pubkeys)-1])
		}
	}

	return pubkeys, acctIdxEntries, stakePubkeys, nil
}
