package accountsdb

import (
	"encoding/binary"
	"fmt"
	"io"

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

func BuildIndexEntriesFromAppendVecs(reader io.Reader, fileSize uint64, slot uint64, fileId uint64) ([]solana.PublicKey, []AccountIndexEntry, error) {
	pubkeys := make([]solana.PublicKey, 0, 20000)
	acctIdxEntries := make([]AccountIndexEntry, 0, 20000)
	var err error

	parser := &appendVecParser{Reader: reader, FileSize: fileSize, FileId: fileId, Slot: slot}

	for {
		pubkeys = append(pubkeys, solana.PublicKey{})
		acctIdxEntries = append(acctIdxEntries, AccountIndexEntry{})
		err = parser.ParseNextAcct(&pubkeys[len(pubkeys)-1], &acctIdxEntries[len(acctIdxEntries)-1])
		if err != nil {
			break
		}
	}

	if err != io.EOF {
		return nil, nil, err
	}

	// Remove the last empty entry added before loop break
	return pubkeys[:len(pubkeys)-1], acctIdxEntries[:len(acctIdxEntries)-1], nil
}
