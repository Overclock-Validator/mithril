package accountsdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/gagliardetto/solana-go"
)

type AppendVecAccount struct {
	WriteVersion uint64
	DataLen      uint64
	Pubkey       solana.PublicKey
	Lamports     uint64
	RentEpoch    uint64
	Owner        solana.PublicKey
	Executable   bool
	Padding      [7]byte
	Hash         [32]byte
	Data         []byte
}

const (
	hdrLen                     = 136
	dataLenOffset              = 8
	pubkeyOffset               = 16
	lamportsOffset             = 48
	ownerOffset                = 64
	maxAppendVecAccountDataLen = 10 * 1024 * 1024
)

type appendVecParser struct {
	Buf      []byte
	FileSize uint64
	Offset   uint64

	FileId uint64
	Slot   uint64
}

func (parser *appendVecParser) ParseNextAcct(pk *solana.PublicKey, a *AccountIndexEntry) error {
	return parser.parseNextAcct(pk, a, nil)
}

// ParseNextAcctWithOwner parses the next account from the appendvec, extracting
// pubkey, index entry, and owner. Used for building stake pubkey index during
// snapshot processing.
func (parser *appendVecParser) ParseNextAcctWithOwner(pk *solana.PublicKey, a *AccountIndexEntry, owner *solana.PublicKey) error {
	return parser.parseNextAcct(pk, a, owner)
}

// parseNextAcct only scans bytes that are part of Buf's valid slice.  In
// particular, a []byte may have zero-filled backing storage between len and
// cap; treating that storage as appendvec data creates phantom default-pubkey
// index entries when a manifest advertises a larger file size than the tar
// member actually contains.
func (parser *appendVecParser) parseNextAcct(pk *solana.PublicKey, a *AccountIndexEntry, owner *solana.PublicKey) error {
	limit := parser.FileSize
	if bufLen := uint64(len(parser.Buf)); limit > bufLen {
		limit = bufLen
	}

	// The final account's data is not necessarily followed by its alignment
	// padding in a snapshot, so Offset can be up to seven bytes beyond limit.
	if parser.Offset >= limit {
		return io.EOF
	}

	remaining := limit - parser.Offset
	if remaining < hdrLen {
		// A zero tail is unused appendvec storage.  Non-zero partial metadata is
		// malformed and must not be mistaken for a clean end-of-file.
		tail := parser.Buf[int(parser.Offset):int(limit):int(limit)]
		for _, b := range tail {
			if b != 0 {
				return fmt.Errorf("truncated appendvec header at offset %d: have %d bytes, need %d", parser.Offset, remaining, hdrLen)
			}
		}
		return io.EOF
	}

	// Give all subsequent slices a capacity equal to the valid scan limit.  The
	// explicit bound is defense in depth against accidentally reslicing into
	// Buf's spare capacity in future parser changes.
	buf := parser.Buf[:int(limit):int(limit)]
	offset := parser.Offset
	dataLen := binary.LittleEndian.Uint64(buf[offset+dataLenOffset : offset+dataLenOffset+8])
	parsedPubkey := solana.PublicKeyFromBytes(buf[offset+pubkeyOffset : offset+pubkeyOffset+32])
	lamports := binary.LittleEndian.Uint64(buf[offset+lamportsOffset : offset+lamportsOffset+8])

	// Agave uses a zero-lamport account with the default pubkey as the marker
	// for unused appendvec storage.  It is a terminator, not an account.
	if parsedPubkey == (solana.PublicKey{}) && lamports == 0 {
		return io.EOF
	}
	if dataLen > maxAppendVecAccountDataLen {
		return fmt.Errorf(
			"appendvec account data length %d exceeds maximum %d at offset %d",
			dataLen,
			maxAppendVecAccountDataLen,
			offset,
		)
	}

	dataOffset := offset + hdrLen
	if dataLen > limit-dataOffset {
		return fmt.Errorf("truncated appendvec account data at offset %d: data length %d exceeds %d available bytes", offset, dataLen, limit-dataOffset)
	}
	alignedDataLen := util.AlignUp(dataLen, 8)
	if alignedDataLen < dataLen {
		return fmt.Errorf("appendvec account data length overflows alignment at offset %d: %d", offset, dataLen)
	}

	*pk = parsedPubkey
	a.Slot = parser.Slot
	a.FileId = parser.FileId
	a.Offset = offset
	if owner != nil {
		*owner = solana.PublicKeyFromBytes(buf[offset+ownerOffset : offset+ownerOffset+32])
	}
	parser.Offset = dataOffset + alignedDataLen

	return nil
}

// GetAppendVecDataLen reads only the bytes necessary to determine the length
// of the append vec. Should be kept in sync with
// (*AppendVecAccount).Unmarshal.
func GetAppendVecDataLen(f *os.File, offset uint64) (uint64, error) {
	if offset > uint64(int64Max-hdrLen) {
		return 0, fmt.Errorf("appendvec account offset %d overflows int64: %w", offset, io.ErrUnexpectedEOF)
	}
	var hdrBytes [8]byte
	_, err := f.ReadAt(hdrBytes[:], int64(offset)+dataLenOffset)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(hdrBytes[:]), nil
}

func (acct *AppendVecAccount) Unmarshal(buf io.Reader) error {
	if err := acct.unmarshalHeader(buf); err != nil {
		return err
	}
	return acct.unmarshalData(buf)
}

func (acct *AppendVecAccount) unmarshalHeader(buf io.Reader) error {
	var hdrBytes [hdrLen]byte
	if _, err := io.ReadFull(buf, hdrBytes[:]); err != nil {
		return err
	}
	acct.unmarshalHeaderBytes(&hdrBytes)
	return nil
}

func (acct *AppendVecAccount) unmarshalHeaderBytes(hdrBytes *[hdrLen]byte) {
	acct.WriteVersion = binary.LittleEndian.Uint64(hdrBytes[:8])
	acct.DataLen = binary.LittleEndian.Uint64(hdrBytes[8:16])
	copy(acct.Pubkey[:], hdrBytes[16:48])
	acct.Lamports = binary.LittleEndian.Uint64(hdrBytes[48:56])
	acct.RentEpoch = binary.LittleEndian.Uint64(hdrBytes[56:64])
	copy(acct.Owner[:], hdrBytes[64:96])
	acct.Executable = hdrBytes[96] != 0
	copy(acct.Padding[:], hdrBytes[97:104])
	copy(acct.Hash[:], hdrBytes[104:136])
}

func (acct *AppendVecAccount) unmarshalData(buf io.Reader) error {
	if acct.isTerminator() {
		return io.EOF
	}
	if acct.DataLen > uint64(maxInt) {
		return fmt.Errorf(
			"appendvec account data length %d overflows addressable range: %w",
			acct.DataLen,
			io.ErrUnexpectedEOF,
		)
	}
	// This is the Solana per-account data limit. Keeping the same bound at the
	// persistence boundary prevents a corrupt appendvec header from turning a
	// tiny read into a multi-gigabyte allocation, without adding a Stat syscall
	// to every account read.
	if acct.DataLen > maxAppendVecAccountDataLen {
		return fmt.Errorf(
			"appendvec account data length %d exceeds maximum %d: %w",
			acct.DataLen,
			maxAppendVecAccountDataLen,
			io.ErrUnexpectedEOF,
		)
	}
	if available, bounded := appendVecReaderRemaining(buf); bounded && acct.DataLen > available {
		return fmt.Errorf(
			"appendvec account data length %d exceeds %d available bytes: %w",
			acct.DataLen,
			available,
			io.ErrUnexpectedEOF,
		)
	}

	acct.Data = make([]byte, int(acct.DataLen))
	_, err := io.ReadFull(buf, acct.Data)
	return err
}

func (acct *AppendVecAccount) isTerminator() bool {
	return acct.Pubkey == (solana.PublicKey{}) && acct.Lamports == 0
}

// appendVecReaderRemaining returns the number of readable bytes when the
// reader exposes a trustworthy in-memory bound. This lets short buffer-backed
// inputs fail before allocating their claimed size. File-backed reads rely on
// the protocol limit above and io.ReadFull, avoiding per-account metadata I/O.
func appendVecReaderRemaining(buf io.Reader) (remaining uint64, bounded bool) {
	if sized, ok := buf.(interface{ Len() int }); ok {
		return uint64(sized.Len()), true
	}
	return 0, false
}

var padding [2048]byte

func (acct *AppendVecAccount) Marshal(buf io.Writer) error {
	_, err := acct.MarshalReturningLength(buf)
	return err
}

func (acct *AppendVecAccount) MarshalReturningLength(buf io.Writer) (int, error) {
	l := 0
	var err error
	var hdrBytes [hdrLen]byte

	binary.LittleEndian.PutUint64(hdrBytes[:8], acct.WriteVersion)
	binary.LittleEndian.PutUint64(hdrBytes[8:16], acct.DataLen)
	copy(hdrBytes[16:48], acct.Pubkey[:])
	binary.LittleEndian.PutUint64(hdrBytes[48:56], acct.Lamports)
	binary.LittleEndian.PutUint64(hdrBytes[56:64], acct.RentEpoch)
	copy(hdrBytes[64:96], acct.Owner[:])

	if acct.Executable {
		hdrBytes[96] = 1
	} else {
		hdrBytes[96] = 0
	}

	copy(hdrBytes[97:104], acct.Padding[:])
	copy(hdrBytes[104:136], acct.Hash[:])

	n, err := buf.Write(hdrBytes[:])
	l += n
	if err != nil {
		return l, err
	}

	n, err = buf.Write(acct.Data)
	l += n
	if err != nil {
		return l, err
	}

	numPaddingBytes := util.AlignUp(acct.DataLen, 8) - acct.DataLen
	n, err = buf.Write(padding[:numPaddingBytes])
	l += n
	if err != nil {
		return l, err
	} else if n != int(numPaddingBytes) {
		return l, fmt.Errorf("number of padding bytes written was %d rather than %d", n, numPaddingBytes)
	}

	return l, nil
}

func (appendVecAcct *AppendVecAccount) ToAccount() *accounts.Account {
	acct := &accounts.Account{Key: appendVecAcct.Pubkey, Lamports: appendVecAcct.Lamports,
		RentEpoch: appendVecAcct.RentEpoch, Owner: appendVecAcct.Owner, Executable: appendVecAcct.Executable,
		Data: appendVecAcct.Data}

	return acct
}

func unmarshalAcctFromAppendVecAcctHeader(buf io.Reader) (*accounts.Account, error) {
	var appendVecAcct AppendVecAccount
	err := appendVecAcct.Unmarshal(buf)
	if err != nil {
		return nil, err
	}

	return appendVecAcct.ToAccount(), nil
}

// unmarshalAcctFromAppendVecAcctHeaderExpected checks the pubkey after reading
// the fixed 136-byte header but before allocating or reading account data. A
// StreamHash non-member can resolve to a real account candidate; rejecting it
// here keeps that exactness check cheap even when the candidate has large data.
// On mismatch, the returned account contains only the stored key and exact is
// false so callers can distinguish a base false-positive from delta corruption.
func unmarshalAcctFromAppendVecAcctHeaderExpected(
	buf io.Reader,
	expected solana.PublicKey,
) (acct *accounts.Account, exact bool, err error) {
	var appendVecAcct AppendVecAccount
	if err := appendVecAcct.unmarshalHeader(buf); err != nil {
		return nil, false, err
	}
	if appendVecAcct.isTerminator() {
		return nil, false, io.EOF
	}
	if appendVecAcct.Pubkey != expected {
		return &accounts.Account{Key: appendVecAcct.Pubkey}, false, nil
	}
	if err := appendVecAcct.unmarshalData(buf); err != nil {
		return nil, false, err
	}
	return appendVecAcct.ToAccount(), true, nil
}

// unmarshalAcctFromAppendVecAcctHeaderExpectedAt is the ReaderAt variant used
// by batch loading. It avoids allocating a SectionReader for every account and
// lets several sorted chunks safely share one appendvec file descriptor.
func unmarshalAcctFromAppendVecAcctHeaderExpectedAt(
	buf *os.File,
	offset int64,
	expected solana.PublicKey,
) (acct *accounts.Account, exact bool, err error) {
	if offset < 0 || offset > int64Max-hdrLen {
		return nil, false, fmt.Errorf("appendvec account offset %d overflows int64: %w", offset, io.ErrUnexpectedEOF)
	}

	var hdrBytes [hdrLen]byte
	if _, err := buf.ReadAt(hdrBytes[:], offset); err != nil {
		return nil, false, err
	}
	var appendVecAcct AppendVecAccount
	appendVecAcct.unmarshalHeaderBytes(&hdrBytes)
	if appendVecAcct.isTerminator() {
		return nil, false, io.EOF
	}
	if appendVecAcct.Pubkey != expected {
		return &accounts.Account{Key: appendVecAcct.Pubkey}, false, nil
	}
	if appendVecAcct.DataLen > uint64(maxInt) ||
		appendVecAcct.DataLen > uint64(int64Max-offset-hdrLen) {
		return nil, false, fmt.Errorf(
			"appendvec account data length %d overflows addressable range: %w",
			appendVecAcct.DataLen,
			io.ErrUnexpectedEOF,
		)
	}
	if appendVecAcct.DataLen > maxAppendVecAccountDataLen {
		return nil, false, fmt.Errorf(
			"appendvec account data length %d exceeds maximum %d: %w",
			appendVecAcct.DataLen,
			maxAppendVecAccountDataLen,
			io.ErrUnexpectedEOF,
		)
	}
	dataOffset := offset + hdrLen
	appendVecAcct.Data = make([]byte, int(appendVecAcct.DataLen))
	if len(appendVecAcct.Data) > 0 {
		if _, err := buf.ReadAt(appendVecAcct.Data, dataOffset); err != nil {
			return nil, false, err
		}
	}
	return appendVecAcct.ToAccount(), true, nil
}

const (
	int64Max = int64(^uint64(0) >> 1)
	maxInt   = int(^uint(0) >> 1)
)
