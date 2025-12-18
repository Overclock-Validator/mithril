package accountsdb

import (
	"encoding/binary"
	"fmt"
	"io"

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
	hdrLen        = 136
	dataLenOffset = 8
	pubkeyOffset  = 16
)

type appendVecParser struct {
	Reader   io.Reader
	FileSize uint64
	Offset   uint64

	FileId uint64
	Slot   uint64
}

func (parser *appendVecParser) ParseNextAcct(pk *solana.PublicKey, a *AccountIndexEntry) error {
	if parser.Offset+hdrLen > parser.FileSize {
		return io.EOF
	}

	var hdr [hdrLen]byte
	if _, err := io.ReadFull(parser.Reader, hdr[:]); err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("reading header: %w", err)
	}

	dataLen := binary.LittleEndian.Uint64(hdr[dataLenOffset : dataLenOffset+8])

	*pk = solana.PublicKeyFromBytes(hdr[pubkeyOffset : pubkeyOffset+32])
	a.Slot = parser.Slot
	a.FileId = parser.FileId
	a.Offset = parser.Offset

	parser.Offset += hdrLen

	alignedLen := util.AlignUp(dataLen, 8)

	if parser.Offset+dataLen > parser.FileSize {
		return fmt.Errorf("overflow")
	}
	if alignedLen > 0 {
		if seeker, ok := parser.Reader.(io.Seeker); ok {
			if _, err := seeker.Seek(int64(alignedLen), io.SeekCurrent); err != nil {
				return fmt.Errorf("seeking data: %w", err)
			}
		} else {
			if _, err := io.CopyN(io.Discard, parser.Reader, int64(alignedLen)); err != nil {
				return fmt.Errorf("discarding data: %w", err)
			}
		}
	}

	parser.Offset += util.AlignUp(dataLen, 8)

	return nil
}

func (acct *AppendVecAccount) Unmarshal(buf io.Reader) error {
	var err error
	var hdrBytes [hdrLen]byte
	_, err = buf.Read(hdrBytes[:])
	if err != nil {
		return err
	}

	acct.WriteVersion = binary.LittleEndian.Uint64(hdrBytes[:8])
	acct.DataLen = binary.LittleEndian.Uint64(hdrBytes[8:16])
	copy(acct.Pubkey[:], hdrBytes[16:48])
	acct.Lamports = binary.LittleEndian.Uint64(hdrBytes[48:56])
	acct.RentEpoch = binary.LittleEndian.Uint64(hdrBytes[56:64])
	copy(acct.Owner[:], hdrBytes[64:96])
	acct.Executable = hdrBytes[96] != 0
	copy(acct.Padding[:], hdrBytes[97:104])
	copy(acct.Hash[:], hdrBytes[104:136])

	acct.Data = make([]byte, acct.DataLen)
	_, err = buf.Read(acct.Data)

	return err
}

var padding [2048]byte

func (acct *AppendVecAccount) Marshal(buf io.Writer) error {
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

	_, err = buf.Write(hdrBytes[:])
	if err != nil {
		return err
	}

	_, err = buf.Write(acct.Data)
	if err != nil {
		return err
	}

	numPaddingBytes := util.AlignUp(acct.DataLen, 8) - acct.DataLen
	n, err := buf.Write(padding[:numPaddingBytes])
	if err != nil {
		return err
	} else if n != int(numPaddingBytes) {
		return fmt.Errorf("number of padding bytes written was %d rather than %d", n, numPaddingBytes)
	}

	return nil
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
