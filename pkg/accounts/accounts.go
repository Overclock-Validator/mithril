package accounts

import (
	"io"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type Accounts interface {
	GetAccount(pubkey *[32]byte) (*Account, error)
	SetAccount(pubkey *[32]byte, acc *Account) error
	SetAccounts(accts []*Account) error
	AllAccounts() []*Account
	SetAccountWithoutLock(pubkey solana.PublicKey, acc *Account) error
	GetAccountWithoutLock(pubkey solana.PublicKey) (*Account, error)
}

type Account struct {
	Slot       uint64
	Key        solana.PublicKey
	Lamports   uint64
	Data       []byte
	Owner      [32]byte
	Executable bool
	RentEpoch  uint64
	IsDummy    bool
	storage    storageInfo
}

type storageInfo struct {
	Slot    uint64
	FileId  uint64
	Offset  uint64
	DataLen uint64
	Valid   bool
}

const NativeLoaderAddrStr = "NativeLoader1111111111111111111111111111111"

var NativeLoaderAddr = base58.MustDecodeFromString(NativeLoaderAddrStr)

func (a *Account) IsExecutable() bool {
	return a.Executable
}

func (a *Account) IsBuiltin() bool {
	return a.Owner == NativeLoaderAddr && len(a.Data) != 0
}

// have this placeholder setter to allow for locking/mutex later
func (a *Account) SetData(data []byte) {
	a.Data = data
}

func (a *Account) SetLamports(lamports uint64) {
	a.Lamports = lamports
}

func (a *Account) SetExecutable(isExecutable bool) {
	a.Executable = isExecutable
}

func (a *Account) Resize(newLen uint64, fillVal byte) {
	currentDataLen := uint64(len(a.Data))

	if newLen > currentDataLen { // extend, copy existing data, and fill the new excess with fillVal
		newData := make([]byte, newLen)
		copy(newData, a.Data)
		for count := uint64(currentDataLen); count < newLen; count++ {
			newData[count] = fillVal
		}
		a.Data = newData
	} else { // truncate
		a.Data = a.Data[:newLen]
	}
}

// This makes a fresh copy of the account's data in 'newCopy' by allocating a new slice and copying the
// existing data into it. Without this, 'newCopy' would be a shallow copy, and its 'Data' member would point
// into the same underlying data slice as 'a'.
func (a *Account) Clone() *Account {
	newCopy := *a
	newCopy.Data = make([]byte, len(a.Data))
	copy(newCopy.Data, a.Data)
	return &newCopy
}

func (a *Account) StorageInfo() (slot uint64, fileId uint64, offset uint64, dataLen uint64, ok bool) {
	if !a.storage.Valid {
		return 0, 0, 0, 0, false
	}
	return a.storage.Slot, a.storage.FileId, a.storage.Offset, a.storage.DataLen, true
}

func (a *Account) SetStorageInfo(slot uint64, fileId uint64, offset uint64, dataLen uint64) {
	a.storage = storageInfo{
		Slot:    slot,
		FileId:  fileId,
		Offset:  offset,
		DataLen: dataLen,
		Valid:   true,
	}
}

func (a *Account) ClearStorageInfo() {
	a.storage = storageInfo{}
}

func (a *Account) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	a.ClearStorageInfo()

	a.Slot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	var keyBytes []byte
	keyBytes, err = decoder.ReadNBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	a.Key = solana.PublicKeyFromBytes(keyBytes)

	a.Lamports, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	var dataLen uint64
	dataLen, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if dataLen > uint64(decoder.Remaining()) {
		return io.ErrUnexpectedEOF
	}
	a.Data, err = decoder.ReadNBytes(int(dataLen))
	if err != nil {
		return err
	}

	var ownerBytes []byte
	ownerBytes, err = decoder.ReadNBytes(solana.PublicKeyLength)
	if err != nil {
		return err
	}
	copy(a.Owner[:], ownerBytes)

	a.Executable, err = decoder.ReadBool()
	if err != nil {
		return err
	}

	a.RentEpoch, err = decoder.ReadUint64(bin.LE)
	return err
}

func (a *Account) MarshalWithEncoder(encoder *bin.Encoder) error {
	_ = encoder.WriteUint64(a.Slot, bin.LE)
	_ = encoder.WriteBytes(a.Key[:], false)
	_ = encoder.WriteUint64(a.Lamports, bin.LE)
	_ = encoder.WriteUint64(uint64(len(a.Data)), bin.LE)
	_ = encoder.WriteBytes(a.Data, false)
	_ = encoder.WriteBytes(a.Owner[:], false)
	_ = encoder.WriteBool(a.Executable)
	return encoder.WriteUint64(a.RentEpoch, bin.LE)
}
