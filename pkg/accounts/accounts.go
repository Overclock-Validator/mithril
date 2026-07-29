package accounts

import (
	"fmt"
	"io"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type Accounts interface {
	GetAccount(pubkey *[32]byte) (*Account, error)
	SetAccount(pubkey *[32]byte, acc *Account) error
	AllAccounts() []*Account
	SetAccountWithoutLock(pubkey solana.PublicKey, acc *Account) error
	GetAccountWithoutLock(pubkey solana.PublicKey) (*Account, error)
}

func validateTransactionAccountBatch(accountStates []*Account, touched []bool) error {
	if len(accountStates) != len(touched) {
		return fmt.Errorf("account states/touched length mismatch: %d != %d", len(accountStates), len(touched))
	}
	for idx, acct := range accountStates {
		if touched[idx] && acct == nil {
			return fmt.Errorf("touched account state at index %d is nil", idx)
		}
	}
	return nil
}

// SetTransactionAccounts publishes touched transaction states in message order,
// canonicalizing zero-lamport states as tombstones. Built-in stores batch their
// synchronization; other Accounts implementations retain the per-key fallback.
func SetTransactionAccounts(store Accounts, accountStates []*Account, touched []bool) error {
	if err := validateTransactionAccountBatch(accountStates, touched); err != nil {
		return err
	}
	switch builtInStore := store.(type) {
	case MemAccounts:
		builtInStore.setTransactionAccounts(accountStates, touched)
		return nil
	case *MemAccounts:
		builtInStore.setTransactionAccounts(accountStates, touched)
		return nil
	case *OverlayAccounts:
		builtInStore.setTransactionAccounts(accountStates, touched)
		return nil
	}
	for idx, acct := range accountStates {
		if !touched[idx] {
			continue
		}
		storedAcct := transactionAccountForStorage(acct)
		key := [32]byte(storedAcct.Key)
		if err := store.SetAccount(&key, storedAcct); err != nil {
			return err
		}
	}
	return nil
}

func transactionAccountForStorage(acct *Account) *Account {
	if acct.Lamports == 0 {
		return &Account{Key: acct.Key, RentEpoch: math.MaxUint64}
	}
	return acct
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
		// make already returns zeroed memory, so a zero fill needs no second
		// pass. Both callers pass 0, and account data runs to
		// MAX_PERMITTED_DATA_LENGTH, so the previous byte-at-a-time loop could
		// rewrite ten megabytes of already-zero bytes on a single allocate.
		//
		// The non-zero path uses `for i := range tail` because that shape is
		// what the compiler recognises and lowers to a memset; the original
		// indexed loop over an offset range is not.
		if fillVal != 0 {
			tail := newData[currentDataLen:]
			for i := range tail {
				tail[i] = fillVal
			}
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

func (a *Account) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
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
