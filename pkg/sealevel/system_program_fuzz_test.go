package sealevel

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzSystemInstrCreateAccount tests CreateAccount instruction deserialization
func FuzzSystemInstrCreateAccount(f *testing.F) {
	f.Add(makeValidCreateAccountInstr(1000000, 100))
	f.Add(makeValidCreateAccountInstr(0, 0))
	f.Add(makeValidCreateAccountInstr(^uint64(0), ^uint64(0)))
	f.Add(makeInvalidCreateAccountInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var createAcct SystemInstrCreateAccount
		_ = createAcct.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrAssign tests Assign instruction deserialization
func FuzzSystemInstrAssign(f *testing.F) {
	f.Add(makeValidAssignInstr())
	f.Add(makeInvalidAssignInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var assign SystemInstrAssign
		_ = assign.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrTransfer tests Transfer instruction deserialization
func FuzzSystemInstrTransfer(f *testing.F) {
	f.Add(makeValidTransferInstr(1000000))
	f.Add(makeValidTransferInstr(0))
	f.Add(makeValidTransferInstr(^uint64(0)))
	f.Add(makeInvalidTransferInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var transfer SystemInstrTransfer
		_ = transfer.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrCreateAccountWithSeed tests CreateAccountWithSeed instruction deserialization
func FuzzSystemInstrCreateAccountWithSeed(f *testing.F) {
	f.Add(makeValidCreateAccountWithSeedInstr("test_seed", 1000000, 100))
	f.Add(makeValidCreateAccountWithSeedInstr("", 0, 0))
	f.Add(makeValidCreateAccountWithSeedInstr("very_long_seed_name_for_testing", ^uint64(0), ^uint64(0)))
	f.Add(makeInvalidCreateAccountWithSeedInstr())
	f.Add(makeInvalidUTF8CreateAccountWithSeedInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var createWithSeed SystemInstrCreateAccountWithSeed
		_ = createWithSeed.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrWithdrawNonceAccount tests WithdrawNonceAccount instruction deserialization
func FuzzSystemInstrWithdrawNonceAccount(f *testing.F) {
	f.Add(makeValidWithdrawNonceInstr(1000000))
	f.Add(makeValidWithdrawNonceInstr(0))
	f.Add(makeValidWithdrawNonceInstr(^uint64(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var withdraw SystemInstrWithdrawNonceAccount
		_ = withdraw.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrInitializeNonceAccount tests InitializeNonceAccount instruction deserialization
func FuzzSystemInstrInitializeNonceAccount(f *testing.F) {
	f.Add(makeValidInitializeNonceInstr())
	f.Add(makeInvalidInitializeNonceInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var initialize SystemInstrInitializeNonceAccount
		_ = initialize.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrAuthorizeNonceAccount tests AuthorizeNonceAccount instruction deserialization
func FuzzSystemInstrAuthorizeNonceAccount(f *testing.F) {
	f.Add(makeValidAuthorizeNonceInstr())
	f.Add(makeInvalidAuthorizeNonceInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var authorize SystemInstrAuthorizeNonceAccount
		_ = authorize.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrAllocate tests Allocate instruction deserialization
func FuzzSystemInstrAllocate(f *testing.F) {
	f.Add(makeValidAllocateInstr(100))
	f.Add(makeValidAllocateInstr(0))
	f.Add(makeValidAllocateInstr(SystemProgMaxPermittedDataLen))
	f.Add(makeValidAllocateInstr(SystemProgMaxPermittedDataLen + 1))
	f.Add(makeValidAllocateInstr(^uint64(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var allocate SystemInstrAllocate
		_ = allocate.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrAllocateWithSeed tests AllocateWithSeed instruction deserialization
func FuzzSystemInstrAllocateWithSeed(f *testing.F) {
	f.Add(makeValidAllocateWithSeedInstr("test_seed", 100))
	f.Add(makeValidAllocateWithSeedInstr("", 0))
	f.Add(makeInvalidAllocateWithSeedInstr())
	f.Add(makeInvalidUTF8AllocateWithSeedInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var allocateWithSeed SystemInstrAllocateWithSeed
		_ = allocateWithSeed.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrAssignWithSeed tests AssignWithSeed instruction deserialization
func FuzzSystemInstrAssignWithSeed(f *testing.F) {
	f.Add(makeValidAssignWithSeedInstr("test_seed"))
	f.Add(makeValidAssignWithSeedInstr(""))
	f.Add(makeInvalidAssignWithSeedInstr())
	f.Add(makeInvalidUTF8AssignWithSeedInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var assignWithSeed SystemInstrAssignWithSeed
		_ = assignWithSeed.UnmarshalWithDecoder(decoder)
	})
}

// FuzzSystemInstrTransferWithSeed tests TransferWithSeed instruction deserialization
func FuzzSystemInstrTransferWithSeed(f *testing.F) {
	f.Add(makeValidTransferWithSeedInstr(1000000, "test_seed"))
	f.Add(makeValidTransferWithSeedInstr(0, ""))
	f.Add(makeInvalidTransferWithSeedInstr())
	f.Add(makeInvalidUTF8TransferWithSeedInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var transferWithSeed SystemInstrTransferWithSeed
		_ = transferWithSeed.UnmarshalWithDecoder(decoder)
	})
}

// Helper functions to create seed data

func makeValidCreateAccountInstr(lamports uint64, space uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(lamports, bin.LE)
	encoder.WriteUint64(space, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // owner
	return buf.Bytes()
}

func makeInvalidCreateAccountInstr() []byte {
	// Truncated instruction
	return []byte{1, 2, 3, 4}
}

func makeValidAssignInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // owner
	return buf.Bytes()
}

func makeInvalidAssignInstr() []byte {
	// Truncated pubkey
	return []byte{1, 2, 3}
}

func makeValidTransferInstr(lamports uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(lamports, bin.LE)
	return buf.Bytes()
}

func makeInvalidTransferInstr() []byte {
	// Truncated uint64
	return []byte{1, 2, 3}
}

func makeValidCreateAccountWithSeedInstr(seed string, lamports uint64, space uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	encoder.WriteRustString(seed)
	encoder.WriteUint64(lamports, bin.LE)
	encoder.WriteUint64(space, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // owner
	return buf.Bytes()
}

func makeInvalidCreateAccountWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	// Invalid string length
	encoder.WriteUint64(0xFFFFFFFFFFFFFFFF, bin.LE)
	return buf.Bytes()
}

func makeInvalidUTF8CreateAccountWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	// Invalid UTF-8 string
	encoder.WriteUint64(3, bin.LE)
	encoder.WriteBytes([]byte{0xFF, 0xFE, 0xFD}, false)
	return buf.Bytes()
}

func makeValidWithdrawNonceInstr(lamports uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(lamports, bin.LE)
	return buf.Bytes()
}

func makeValidInitializeNonceInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // authority
	return buf.Bytes()
}

func makeInvalidInitializeNonceInstr() []byte {
	// Truncated pubkey
	return []byte{1, 2, 3, 4, 5}
}

func makeValidAuthorizeNonceInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // authority
	return buf.Bytes()
}

func makeInvalidAuthorizeNonceInstr() []byte {
	// Truncated pubkey
	return []byte{1, 2, 3, 4, 5}
}

func makeValidAllocateInstr(space uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(space, bin.LE)
	return buf.Bytes()
}

func makeValidAllocateWithSeedInstr(seed string, space uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	encoder.WriteRustString(seed)
	encoder.WriteUint64(space, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // owner
	return buf.Bytes()
}

func makeInvalidAllocateWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	// Invalid string length
	encoder.WriteUint64(0xFFFFFFFFFFFFFFFF, bin.LE)
	return buf.Bytes()
}

func makeInvalidUTF8AllocateWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	// Invalid UTF-8 string
	encoder.WriteUint64(3, bin.LE)
	encoder.WriteBytes([]byte{0xFF, 0xFE, 0xFD}, false)
	return buf.Bytes()
}

func makeValidAssignWithSeedInstr(seed string) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	encoder.WriteRustString(seed)
	encoder.WriteBytes(make([]byte, 32), false) // owner
	return buf.Bytes()
}

func makeInvalidAssignWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	// Invalid string length
	encoder.WriteUint64(0xFFFFFFFFFFFFFFFF, bin.LE)
	return buf.Bytes()
}

func makeInvalidUTF8AssignWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false) // base
	// Invalid UTF-8 string
	encoder.WriteUint64(3, bin.LE)
	encoder.WriteBytes([]byte{0xFF, 0xFE, 0xFD}, false)
	return buf.Bytes()
}

func makeValidTransferWithSeedInstr(lamports uint64, seed string) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(lamports, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // from_base
	encoder.WriteRustString(seed)
	encoder.WriteBytes(make([]byte, 32), false) // from_owner
	return buf.Bytes()
}

func makeInvalidTransferWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(1000000, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // from_base
	// Invalid string length
	encoder.WriteUint64(0xFFFFFFFFFFFFFFFF, bin.LE)
	return buf.Bytes()
}

func makeInvalidUTF8TransferWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(1000000, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // from_base
	// Invalid UTF-8 string
	encoder.WriteUint64(3, bin.LE)
	encoder.WriteBytes([]byte{0xFF, 0xFE, 0xFD}, false)
	return buf.Bytes()
}
