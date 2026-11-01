package sealevel

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	AddrLookupTableInstrTypeCreateLookupTable = iota
	AddrLookupTableInstrTypeFreezeLookupTable
	AddrLookupTableInstrTypeExtendLookupTable
	AddrLookupTableInstrTypeDeactivateLookupTable
	AddrLookupTableInstrTypeCloseLookupTable
)

const LookupTableMaxAddresses = 256

type AddrLookupTableInstrCreateLookupTable struct {
	RecentSlot uint64
	BumpSeed   byte
}

type AddrLookupTableInstrExtendLookupTable struct {
	NewAddresses []solana.PublicKey
}

// account states
const (
	AddressLookupTableProgramStateUninitialized = iota
	AddressLookupTableProgramStateLookupTable
)

type AddressLookupTableStatus struct {
	Status                      uint64
	DeactivatingRemainingBlocks uint64
}

// address lookup table statuses
const (
	AddressLookupTableStatusTypeActivated = iota
	AddressLookupTableStatusTypeDeactivating
	AddressLookupTableStatusTypeDeactivated
)

const AddressLookupTableMetaSize = 56

type LookupTableMeta struct {
	DeactivationSlot           uint64
	LastExtendedSlot           uint64
	LastExtendedSlotStartIndex byte
	Authority                  *solana.PublicKey
	Padding                    uint16
}

type AddressLookupTable struct {
	State     uint32
	Meta      LookupTableMeta
	Addresses []solana.PublicKey
}

func (createLookupTable *AddrLookupTableInstrCreateLookupTable) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	createLookupTable.RecentSlot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	createLookupTable.BumpSeed, err = decoder.ReadByte()
	return err
}

func (createLookupTable *AddrLookupTableInstrCreateLookupTable) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint32(AddrLookupTableInstrTypeCreateLookupTable, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(createLookupTable.RecentSlot, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteByte(createLookupTable.BumpSeed)
	return err
}

func (extendLookupTable *AddrLookupTableInstrExtendLookupTable) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	size, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	for count := uint64(0); count < size; count++ {
		pkBytes, err := decoder.ReadBytes(solana.PublicKeyLength)
		if err != nil {
			return err
		}
		pk := solana.PublicKeyFromBytes(pkBytes)
		extendLookupTable.NewAddresses = append(extendLookupTable.NewAddresses, pk)
		if decoder.Position() > 1232 {
			return InstrErrInvalidInstructionData
		}
	}

	return nil
}

func (extendLookupTable *AddrLookupTableInstrExtendLookupTable) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint32(AddrLookupTableInstrTypeExtendLookupTable, bin.LE)
	if err != nil {
		return err
	}

	addressesLen := uint64(len(extendLookupTable.NewAddresses))
	err = encoder.WriteUint64(addressesLen, bin.LE)
	if err != nil {
		return err
	}

	for _, addr := range extendLookupTable.NewAddresses {
		err = encoder.WriteBytes(addr[:], false)
		if err != nil {
			return err
		}
	}

	return nil
}

func (lookupTableMeta *LookupTableMeta) Status(currentSlot uint64, slotHashes SysvarSlotHashes) AddressLookupTableStatus {
	if lookupTableMeta.DeactivationSlot == math.MaxUint64 {
		return AddressLookupTableStatus{Status: AddressLookupTableStatusTypeActivated}
	} else if lookupTableMeta.DeactivationSlot == currentSlot {
		return AddressLookupTableStatus{Status: AddressLookupTableStatusTypeDeactivating, DeactivatingRemainingBlocks: SlotHashesMaxEntries + 1}
	} else if slotHashPosition, err := slotHashes.Position(lookupTableMeta.DeactivationSlot); err == nil {
		return AddressLookupTableStatus{Status: AddressLookupTableStatusTypeDeactivating, DeactivatingRemainingBlocks: safemath.SaturatingSubU64(SlotHashesMaxEntries, slotHashPosition)}
	} else {
		return AddressLookupTableStatus{Status: AddressLookupTableStatusTypeDeactivated}
	}
}

func (lookupTableMeta *LookupTableMeta) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	lookupTableMeta.DeactivationSlot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	lookupTableMeta.LastExtendedSlot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	lookupTableMeta.LastExtendedSlotStartIndex, err = decoder.ReadByte()
	if err != nil {
		return err
	}

	hasAuthority, err := decoder.ReadBool()
	if err != nil {
		return err
	}

	if hasAuthority {
		authorityBytes, err := decoder.ReadBytes(solana.PublicKeyLength)
		if err != nil {
			return err
		}

		authorityPk := solana.PublicKeyFromBytes(authorityBytes)
		lookupTableMeta.Authority = authorityPk.ToPointer()
	}

	lookupTableMeta.Padding, err = decoder.ReadUint16(bin.LE)
	return err
}

func (lookupTableMeta *LookupTableMeta) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint64(lookupTableMeta.DeactivationSlot, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(lookupTableMeta.LastExtendedSlot, bin.LE)
	if err != nil {
		return err
	}

	err = encoder.WriteByte(lookupTableMeta.LastExtendedSlotStartIndex)
	if err != nil {
		return err
	}

	if lookupTableMeta.Authority != nil {
		err = encoder.WriteBool(true)
		if err != nil {
			return err
		}
		authority := *lookupTableMeta.Authority
		err = encoder.WriteBytes(authority[:], false)
		if err != nil {
			return err
		}
	} else {
		err = encoder.WriteBool(false)
		if err != nil {
			return err
		}
	}

	err = encoder.WriteUint16(lookupTableMeta.Padding, bin.LE)
	return err
}

// Reserve the decoded address slice at its validated length (on by default;
// MITHRIL_ALT_EXACT_CAPACITY=0 restores append growth for comparisons).
var exactAddressLookupTableCapacity = os.Getenv("MITHRIL_ALT_EXACT_CAPACITY") != "0"

func UnmarshalAddressLookupTable(data []byte) (*AddressLookupTable, error) {
	return unmarshalAddressLookupTable(data, exactAddressLookupTableCapacity)
}

func unmarshalAddressLookupTable(data []byte, exactCapacity bool) (*AddressLookupTable, error) {
	addrLookupTable := new(AddressLookupTable)
	decoder := bin.NewBinDecoder(data)

	state, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return nil, InstrErrInvalidAccountData
	}

	if state == AddressLookupTableProgramStateUninitialized {
		return nil, InstrErrUninitializedAccount
	}

	if state != AddressLookupTableProgramStateLookupTable {
		return nil, InstrErrInvalidAccountData
	}

	err = addrLookupTable.Meta.UnmarshalWithDecoder(decoder)
	if err != nil {
		return nil, InstrErrInvalidAccountData
	}

	addrLookupTable.State = AddressLookupTableProgramStateLookupTable

	if len(data) < AddressLookupTableMetaSize {
		return nil, InstrErrInvalidAccountData
	}

	rawAddrData := data[AddressLookupTableMetaSize:]
	rawAddrDataLen := len(rawAddrData)

	if (rawAddrDataLen % solana.PublicKeyLength) != 0 {
		return nil, InstrErrInvalidAccountData
	}

	var addrs []solana.PublicKey
	if exactCapacity && rawAddrDataLen != 0 {
		// The validated wire length gives the exact count. Keep owned storage,
		// but avoid geometrically growing and copying it as keys are decoded.
		// Preserve nil addresses for an empty table.
		addrs = make([]solana.PublicKey, 0, rawAddrDataLen/solana.PublicKeyLength)
	}

	for pos := 0; pos < len(rawAddrData); pos += solana.PublicKeyLength {
		pkBytes := rawAddrData[pos : pos+solana.PublicKeyLength]
		pk := solana.PublicKeyFromBytes(pkBytes)
		addrs = append(addrs, pk)
	}

	addrLookupTable.Addresses = addrs

	return addrLookupTable, nil
}

func marshalAddressLookupTable(addrLookupTable *AddressLookupTable) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buffer)

	err := encoder.WriteUint32(addrLookupTable.State, bin.LE)
	if err != nil {
		return nil, err
	}

	// nothing else to serialize up for an uninitialized account state
	if addrLookupTable.State == AddressLookupTableProgramStateUninitialized {
		return buffer.Bytes(), nil
	}

	err = addrLookupTable.Meta.MarshalWithEncoder(encoder)
	if err != nil {
		return nil, err
	}

	for _, addr := range addrLookupTable.Addresses {
		pkBytes := addr[:]
		err = encoder.WriteBytes(pkBytes, false)
		if err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func AddressLookupTableExecute(execCtx *ExecutionCtx) error {
	err := execCtx.ComputeMeter.Consume(cu.CUAddressLookupTableDefaultComputeUnits)
	if err != nil {
		return InstrErrComputationalBudgetExceeded
	}

	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	instrData := instrCtx.Data

	decoder := bin.NewBinDecoder(instrData)
	instructionType, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return InstrErrInvalidInstructionData
	}

	switch instructionType {
	case AddrLookupTableInstrTypeCreateLookupTable:
		{
			var createLookupTable AddrLookupTableInstrCreateLookupTable
			err = createLookupTable.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = AddressLookupTableCreateLookupTable(execCtx, createLookupTable.RecentSlot, createLookupTable.BumpSeed)
		}

	case AddrLookupTableInstrTypeFreezeLookupTable:
		{
			err = AddressLookupTableFreezeLookupTable(execCtx)
		}

	case AddrLookupTableInstrTypeExtendLookupTable:
		{
			var extend AddrLookupTableInstrExtendLookupTable
			err = extend.UnmarshalWithDecoder(decoder)
			if err != nil {
				return InstrErrInvalidInstructionData
			}

			err = AddressLookupTableExtendLookupTable(execCtx, extend.NewAddresses)
		}

	case AddrLookupTableInstrTypeDeactivateLookupTable:
		{
			err = AddressLookupTableDeactivateLookupTable(execCtx)
		}

	case AddrLookupTableInstrTypeCloseLookupTable:
		{
			err = AddressLookupTableCloseLookupTable(execCtx)
		}

	default:
		{
			err = InstrErrInvalidInstructionData
		}
	}

	return err
}

func setAddrTableLookupAccountState(acct *BorrowedAccount, state *AddressLookupTable, f features.Features) error {
	acctStateBytes, err := marshalAddressLookupTable(state)
	if err != nil {
		return err
	}

	err = acct.SetState(f, acctStateBytes)
	return err
}

func overwriteAddrLookupTableMetadata(acct *BorrowedAccount, lookupTableMeta *LookupTableMeta, f features.Features) error {
	meta, err := acct.DataMutable(f)
	if err != nil {
		return err
	}

	if len(meta) < AddressLookupTableMetaSize {
		return InstrErrInvalidAccountData
	}

	for count := 0; count < AddressLookupTableMetaSize; count++ {
		meta[count] = 0
	}

	writer := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(writer)
	err = lookupTableMeta.MarshalWithEncoder(encoder)
	if err != nil {
		return InstrErrGenericError
	}

	binary.LittleEndian.PutUint32(meta, AddressLookupTableProgramStateLookupTable)
	copy(meta[4:], writer.Bytes())
	return nil
}

func AddressLookupTableCreateLookupTable(execCtx *ExecutionCtx, untrustedRecentSlot uint64, bumpSeed byte) error {
	//mlog.Log.Debugf("AddressLookupTableCreateLookupTable")

	txCtx := execCtx.TransactionContext

	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	lookupTableAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer lookupTableAcct.Drop()

	lookupTableLamports := lookupTableAcct.Lamports()
	tableKey := lookupTableAcct.Key()
	lookupTableOwner := lookupTableAcct.Owner()

	if !execCtx.Features.IsActive(features.RelaxAuthoritySignerCheckForLookupTableCreation) &&
		len(lookupTableAcct.Data()) != 0 {
		return InstrErrAccountAlreadyInitialized
	}

	lookupTableAcct.Drop()

	authorityAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
	if err != nil {
		return err
	}
	defer authorityAcct.Drop()

	authorityKey := authorityAcct.Key()

	if !execCtx.Features.IsActive(features.RelaxAuthoritySignerCheckForLookupTableCreation) &&
		!authorityAcct.IsSigner() {
		return InstrErrMissingRequiredSignature
	}

	authorityAcct.Drop()

	payerAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}
	defer payerAcct.Drop()

	payerKey := payerAcct.Key()

	if !payerAcct.IsSigner() {
		return InstrErrMissingRequiredSignature
	}

	payerAcct.Drop()

	slotHashes, err := ReadSlotHashesSysvar(execCtx)
	if err != nil {
		return err
	}
	_, err = slotHashes.Get(untrustedRecentSlot)
	if err != nil {
		return InstrErrInvalidInstructionData
	}

	derivationSlot := untrustedRecentSlot

	var seeds [][]byte
	seeds = append(seeds, authorityKey[:])
	derivationSlotBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(derivationSlotBytes, derivationSlot)
	seeds = append(seeds, derivationSlotBytes)
	seeds = append(seeds, []byte{bumpSeed})

	derivedTableKey, err := solana.CreateProgramAddress(seeds, a.AddressLookupTableAddr)
	if err != nil {
		return PubkeyErrInvalidSeeds
	}

	if tableKey != derivedTableKey {
		//mlog.Log.Debugf("Table address must match derived address: %s", derivedTableKey)
		return InstrErrInvalidArgument
	}

	if execCtx.Features.IsActive(features.RelaxAuthoritySignerCheckForLookupTableCreation) &&
		lookupTableOwner == a.AddressLookupTableAddr {
		return nil
	}

	tableAcctDataLen := uint64(AddressLookupTableMetaSize)
	rent, err := ReadRentSysvar(execCtx)
	if err != nil {
		return err
	}

	minBalance := rent.MinimumBalance(tableAcctDataLen)
	if minBalance < 1 {
		minBalance = 1
	}
	requiredLamports := safemath.SaturatingSubU64(minBalance, lookupTableLamports)

	if requiredLamports > 0 {
		//mlog.Log.Debugf("calling transfer via native invoke")
		txInstr := newTransferInstruction(payerKey, tableKey, requiredLamports)
		err = execCtx.NativeInvoke(*txInstr, []solana.PublicKey{payerKey})
		if err != nil {
			return err
		}
	}

	//mlog.Log.Debugf("calling allocate via native invoke")
	allocInstr := newAllocateInstruction(tableKey, tableAcctDataLen)
	err = execCtx.NativeInvoke(*allocInstr, []solana.PublicKey{tableKey})
	if err != nil {
		return err
	}

	//mlog.Log.Debugf("calling assign via native invoke")
	assignInstr := newAssignInstruction(tableKey, a.AddressLookupTableAddr)
	err = execCtx.NativeInvoke(*assignInstr, []solana.PublicKey{tableKey})
	if err != nil {
		return err
	}

	lookupTableAcct, err = instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}

	newState := &AddressLookupTable{State: AddressLookupTableProgramStateLookupTable, Meta: LookupTableMeta{Authority: &authorityKey, DeactivationSlot: math.MaxUint64}}
	err = setAddrTableLookupAccountState(lookupTableAcct, newState, execCtx.Features)
	lookupTableAcct.Drop()

	return err
}

func AddressLookupTableFreezeLookupTable(execCtx *ExecutionCtx) error {
	txCtx := execCtx.TransactionContext

	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	lookupTableAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer lookupTableAcct.Drop()

	if lookupTableAcct.Owner() != a.AddressLookupTableAddr {
		return InstrErrInvalidAccountOwner
	}

	lookupTableAcct.Drop()

	authorityAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
	if err != nil {
		return err
	}
	defer authorityAcct.Drop()

	authorityKey := authorityAcct.Key()

	if !authorityAcct.IsSigner() {
		//mlog.Log.Debugf("authority didn't sign")
		return InstrErrMissingRequiredSignature
	}

	authorityAcct.Drop()

	lookupTableAcct, err = instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}

	lookupTableData := lookupTableAcct.Data()
	lookupTable, err := UnmarshalAddressLookupTable(lookupTableData)
	if err != nil {
		return err
	}

	if lookupTable.Meta.Authority == nil {
		//mlog.Log.Debugf("lookup table is already frozen")
		return InstrErrImmutable
	}

	if *lookupTable.Meta.Authority != authorityKey {
		//mlog.Log.Debugf("wrong authority")
		return InstrErrIncorrectAuthority
	}

	if lookupTable.Meta.DeactivationSlot != math.MaxUint64 {
		//mlog.Log.Debugf("Deactivated tables cannot be frozen")
		return InstrErrInvalidArgument
	}

	if len(lookupTable.Addresses) == 0 {
		//mlog.Log.Debugf("Empty lookup tables cannot be frozen")
		return InstrErrInvalidInstructionData
	}

	lookupTable.Meta.Authority = nil
	err = overwriteAddrLookupTableMetadata(lookupTableAcct, &lookupTable.Meta, execCtx.Features)
	lookupTableAcct.Drop()

	return err
}

func AddressLookupTableExtendLookupTable(execCtx *ExecutionCtx, newAddresses []solana.PublicKey) error {
	txCtx := execCtx.TransactionContext

	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	lookupTableAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer lookupTableAcct.Drop()

	tableKey := lookupTableAcct.Key()

	if lookupTableAcct.Owner() != a.AddressLookupTableAddr {
		return InstrErrInvalidAccountOwner
	}

	lookupTableAcct.Drop()

	authorityAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
	if err != nil {
		return err
	}
	defer authorityAcct.Drop()

	authorityKey := authorityAcct.Key()

	if !authorityAcct.IsSigner() {
		//mlog.Log.Debugf("authority didn't sign")
		return InstrErrMissingRequiredSignature
	}

	authorityAcct.Drop()

	lookupTableAcct, err = instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}

	lookupTableData := lookupTableAcct.Data()
	lookupTableLamports := lookupTableAcct.Lamports()
	lookupTable, err := UnmarshalAddressLookupTable(lookupTableData)
	if err != nil {
		return err
	}

	if lookupTable.Meta.Authority == nil {
		return InstrErrImmutable
	}

	if *lookupTable.Meta.Authority != authorityKey {
		//mlog.Log.Debugf("incorrect authority")
		return InstrErrIncorrectAuthority
	}

	if lookupTable.Meta.DeactivationSlot != math.MaxUint64 {
		//mlog.Log.Debugf("Deactivated tables cannot be extended")
		return InstrErrInvalidArgument
	}

	if len(lookupTable.Addresses) >= LookupTableMaxAddresses {
		//mlog.Log.Debugf("Lookup table is full and cannot contain more addresses")
		return InstrErrInvalidArgument
	}

	if len(newAddresses) == 0 {
		//mlog.Log.Debugf("Must extend with at least one address")
		return InstrErrInvalidInstructionData
	}

	newTableAddressesLen := safemath.SaturatingAddU64(uint64(len(lookupTable.Addresses)), uint64(len(newAddresses)))
	if newTableAddressesLen > LookupTableMaxAddresses {
		//mlog.Log.Debugf("Extended lookup table length %d would exceed max capacity of %d", newTableAddressesLen, LookupTableMaxAddresses)
		return InstrErrInvalidInstructionData
	}

	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return err
	}

	if clock.Slot != lookupTable.Meta.LastExtendedSlot {
		lookupTable.Meta.LastExtendedSlot = clock.Slot
		lookupTable.Meta.LastExtendedSlotStartIndex = byte(len(lookupTable.Addresses))
	}

	newTableDataLen := AddressLookupTableMetaSize + (newTableAddressesLen * solana.PublicKeyLength)

	err = overwriteAddrLookupTableMetadata(lookupTableAcct, &lookupTable.Meta, execCtx.Features)
	if err != nil {
		return err
	}

	for _, newAddr := range newAddresses {
		err = lookupTableAcct.ExtendFromSlice(execCtx.Features, newAddr[:])
		if err != nil {
			return err
		}
	}

	lookupTableAcct.Drop()

	rent, err := ReadRentSysvar(execCtx)
	if err != nil {
		return err
	}

	minBalance := rent.MinimumBalance(newTableDataLen)
	if minBalance < 1 {
		minBalance = 1
	}
	requiredLamports := safemath.SaturatingSubU64(minBalance, lookupTableLamports)

	if requiredLamports > 0 {
		payerAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 2)
		if err != nil {
			return err
		}
		defer payerAcct.Drop()

		payerKey := payerAcct.Key()
		if !payerAcct.IsSigner() {
			//mlog.Log.Debugf("payer account must be a signer")
			return InstrErrMissingRequiredSignature
		}
		payerAcct.Drop()

		txIx := newTransferInstruction(payerKey, tableKey, requiredLamports)
		err = execCtx.NativeInvoke(*txIx, []solana.PublicKey{payerKey})
		if err != nil {
			return err
		}
	}

	return nil
}

func AddressLookupTableDeactivateLookupTable(execCtx *ExecutionCtx) error {
	txCtx := execCtx.TransactionContext

	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	lookupTableAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer lookupTableAcct.Drop()

	if lookupTableAcct.Owner() != a.AddressLookupTableAddr {
		return InstrErrInvalidAccountOwner
	}

	lookupTableAcct.Drop()

	authorityAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
	if err != nil {
		return err
	}
	defer authorityAcct.Drop()

	authorityKey := authorityAcct.Key()

	if !authorityAcct.IsSigner() {
		return InstrErrMissingRequiredSignature
	}

	authorityAcct.Drop()

	lookupTableAcct, err = instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}

	lookupTableData := lookupTableAcct.Data()
	lookupTable, err := UnmarshalAddressLookupTable(lookupTableData)
	if err != nil {
		return err
	}

	if lookupTable.Meta.Authority == nil {
		//mlog.Log.Debugf("lookup table is frozen")
		return InstrErrImmutable
	}

	if *lookupTable.Meta.Authority != authorityKey {
		return InstrErrIncorrectAuthority
	}

	if lookupTable.Meta.DeactivationSlot != math.MaxUint64 {
		//mlog.Log.Debugf("Lookup table is already deactivated")
		return InstrErrInvalidArgument
	}

	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return err
	}

	lookupTable.Meta.DeactivationSlot = clock.Slot
	err = overwriteAddrLookupTableMetadata(lookupTableAcct, &lookupTable.Meta, execCtx.Features)
	lookupTableAcct.Drop()

	return err
}

func AddressLookupTableCloseLookupTable(execCtx *ExecutionCtx) error {
	txCtx := execCtx.TransactionContext

	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	lookupTableAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	defer lookupTableAcct.Drop()

	if lookupTableAcct.Owner() != a.AddressLookupTableAddr {
		return InstrErrInvalidAccountOwner
	}

	lookupTableAcct.Drop()

	authorityAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 1)
	if err != nil {
		return err
	}
	defer authorityAcct.Drop()

	authorityKey := authorityAcct.Key()

	if !authorityAcct.IsSigner() {
		//mlog.Log.Debugf("authority did not sign")
		return InstrErrMissingRequiredSignature
	}

	authorityAcct.Drop()

	err = instrCtx.CheckNumOfInstructionAccounts(3)
	if err != nil {
		return err
	}

	idxInTx1, err := instrCtx.IndexOfInstructionAccountInTransaction(0)
	if err != nil {
		return err
	}

	idxInTx2, err := instrCtx.IndexOfInstructionAccountInTransaction(2)
	if err != nil {
		return err
	}

	if idxInTx1 == idxInTx2 {
		//mlog.Log.Debugf("lookup table cannot be the recipient of reclaimed lamports")
		return InstrErrInvalidArgument
	}

	lookupTableAcct, err = instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	withdrawnLamports := lookupTableAcct.Lamports()
	lookupTableData := lookupTableAcct.Data()

	lookupTable, err := UnmarshalAddressLookupTable(lookupTableData)
	if err != nil {
		return err
	}

	if lookupTable.Meta.Authority == nil {
		//mlog.Log.Debugf("lookup table is frozen")
		return InstrErrImmutable
	}

	if *lookupTable.Meta.Authority != authorityKey {
		return InstrErrIncorrectAuthority
	}

	clock, err := ReadClockSysvar(execCtx)
	if err != nil {
		return err
	}

	slotHashes, err := ReadSlotHashesSysvar(execCtx)
	if err != nil {
		return err
	}

	status := lookupTable.Meta.Status(clock.Slot, slotHashes)

	switch status.Status {
	case AddressLookupTableStatusTypeActivated:
		{
			//mlog.Log.Debugf("lookup table is not deactivated")
			return InstrErrInvalidArgument
		}

	case AddressLookupTableStatusTypeDeactivating:
		{
			//mlog.Log.Debugf("table cannot be closed until it's fully deactivated in %d blocks", status.DeactivatingRemainingBlocks)
			return InstrErrInvalidArgument
		}
	}

	lookupTableAcct.Drop()

	recipientAcct, err := instrCtx.BorrowInstructionAccount(txCtx, 2)
	if err != nil {
		return err
	}
	defer recipientAcct.Drop()

	err = recipientAcct.CheckedAddLamports(withdrawnLamports, execCtx.Features)
	if err != nil {
		return err
	}

	recipientAcct.Drop()

	lookupTableAcct, err = instrCtx.BorrowInstructionAccount(txCtx, 0)
	if err != nil {
		return err
	}
	err = lookupTableAcct.SetDataLength(0, execCtx.Features)
	if err != nil {
		return err
	}
	err = lookupTableAcct.SetLamports(0, execCtx.Features)
	lookupTableAcct.Drop()

	return err
}
