package sealevel

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
)

const SysvarSlotHashesAddrStr = "SysvarS1otHashes111111111111111111111111111"

var SysvarSlotHashesAddr = base58.MustDecodeFromString(SysvarSlotHashesAddrStr)

const SlotHashesMaxEntries = 512

type SlotHash struct {
	Slot uint64
	Hash [32]byte
}

type SysvarSlotHashes []SlotHash

func (sh *SysvarSlotHashes) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	hashesLen, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read length of SlotHashes vec when decoding SysvarSlotHashes: %w", err)
	}

	slotHashes := SysvarSlotHashes{}

	for count := uint64(0); count < hashesLen; count++ {
		slot, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			return fmt.Errorf("%d: failed to read Slot when decoding a SlotHash in SysvarSlotHashes: %w", count, err)
		}
		hash, err := decoder.ReadBytes(32)
		if err != nil {
			return fmt.Errorf("failed to read Hash when decoding a SlotHash in SysvarSlotHashes: %w", err)
		}
		slotHash := SlotHash{}
		slotHash.Slot = slot
		copy(slotHash.Hash[:], hash)

		slotHashes = append(slotHashes, slotHash)
	}

	*sh = slotHashes

	return
}

func (sh *SysvarSlotHashes) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sh.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func (sh *SysvarSlotHashes) MustMarshal() []byte {
	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	numSlotHashes := len(*sh)

	err := enc.WriteUint64(uint64(numSlotHashes), bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize len of SlotHashes for SlotHashes sysvar: %w", err)
		panic(err)
	}

	for _, slotHashEntry := range *sh {
		err = enc.WriteUint64(slotHashEntry.Slot, bin.LE)
		if err != nil {
			err = fmt.Errorf("failed to serialize Slot for SlotHashes sysvar: %w", err)
			panic(err)
		}

		err = enc.WriteBytes(slotHashEntry.Hash[:], false)
		if err != nil {
			err = fmt.Errorf("failed to serialize Hash for SlotHashes sysvar: %w", err)
			panic(err)
		}
	}

	return data.Bytes()
}

func (sh *SysvarSlotHashes) Get(slot uint64) ([32]byte, error) {
	for _, slotHash := range *sh {
		if slotHash.Slot == slot {
			return slotHash.Hash, nil
		}
	}
	return [32]byte{}, fmt.Errorf("slothash not found")
}

func (sh *SysvarSlotHashes) Position(slot uint64) (uint64, error) {
	for idx, slotHash := range *sh {
		if slotHash.Slot == slot {
			return uint64(idx), nil
		}
	}

	return 0, fmt.Errorf("not found")
}

func (sh *SysvarSlotHashes) UpdateWithSlotCtx(slotCtx *SlotCtx) {
	var found bool

	for count := 0; count < len(*sh); count++ {
		if (*sh)[count].Slot == slotCtx.Slot {
			(*sh)[count].Hash = slotCtx.SlotBank.BanksHash
			found = true
		}
	}

	if !found {
		slotHashEntry := SlotHash{Hash: slotCtx.SlotBank.BanksHash, Slot: slotCtx.SlotBank.PreviousSlot}
		*sh = append(*sh, slotHashEntry)
	}
}

func (sh *SysvarSlotHashes) Update(slot uint64, parentSlot uint64, hash [32]byte) {
	var found bool

	for count := 0; count < len(*sh); count++ {
		if (*sh)[count].Slot == slot {
			(*sh)[count].Hash = hash
			found = true
		}
	}

	if !found {
		slotHashEntry := SlotHash{Hash: hash, Slot: parentSlot}
		if len(*sh) == SlotHashesMaxEntries {
			*sh = (*sh)[:len(*sh)-1]
		}
		*sh = append([]SlotHash{slotHashEntry}, (*sh)...)
	}
}

func ReadSlotHashesSysvar(execCtx *ExecutionCtx) (SysvarSlotHashes, error) {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			slotHashes, ok := bankSysvars.SlotHashes()
			if !ok {
				return SysvarSlotHashes{}, InstrErrUnsupportedSysvar
			}
			return slotHashes, nil
		}
	}

	if slotHashesAcct, ok := localSysvarAccount(execCtx, SysvarSlotHashesAddr); ok {
		if slotHashesAcct.Lamports == 0 {
			return SysvarSlotHashes{}, InstrErrUnsupportedSysvar
		}
		var slotHashes SysvarSlotHashes
		if err := slotHashes.UnmarshalWithDecoder(bin.NewBinDecoder(slotHashesAcct.Data)); err != nil {
			return SysvarSlotHashes{}, InstrErrUnsupportedSysvar
		}
		return slotHashes, nil
	}

	if SysvarCache.SlotHashes.Sysvar != nil {
		return *SysvarCache.SlotHashes.Sysvar, nil
	}
	return SysvarSlotHashes{}, InstrErrUnsupportedSysvar
}

func WriteSlotHashesSysvar(accts *accounts.Accounts, slotHashes SysvarSlotHashes) {

	slotHashesSysvarAcct, err := (*accts).GetAccount(&SysvarSlotHashesAddr)
	if err != nil {
		panic("failed to read EpochRewards sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	numSlotHashes := len(slotHashes)

	err = enc.WriteUint64(uint64(numSlotHashes), bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize len of SlotHashes for SlotHashes sysvar: %w", err)
		panic(err)
	}

	for count := 0; count < numSlotHashes; count++ {
		err = enc.WriteUint64(slotHashes[count].Slot, bin.LE)
		if err != nil {
			err = fmt.Errorf("failed to serialize Slot for SlotHashes sysvar: %w", err)
			panic(err)
		}

		enc.WriteBytes(slotHashes[count].Hash[:], false)
	}

	slotHashesSysvarAcct.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarSlotHashesAddr, slotHashesSysvarAcct)
	if err != nil {
		err = fmt.Errorf("failed to write newly serialized SlotHashes sysvar to sysvar account: %w", err)
		panic(err)
	}
}

func (slotHashes *SysvarSlotHashes) FromInstrAcct(execCtx *ExecutionCtx, instrAcctIdx uint64) error {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	sysvarAcct, err := instrCtx.BorrowInstructionAccount(txCtx, instrAcctIdx)
	if err != nil {
		return err
	}

	if sysvarAcct.Key() != SysvarSlotHashesAddr {
		return InstrErrInvalidArgument
	}

	acct, _ := execCtx.Accounts.GetAccount(&SysvarSlotHashesAddr)
	decoder := bin.NewBinDecoder(acct.Data)
	err = slotHashes.UnmarshalWithDecoder(decoder)
	if err != nil {
		return InstrErrUnsupportedSysvar
	}

	return nil
}

func checkAcctForSlotHashesSysvar(txCtx *TransactionCtx, instrCtx *InstructionCtx, instrAcctIdx uint64) error {
	idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(instrAcctIdx)
	if err != nil {
		return err
	}
	pk, err := txCtx.KeyOfAccountAtIndex(idxInTx)
	if err != nil {
		return err
	}
	if pk == SysvarSlotHashesAddr {
		return nil
	} else {
		return InstrErrInvalidArgument
	}
}
