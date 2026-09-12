package sealevel

import (
	"bytes"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	bin "github.com/gagliardetto/binary"
)

const SysvarRentAddrStr = "SysvarRent111111111111111111111111111111111"

var SysvarRentAddr = base58.MustDecodeFromString(SysvarRentAddrStr)

const SysvarRentStructLen = 24

const rentAccountStorageOverhead = 128

type SysvarRent struct {
	LamportsPerUint8Year uint64
	ExemptionThreshold   float64
	BurnPercent          byte
}

func (sr *SysvarRent) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	lamportsPerUint8Year, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read LamportsPerUint8Year when decoding SysvarRent: %w", err)
	}
	sr.LamportsPerUint8Year = lamportsPerUint8Year

	exemptionThreshold, err := decoder.ReadFloat64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read ExemptionThreshold when decoding SysvarRent: %w", err)
	}
	sr.ExemptionThreshold = exemptionThreshold

	burnPercent, err := decoder.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read BurnPercent when decoding SysvarRent: %w", err)
	}
	sr.BurnPercent = burnPercent

	return
}

func (sr *SysvarRent) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sr.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func (sr *SysvarRent) MustMarshal() []byte {
	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err := enc.WriteUint64(sr.LamportsPerUint8Year, bin.LE)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal LamportsPerUint8Year in sysvar rent: %s", err))
	}

	err = enc.WriteFloat64(sr.ExemptionThreshold, bin.LE)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal ExemptionThreshold in sysvar rent: %s", err))
	}

	err = enc.WriteByte(sr.BurnPercent)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal BurnPercent in sysvar rent: %s", err))
	}

	return data.Bytes()
}

func (sr *SysvarRent) MinimumBalance(dataLen uint64) uint64 {
	dataLenWithOverhead := safemath.SaturatingAddU64(dataLen, rentAccountStorageOverhead)
	lamportsPerYear := safemath.SaturatingMulU64(dataLenWithOverhead, sr.LamportsPerUint8Year)
	min := float64(lamportsPerYear) * sr.ExemptionThreshold

	// saturate at MaxUint64 if the result exceeds this numerical boundary
	if min > float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(min)
}

func (sr *SysvarRent) IsExempt(balance uint64, dataLen uint64) bool {
	return balance >= sr.MinimumBalance(dataLen)
}

func (sr *SysvarRent) InitializeDefault() {
	*sr = NewDefaultRentSysvar()
}

func ReadRentSysvar(execCtx *ExecutionCtx) (SysvarRent, error) {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			rent, ok := bankSysvars.Rent()
			if !ok {
				return SysvarRent{}, InstrErrUnsupportedSysvar
			}
			return rent, nil
		}
	}

	if rentAcct, ok := localSysvarAccount(execCtx, SysvarRentAddr); ok {
		if rentAcct.Lamports == 0 {
			return SysvarRent{}, InstrErrUnsupportedSysvar
		}
		var rent SysvarRent
		if err := rent.UnmarshalWithDecoder(bin.NewBinDecoder(rentAcct.Data)); err != nil {
			return SysvarRent{}, InstrErrUnsupportedSysvar
		}
		return rent, nil
	}

	if SysvarCache.Rent.Sysvar != nil {
		return *SysvarCache.Rent.Sysvar, nil
	}
	return SysvarRent{}, InstrErrUnsupportedSysvar
}

func WriteRentSysvar(accts *accounts.Accounts, rent SysvarRent) {

	rentSysvarAcct, err := (*accts).GetAccount(&SysvarRentAddr)
	if err != nil {
		panic("failed to read Rent sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err = enc.WriteUint64(rent.LamportsPerUint8Year, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize LamportsPerUint8Year for Rent sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteFloat64(rent.ExemptionThreshold, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize ExemptionThreshold for Rent sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteByte(rent.BurnPercent)
	if err != nil {
		err = fmt.Errorf("failed to serialize BurnPercent for Rent sysvar: %w", err)
		panic(err)
	}

	rentSysvarAcct.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarRentAddr, rentSysvarAcct)
	if err != nil {
		err = fmt.Errorf("failed write newly serialized Rent sysvar to sysvar account: %w", err)
		panic(err)
	}
}

func NewDefaultRentSysvar() SysvarRent {
	rent := SysvarRent{LamportsPerUint8Year: 3480, ExemptionThreshold: 2.0, BurnPercent: 50}
	return rent
}

func checkAcctForRentSysvar(txCtx *TransactionCtx, instrCtx *InstructionCtx, instrAcctIdx uint64) error {
	idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(instrAcctIdx)
	if err != nil {
		return err
	}
	pk, err := txCtx.KeyOfAccountAtIndex(idxInTx)
	if err != nil {
		return err
	}
	if pk == SysvarRentAddr {
		return nil
	} else {
		return InstrErrInvalidArgument
	}
}
