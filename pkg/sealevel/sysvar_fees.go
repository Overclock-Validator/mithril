package sealevel

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
)

const SysvarFeesAddrStr = "SysvarFees111111111111111111111111111111111"

var SysvarFeesAddr = base58.MustDecodeFromString(SysvarFeesAddrStr)

const SysvarFeesStructLen = 8

type FeeCalculator struct {
	LamportsPerSignature uint64
}
type SysvarFees struct {
	FeeCalculator FeeCalculator
}

func (sf *SysvarFees) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	lamportsPerSignature, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read LamportsPerSignature when decoding SysvarFees: %w", err)
	}
	sf.FeeCalculator.LamportsPerSignature = lamportsPerSignature
	return
}

func (sf *SysvarFees) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sf.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func (sf *SysvarFees) Update(lamportsPerSignature uint64) {
	sf.FeeCalculator.LamportsPerSignature = lamportsPerSignature
}

func ReadFeesSysvar(execCtx *ExecutionCtx) SysvarFees {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			fees, ok := bankSysvars.Fees()
			if !ok {
				panic("Fees sysvar is absent from bank snapshot")
			}
			return fees
		}
	}

	if feesAcct, ok := localSysvarAccount(execCtx, SysvarFeesAddr); ok {
		if feesAcct.Lamports == 0 {
			panic("Fees sysvar account is absent")
		}
		var fees SysvarFees
		fees.MustUnmarshalWithDecoder(bin.NewBinDecoder(feesAcct.Data))
		return fees
	}

	if SysvarCache.Fees.Sysvar != nil {
		return *SysvarCache.Fees.Sysvar
	}
	panic("failed to read fees sysvar account")
}

func WriteFeesSysvar(accts *accounts.Accounts, fees SysvarFees) {
	feesSysvarAcct, err := (*accts).GetAccount(&SysvarFeesAddr)
	if err != nil {
		panic("failed to read Fees sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err = enc.WriteUint64(fees.FeeCalculator.LamportsPerSignature, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize LamportsPerSignature for Fees sysvar: %w", err)
		panic(err)
	}

	feesSysvarAcct.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarFeesAddr, feesSysvarAcct)
	if err != nil {
		err = fmt.Errorf("failed write newly serialized Fees sysvar to sysvar account: %w", err)
		panic(err)
	}
}
