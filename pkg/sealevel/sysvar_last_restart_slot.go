package sealevel

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
)

const SysvarLastRestartSlotAddrStr = "SysvarLastRestartS1ot1111111111111111111111"

var SysvarLastRestartSlotAddr = base58.MustDecodeFromString(SysvarLastRestartSlotAddrStr)

const SysvarLastRestartSlotStructLen = 8

type SysvarLastRestartSlot struct {
	LastRestartSlot uint64
}

func (lrs *SysvarLastRestartSlot) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	lastRestartSlot, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read LastRestartSlot when decoding SysvarLastRestartSlot: %w", err)
	}
	lrs.LastRestartSlot = lastRestartSlot
	return
}

func (sr *SysvarLastRestartSlot) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sr.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func ReadLastRestartSlotSysvar(execCtx *ExecutionCtx) (SysvarLastRestartSlot, error) {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			lastRestartSlot, ok := bankSysvars.LastRestartSlot()
			if !ok {
				return SysvarLastRestartSlot{}, InstrErrUnsupportedSysvar
			}
			return lastRestartSlot, nil
		}
	}

	if lrsAcct, ok := localSysvarAccount(execCtx, SysvarLastRestartSlotAddr); ok {
		if lrsAcct.Lamports == 0 {
			return SysvarLastRestartSlot{}, InstrErrUnsupportedSysvar
		}
		var lrs SysvarLastRestartSlot
		if err := lrs.UnmarshalWithDecoder(bin.NewBinDecoder(lrsAcct.Data)); err != nil {
			return SysvarLastRestartSlot{}, InstrErrUnsupportedSysvar
		}
		return lrs, nil
	}

	if SysvarCache.LastRestartSlot.Sysvar != nil {
		return *SysvarCache.LastRestartSlot.Sysvar, nil
	}
	return SysvarLastRestartSlot{}, InstrErrUnsupportedSysvar
}

func WriteLastRestartSlotSysvar(accts *accounts.Accounts, lastRestartSlot SysvarLastRestartSlot) {

	lrsSysvarAcct, err := (*accts).GetAccount(&SysvarLastRestartSlotAddr)
	if err != nil {
		panic("failed to read LastRestartSlot sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err = enc.WriteUint64(lastRestartSlot.LastRestartSlot, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize LastRestartSlot for LastRestartSlot sysvar: %w", err)
		panic(err)
	}

	lrsSysvarAcct.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarLastRestartSlotAddr, lrsSysvarAcct)
	if err != nil {
		err = fmt.Errorf("failed write newly serialized LastRestartSlot sysvar to sysvar account: %w", err)
		panic(err)
	}
}
