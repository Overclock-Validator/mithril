package sealevel

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
)

const SysvarClockAddrStr = "SysvarC1ock11111111111111111111111111111111"

var SysvarClockAddr = base58.MustDecodeFromString(SysvarClockAddrStr)

const SysvarClockStructLen = 40

type SysvarClock struct {
	Slot                uint64
	EpochStartTimestamp int64
	Epoch               uint64
	LeaderScheduleEpoch uint64
	UnixTimestamp       int64
}

func (sc *SysvarClock) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	slot, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read Slot when decoding SysvarClock: %w", err)
	}
	sc.Slot = slot

	epochStartTimestamp, err := decoder.ReadInt64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read EpochStartTimestamp when decoding SysvarClock: %w", err)
	}
	sc.EpochStartTimestamp = epochStartTimestamp

	epoch, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read Epoch when decoding SysvarClock: %w", err)
	}
	sc.Epoch = epoch

	leaderScheduleEpoch, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read LeaderScheduleEpoch when decoding SysvarClock: %w", err)
	}
	sc.LeaderScheduleEpoch = leaderScheduleEpoch

	unixTimestamp, err := decoder.ReadInt64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read UnixTimestamp when decoding SysvarClock: %w", err)
	}
	sc.UnixTimestamp = unixTimestamp
	return
}

func (sc *SysvarClock) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sc.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func (sc *SysvarClock) MustMarshal() []byte {
	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err := enc.WriteUint64(sc.Slot, bin.LE)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal slot in sysvar clock: %s", err))
	}

	err = enc.WriteInt64(sc.EpochStartTimestamp, bin.LE)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal epoch_start_timestamp in sysvar clock: %s", err))
	}

	err = enc.WriteUint64(sc.Epoch, bin.LE)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal epoch in sysvar clock: %s", err))
	}

	err = enc.WriteUint64(sc.LeaderScheduleEpoch, bin.LE)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal leader_schedule_epoch in sysvar clock: %s", err))
	}

	err = enc.WriteInt64(sc.UnixTimestamp, bin.LE)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal epoch_start_timestamp in sysvar clock: %s", err))
	}

	return data.Bytes()
}

func ReadClockSysvar(execCtx *ExecutionCtx) (SysvarClock, error) {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			clock, ok := bankSysvars.Clock()
			if !ok {
				return SysvarClock{}, InstrErrUnsupportedSysvar
			}
			return clock, nil
		}
	}

	if clockAccount, ok := localSysvarAccount(execCtx, SysvarClockAddr); ok {
		if clockAccount.Lamports == 0 {
			return SysvarClock{}, InstrErrUnsupportedSysvar
		}
		var clock SysvarClock
		if err := clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockAccount.Data)); err != nil {
			return SysvarClock{}, InstrErrUnsupportedSysvar
		}
		return clock, nil
	}

	// The global cache remains the fallback for execution contexts that do not
	// carry a bank-local Clock (primarily isolated native-program tests). A
	// present bank account is authoritative even when the bank is a speculative
	// leader bank rather than an ordered-replay bank.
	if SysvarCache.Clock.Sysvar != nil {
		return *SysvarCache.Clock.Sysvar, nil
	}
	return SysvarClock{}, InstrErrUnsupportedSysvar
}

func WriteClockSysvar(accts *accounts.Accounts, clock SysvarClock) {

	clockAccount, err := (*accts).GetAccount(&SysvarClockAddr)
	if err != nil {
		panic("failed to read Clock sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err = enc.WriteUint64(clock.Slot, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize Slot for clock sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteInt64(clock.EpochStartTimestamp, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize EpochStartTimestamp for clock sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteUint64(clock.Epoch, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize Epoch for clock sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteUint64(clock.LeaderScheduleEpoch, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize LeaderScheduleEpoch for clock sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteInt64(clock.UnixTimestamp, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize UnixTimestamp for clock sysvar: %w", err)
		panic(err)
	}

	clockAccount.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarClockAddr, clockAccount)
	if err != nil {
		err = fmt.Errorf("failed write newly serialized clock sysvar to sysvar account: %w", err)
		panic(err)
	}
}

func checkAcctForClockSysvar(txCtx *TransactionCtx, instrCtx *InstructionCtx, instrAcctIdx uint64) error {
	idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(instrAcctIdx)
	if err != nil {
		return err
	}
	pk, err := txCtx.KeyOfAccountAtIndex(idxInTx)
	if err != nil {
		return err
	}
	if pk == SysvarClockAddr {
		return nil
	} else {
		return InstrErrInvalidArgument
	}
}
