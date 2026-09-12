package sealevel

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	bin "github.com/gagliardetto/binary"
)

const SysvarEpochScheduleAddrStr = "SysvarEpochSchedu1e111111111111111111111111"

var SysvarEpochScheduleAddr = base58.MustDecodeFromString(SysvarEpochScheduleAddrStr)

const SysvarEpochScheduleStructLen = 40

const MinimumSlotsPerEpoch = 32

type SysvarEpochSchedule struct {
	SlotsPerEpoch            uint64
	LeaderScheduleSlotOffset uint64
	Warmup                   bool
	FirstNormalEpoch         uint64
	FirstNormalSlot          uint64
}

func (ses *SysvarEpochSchedule) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	slotsPerEpoch, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read SlotsPerEpoch when decoding SysvarEpochSchedule: %w", err)
	}
	ses.SlotsPerEpoch = slotsPerEpoch

	leaderScheduleSlotOffset, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read LeaderScheduleSlotOffset when decoding SysvarEpochSchedule: %w", err)
	}
	ses.LeaderScheduleSlotOffset = leaderScheduleSlotOffset

	warmup, err := ReadBool(decoder)
	if err != nil {
		return fmt.Errorf("failed to read Warmup when decoding SysvarEpochSchedule: %w", err)
	}
	ses.Warmup = warmup

	firstNormalEpoch, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read FirstNormalEpoch when decoding SysvarEpochSchedule: %w", err)
	}
	ses.FirstNormalEpoch = firstNormalEpoch

	firstNormalSlot, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read FirstNormalSlot when decoding SysvarEpochSchedule: %w", err)
	}
	ses.FirstNormalSlot = firstNormalSlot

	return
}

func (sr *SysvarEpochSchedule) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sr.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func (sr *SysvarEpochSchedule) GetEpoch(slot uint64) uint64 {
	epoch, _ := sr.GetEpochAndSlotIndex(slot)
	return epoch
}

func (sr *SysvarEpochSchedule) SlotsInEpoch(epoch uint64) uint64 {
	if epoch < sr.FirstNormalEpoch {
		return safemath.SaturatingPow(2, uint32(epoch)+uint32(bits.TrailingZeros64(MinimumSlotsPerEpoch)))
	} else {
		return sr.SlotsPerEpoch
	}
}

func (sr *SysvarEpochSchedule) Slot0(epoch uint64) uint64 {
	if epoch < sr.FirstNormalEpoch {
		var power uint64
		if epoch < 64 {
			power = 1 << epoch
		} else {
			power = math.MaxUint64
		}
		return safemath.SaturatingMulU64(power-1, MinimumSlotsPerEpoch)
	}

	nEpoch := epoch - sr.FirstNormalEpoch
	nSlot := nEpoch * sr.SlotsPerEpoch

	return sr.FirstNormalSlot + nSlot

}

func (sr *SysvarEpochSchedule) GetEpochAndSlotIndex(slot uint64) (uint64, uint64) {
	if slot < sr.FirstNormalSlot {
		nextPowerOfTwo := func(n uint64) uint64 {
			if n <= 1 {
				return 1
			}
			shift := bits.Len64(n - 1)
			if shift >= 64 {
				return math.MaxUint64
			}
			return 1 << uint(shift)
		}

		nextPower := nextPowerOfTwo(safemath.SaturatingAddU64(slot, MinimumSlotsPerEpoch+1))
		minEpochLenTrailingZeros := uint64(bits.TrailingZeros64(MinimumSlotsPerEpoch))
		epoch := safemath.SaturatingSubU64(
			safemath.SaturatingSubU64(uint64(bits.TrailingZeros64(nextPower)), minEpochLenTrailingZeros),
			1,
		)
		epochLen := safemath.SaturatingPow(2, uint32(epoch)+uint32(minEpochLenTrailingZeros))
		return epoch, safemath.SaturatingSubU64(slot, safemath.SaturatingSubU64(epochLen, MinimumSlotsPerEpoch))
	} else {
		normalSlotIndex := slot - sr.FirstNormalSlot
		normalEpochIndex := normalSlotIndex / sr.SlotsPerEpoch
		epoch := sr.FirstNormalEpoch + normalEpochIndex
		slotIndex := normalSlotIndex % sr.SlotsPerEpoch
		return epoch, slotIndex
	}
}

func (sr *SysvarEpochSchedule) FirstSlotInEpoch(epoch uint64) uint64 {
	if epoch <= sr.FirstNormalEpoch {
		return (safemath.SaturatingPow(2, uint32(epoch)) - 1) * MinimumSlotsPerEpoch
	} else {
		return safemath.SaturatingAddU64(safemath.SaturatingMulU64(safemath.SaturatingSubU64(epoch, sr.FirstNormalEpoch), sr.SlotsPerEpoch), sr.FirstNormalSlot)
	}
}

func (sr *SysvarEpochSchedule) LeaderScheduleEpoch(slot uint64) uint64 {
	if slot < sr.FirstNormalSlot {
		e, _ := sr.GetEpochAndSlotIndex(slot)
		return safemath.SaturatingAddU64(e, 1)
	} else {
		newSlotsSinceFirstNormalSlot := safemath.SaturatingSubU64(slot, sr.FirstNormalSlot)
		newFirstNormalLeaderScheduleSlot := safemath.SaturatingAddU64(newSlotsSinceFirstNormalSlot, sr.LeaderScheduleSlotOffset)
		var newEpochsSinceFirstNormalLeaderSchedule uint64
		if sr.SlotsPerEpoch == 0 {
			newEpochsSinceFirstNormalLeaderSchedule = 0
		} else {
			newEpochsSinceFirstNormalLeaderSchedule = newFirstNormalLeaderScheduleSlot / sr.SlotsPerEpoch
		}

		return safemath.SaturatingAddU64(sr.FirstNormalEpoch, newEpochsSinceFirstNormalLeaderSchedule)
	}
}

func ReadEpochScheduleSysvar(execCtx *ExecutionCtx) (SysvarEpochSchedule, error) {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			epochSchedule, ok := bankSysvars.EpochSchedule()
			if !ok {
				return SysvarEpochSchedule{}, InstrErrUnsupportedSysvar
			}
			return epochSchedule, nil
		}
	}

	if epochScheduleAcct, ok := localSysvarAccount(execCtx, SysvarEpochScheduleAddr); ok {
		if epochScheduleAcct.Lamports == 0 {
			return SysvarEpochSchedule{}, InstrErrUnsupportedSysvar
		}
		var epochSchedule SysvarEpochSchedule
		if err := epochSchedule.UnmarshalWithDecoder(bin.NewBinDecoder(epochScheduleAcct.Data)); err != nil {
			return SysvarEpochSchedule{}, InstrErrUnsupportedSysvar
		}
		return epochSchedule, nil
	}

	if SysvarCache.EpochSchedule.Sysvar != nil {
		return *SysvarCache.EpochSchedule.Sysvar, nil
	}
	return SysvarEpochSchedule{}, InstrErrUnsupportedSysvar
}

func WriteEpochScheduleSysvar(accts *accounts.Accounts, epochSchedule SysvarEpochSchedule) {

	epochScheduleSysvarAcct, err := (*accts).GetAccount(&SysvarEpochScheduleAddr)
	if err != nil {
		panic("failed to read EpochSchedule sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err = enc.WriteUint64(epochSchedule.SlotsPerEpoch, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize SlotsPerEpoch for EpochSchedule sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteUint64(epochSchedule.LeaderScheduleSlotOffset, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize LeaderScheduleSlotOffset for EpochSchedule sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteBool(epochSchedule.Warmup)
	if err != nil {
		err = fmt.Errorf("failed to serialize Warmup for EpochSchedule sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteUint64(epochSchedule.FirstNormalEpoch, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize FirstNormalEpoch for EpochSchedule sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteUint64(epochSchedule.FirstNormalSlot, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize FirstNormalSlot for EpochSchedule sysvar: %w", err)
		panic(err)
	}

	epochScheduleSysvarAcct.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarEpochScheduleAddr, epochScheduleSysvarAcct)
	if err != nil {
		err = fmt.Errorf("failed write newly serialized EpochSchedule sysvar to sysvar account: %w", err)
		panic(err)
	}
}
