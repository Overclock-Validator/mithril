package sealevel

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/cu"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/gagliardetto/solana-go"
)

// SyscallGetClockSysvarImpl is an implementation of the sol_get_clock_sysvar syscall
func SyscallGetClockSysvarImpl(vm sbpf.VM, addr uint64) (uint64, error) {
	execCtx := executionCtx(vm)

	cost := uint64(cu.CUSyscallBaseCost + SysvarClockStructLen)
	err := execCtx.ComputeMeter.Consume(cost)
	if err != nil {
		return syscallCuErr()
	}
	if !syscallAddressIsAligned(execCtx, addr, 8) {
		return syscallErrCustom("SyscallError::UnalignedPointer")
	}
	if syscallParameterAddressRestricted(execCtx, addr) {
		return syscallErrCustom("SyscallError::InvalidPointer")
	}

	var clockDst []byte
	clockDst, err = vm.Translate(addr, SysvarClockStructLen, true)
	if err != nil {
		return syscallErr(err)
	}

	var clock SysvarClock
	clock, err = ReadClockSysvar(execCtx)
	if err != nil {
		return syscallErr(err)
	}

	binary.LittleEndian.PutUint64(clockDst[:8], clock.Slot)
	binary.LittleEndian.PutUint64(clockDst[8:16], uint64(clock.EpochStartTimestamp))
	binary.LittleEndian.PutUint64(clockDst[16:24], clock.Epoch)
	binary.LittleEndian.PutUint64(clockDst[24:32], clock.LeaderScheduleEpoch)
	binary.LittleEndian.PutUint64(clockDst[32:40], uint64(clock.UnixTimestamp))

	return syscallSuccess(0)
}

var SyscallGetClockSysvar = sbpf.SyscallFunc1(SyscallGetClockSysvarImpl)

// SyscallGetRentSysvarImpl is an implementation of the sol_get_rent_sysvar syscall
func SyscallGetRentSysvarImpl(vm sbpf.VM, addr uint64) (uint64, error) {
	execCtx := executionCtx(vm)

	cost := uint64(cu.CUSyscallBaseCost + SysvarRentStructLen)
	err := execCtx.ComputeMeter.Consume(cost)
	if err != nil {
		return syscallCuErr()
	}
	if !syscallAddressIsAligned(execCtx, addr, 8) {
		return syscallErrCustom("SyscallError::UnalignedPointer")
	}
	if syscallParameterAddressRestricted(execCtx, addr) {
		return syscallErrCustom("SyscallError::InvalidPointer")
	}

	rentDst, err := vm.Translate(addr, SysvarRentStructLen, true)
	if err != nil {
		return syscallErr(err)
	}

	rent, err := ReadRentSysvar(execCtx)
	if err != nil {
		return syscallErr(err)
	}

	binary.LittleEndian.PutUint64(rentDst[0:8], rent.LamportsPerUint8Year)
	binary.LittleEndian.PutUint64(rentDst[8:16], math.Float64bits(rent.ExemptionThreshold))
	rentDst[16] = rent.BurnPercent

	return syscallSuccess(0)
}

var SyscallGetRentSysvar = sbpf.SyscallFunc1(SyscallGetRentSysvarImpl)

// SyscallGetEpochScheduleSysvarImpl is an implementation of the sol_get_epoch_schedule_sysvar syscall
func SyscallGetEpochScheduleSysvarImpl(vm sbpf.VM, addr uint64) (uint64, error) {
	execCtx := executionCtx(vm)

	cost := uint64(cu.CUSyscallBaseCost + SysvarEpochScheduleStructLen)
	err := execCtx.ComputeMeter.Consume(cost)
	if err != nil {
		return syscallCuErr()
	}
	if !syscallAddressIsAligned(execCtx, addr, 8) {
		return syscallErrCustom("SyscallError::UnalignedPointer")
	}
	if syscallParameterAddressRestricted(execCtx, addr) {
		return syscallErrCustom("SyscallError::InvalidPointer")
	}

	epochScheduleDst, err := vm.Translate(addr, SysvarEpochScheduleStructLen, true)
	if err != nil {
		return syscallErr(err)
	}

	epochSchedule, err := ReadEpochScheduleSysvar(execCtx)
	if err != nil {
		return syscallErr(err)
	}

	epochScheduleBytes := marshalEpochScheduleSysvar(epochSchedule)
	copy(epochScheduleDst, epochScheduleBytes[:])

	return syscallSuccess(0)
}

var SyscallGetEpochScheduleSysvar = sbpf.SyscallFunc1(SyscallGetEpochScheduleSysvarImpl)

func marshalEpochScheduleSysvar(epochSchedule SysvarEpochSchedule) [SysvarEpochScheduleStructLen]byte {
	var epochScheduleBytes [SysvarEpochScheduleStructLen]byte

	binary.LittleEndian.PutUint64(epochScheduleBytes[0:8], epochSchedule.SlotsPerEpoch)
	binary.LittleEndian.PutUint64(epochScheduleBytes[8:16], epochSchedule.LeaderScheduleSlotOffset)
	if epochSchedule.Warmup {
		epochScheduleBytes[16] = 1
	}

	// sol_get_epoch_schedule_sysvar needs to return a representation consistent with Rust's
	// native `repr(C)` layout, which includes 7 bytes of padding after the bool, before the
	// next u64 field
	binary.LittleEndian.PutUint64(epochScheduleBytes[24:32], epochSchedule.FirstNormalEpoch)
	binary.LittleEndian.PutUint64(epochScheduleBytes[32:40], epochSchedule.FirstNormalSlot)

	return epochScheduleBytes
}

// SyscallGetEpochRewardsSysvarImpl is an implementation of the sol_get_epoch_rewards_sysvar syscall
func SyscallGetEpochRewardsSysvarImpl(vm sbpf.VM, addr uint64) (uint64, error) {
	execCtx := executionCtx(vm)

	cost := uint64(cu.CUSyscallBaseCost + SysvarEpochRewardsStructLen)
	err := execCtx.ComputeMeter.Consume(cost)
	if err != nil {
		return syscallCuErr()
	}
	if !syscallAddressIsAligned(execCtx, addr, 16) {
		return syscallErrCustom("SyscallError::UnalignedPointer")
	}
	if syscallParameterAddressRestricted(execCtx, addr) {
		return syscallErrCustom("SyscallError::InvalidPointer")
	}

	epochRewardsDst, err := vm.Translate(addr, SysvarEpochRewardsStructLen, true)
	if err != nil {
		return syscallErr(err)
	}

	epochRewards, err := ReadEpochRewardsSysvar(execCtx)
	if err != nil {
		return syscallErr(err)
	}

	binary.LittleEndian.PutUint64(epochRewardsDst[:8], epochRewards.DistributionStartingBlockHeight)
	binary.LittleEndian.PutUint64(epochRewardsDst[8:16], epochRewards.NumPartitions)
	copy(epochRewardsDst[16:48], epochRewards.ParentBlockhash[:])

	binary.LittleEndian.PutUint64(epochRewardsDst[48:56], epochRewards.TotalPoints.Lo)
	binary.LittleEndian.PutUint64(epochRewardsDst[56:64], epochRewards.TotalPoints.Hi)
	util.ReverseBytesInPlace(epochRewardsDst[48:64])

	binary.LittleEndian.PutUint64(epochRewardsDst[64:72], epochRewards.TotalRewards)
	binary.LittleEndian.PutUint64(epochRewardsDst[72:80], epochRewards.DistributedRewards)

	if epochRewards.Active {
		epochRewardsDst[80] = 1
	} else {
		epochRewardsDst[80] = 0
	}

	return syscallSuccess(0)
}

var SyscallGetEpochRewardsSysvar = sbpf.SyscallFunc1(SyscallGetEpochRewardsSysvarImpl)

// SyscallGetLastRestartSlotSysvarImpl is an implementation of the sol_get_last_restart_slot_sysvar syscall
func SyscallGetLastRestartSlotSysvarImpl(vm sbpf.VM, addr uint64) (uint64, error) {
	execCtx := executionCtx(vm)

	cost := uint64(cu.CUSyscallBaseCost + SysvarLastRestartSlotStructLen)
	err := execCtx.ComputeMeter.Consume(cost)
	if err != nil {
		return syscallCuErr()
	}
	if !syscallAddressIsAligned(execCtx, addr, 8) {
		return syscallErrCustom("SyscallError::UnalignedPointer")
	}
	if syscallParameterAddressRestricted(execCtx, addr) {
		return syscallErrCustom("SyscallError::InvalidPointer")
	}

	lastRestartSlotDst, err := vm.Translate(addr, SysvarLastRestartSlotStructLen, true)
	if err != nil {
		return syscallErr(err)
	}

	lrs, err := ReadLastRestartSlotSysvar(execCtx)
	if err != nil {
		return syscallErr(err)
	}
	binary.LittleEndian.PutUint64(lastRestartSlotDst[:8], lrs.LastRestartSlot)

	return syscallSuccess(0)
}

var SyscallGetLastRestartSlotSysvar = sbpf.SyscallFunc1(SyscallGetLastRestartSlotSysvarImpl)

const (
	offsetLenExceedsSysvar = 1
	sysvarNotFound         = 2
)

var permittedSysvarAddrs = []solana.PublicKey{SysvarClockAddr, SysvarEpochScheduleAddr, SysvarEpochRewardsAddr, SysvarRentAddr,
	SysvarSlotHashesAddr, SysvarStakeHistoryAddr, SysvarLastRestartSlotAddr}

func fetchSysvarBytesForPubkey(execCtx *ExecutionCtx, pubkey solana.PublicKey) ([]byte, error) {
	if !slices.Contains(permittedSysvarAddrs, pubkey) {
		return nil, fmt.Errorf("unrecognised sysvar")
	}
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			if data, ok := bankSysvars.RawView(pubkey); ok {
				return data, nil
			}
			return nil, fmt.Errorf("sysvar account not found in bank snapshot")
		}
	}

	accts := addrObjectForLookup(execCtx)
	if accts != nil && *accts != nil {
		key := [32]byte(pubkey)
		acct, err := (*accts).GetAccount(&key)
		if err == nil {
			return acct.Data, nil
		}
	}

	var sysvarAcct *accounts.Account
	if pubkey == SysvarClockAddr {
		sysvarAcct = SysvarCache.Clock.Acct
	} else if pubkey == SysvarEpochScheduleAddr {
		sysvarAcct = SysvarCache.EpochSchedule.Acct
	} else if pubkey == SysvarEpochRewardsAddr {
		sysvarAcct = SysvarCache.EpochRewards.Acct
	} else if pubkey == SysvarRentAddr {
		sysvarAcct = SysvarCache.Rent.Acct
	} else if pubkey == SysvarSlotHashesAddr {
		sysvarAcct = SysvarCache.SlotHashes.Acct
	} else if pubkey == SysvarStakeHistoryAddr {
		sysvarAcct = SysvarCache.StakeHistory.Acct
	} else if pubkey == SysvarLastRestartSlotAddr {
		sysvarAcct = SysvarCache.LastRestartSlot.Acct
	}
	if sysvarAcct == nil {
		return nil, fmt.Errorf("sysvar account not found")
	}

	return sysvarAcct.Data, nil
}

func SyscallGetSysvarImpl(vm sbpf.VM, sysvarIdAddr uint64, varAddr uint64, offset uint64, length uint64) (uint64, error) {
	execCtx := executionCtx(vm)

	sysvarIdCost := uint64(32 / cu.CUCpiBytesPerUnit)
	sysvarBufCost := length / cu.CUCpiBytesPerUnit
	totalCost := safemath.SaturatingAddU64(safemath.SaturatingAddU64(cu.CUSysvarBaseCost, sysvarIdCost), max(sysvarBufCost, cu.CUMemOpBaseCost))

	err := execCtx.ComputeMeter.Consume(totalCost)
	if err != nil {
		return syscallCuErr()
	}
	if !syscallCheckAligned(execCtx) {
		return syscallErrCustom("SyscallError::UnalignedPointer")
	}
	if syscallParameterAddressRestricted(execCtx, varAddr) {
		return syscallErrCustom("SyscallError::InvalidPointer")
	}

	varBuf, err := vm.Translate(varAddr, length, true)
	if err != nil {
		return syscallErr(err)
	}

	sysvarIdBytes, err := vm.Translate(sysvarIdAddr, 32, false)
	if err != nil {
		return syscallErr(err)
	}
	sysvarId := solana.PublicKeyFromBytes(sysvarIdBytes)

	offsetLen, err := safemath.CheckedAddU64(offset, length)
	if err != nil {
		return syscallErr(InstrErrArithmeticOverflow)
	}

	_, err = safemath.CheckedAddU64(varAddr, length)
	if err != nil {
		return syscallErr(InstrErrArithmeticOverflow)
	}

	sysvarBuf, err := fetchSysvarBytesForPubkey(execCtx, sysvarId)
	if err != nil {
		return syscallSuccess(sysvarNotFound)
	}

	if offsetLen > uint64(len(sysvarBuf)) {
		return syscallSuccess(offsetLenExceedsSysvar)
	}

	copy(varBuf, sysvarBuf[offset:])

	return syscallSuccess(0)
}

var SyscallGetSysvar = sbpf.SyscallFunc4(SyscallGetSysvarImpl)

func SyscallGetEpochStakeImpl(vm sbpf.VM, varAddr uint64) (uint64, error) {
	var err error
	execCtx := executionCtx(vm)

	if varAddr == 0 {
		err = execCtx.ComputeMeter.Consume(cu.CUSyscallBaseCost)
		if err != nil {
			return syscallCuErr()
		}

		return syscallSuccess(execCtx.SlotCtx.TotalEpochStake)
	} else {
		cost := uint64(cu.CUSyscallBaseCost + (32 / cu.CUCpiBytesPerUnit) + cu.CUMemOpBaseCost)
		err = execCtx.ComputeMeter.Consume(cost)
		if err != nil {
			return syscallCuErr()
		}

		var voteAddrBytes []byte
		voteAddrBytes, err = vm.Translate(varAddr, 32, false)
		if err != nil {
			return syscallErr(err)
		}
		voteAddr := solana.PublicKeyFromBytes(voteAddrBytes)

		stake, exists := execCtx.SlotCtx.VoteAccts[voteAddr]
		if exists {
			return syscallSuccess(stake)
		} else {
			return syscallSuccess(0)
		}
	}
}

var SyscallGetEpochStake = sbpf.SyscallFunc1(SyscallGetEpochStakeImpl)
