package sealevel

import (
	"bytes"
	"slices"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
)

// SyscallGetStackHeightImpl is an implementation of the sol_get_stack_height syscall
func SyscallGetStackHeightImpl(vm sbpf.VM) (uint64, error) {
	//mlog.Log.Debugf("SyscallGetStackHeight")

	execCtx := executionCtx(vm)
	err := execCtx.ComputeMeter.Consume(cu.CUSyscallBaseCost)
	if err != nil {
		return syscallCuErr()
	}

	return syscallSuccess(transactionCtx(vm).InstructionCtxStackHeight())
}

var SyscallGetStackHeight = sbpf.SyscallFunc0(SyscallGetStackHeightImpl)

// SyscallGetReturnDataImpl is an implementation of the sol_get_return_data syscall
func SyscallGetReturnDataImpl(vm sbpf.VM, returnDataAddr, length, programIdAddr uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallGetReturnData")

	execCtx := executionCtx(vm)
	err := execCtx.ComputeMeter.Consume(cu.CUSyscallBaseCost)
	if err != nil {
		return syscallCuErr()
	}

	programId, returnData := transactionCtx(vm).ReturnData()

	length = min(uint64(len(returnData)), length)

	if length != 0 {
		result := safemath.SaturatingAddU64(length, solana.PublicKeyLength) / cu.CUCpiBytesPerUnit
		err = execCtx.ComputeMeter.Consume(result)
		if err != nil {
			return syscallCuErr()
		}

		var returnDataResult []byte
		returnDataResult, err = vm.Translate(returnDataAddr, length, true)
		if err != nil {
			return syscallErr(err)
		}

		// Compare against the clamped prefix, not the whole return data.
		// Agave slices first -- from_slice = return_data.get(..length) -- so its
		// two sides are equal by construction and the check only guards the
		// translation. Comparing the full return data here rejected every call
		// that asked for fewer bytes than were available, which is the ordinary
		// case for a program reading a fixed-size header out of a longer
		// result.
		fromSlice := returnData[:length]
		if len(fromSlice) != len(returnDataResult) {
			return syscallErr(SyscallErrInvalidLength)
		}

		copy(returnDataResult, fromSlice)

		var programIdResult []byte
		programIdResult, err = vm.Translate(programIdAddr, solana.PublicKeyLength, true)
		if err != nil {
			return syscallErr(err)
		}

		if !isNonOverlapping(returnDataAddr, length, programIdAddr, solana.PublicKeyLength) {
			return syscallErr(SyscallErrCopyOverlapping)
		}

		copy(programIdResult, programId[:])
	}

	return syscallSuccess(uint64(len(returnData)))
}

var SyscallGetReturnData = sbpf.SyscallFunc3(SyscallGetReturnDataImpl)

const MaxReturnData = 1024

// SyscallSetReturnDataImpl is an implementation of the sol_set_return_data syscall
func SyscallSetReturnDataImpl(vm sbpf.VM, addr, length uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallSetReturnData")

	execCtx := executionCtx(vm)
	cost := safemath.SaturatingAddU64(length/cu.CUCpiBytesPerUnit, cu.CUSyscallBaseCost)
	err := execCtx.ComputeMeter.Consume(cost)
	if err != nil {
		return syscallCuErr()
	}

	if length > MaxReturnData {
		return syscallErr(SyscallErrReturnDataTooLarge)
	}

	var returnData []byte
	if length > 0 {
		returnDataBytes, err := vm.Translate(addr, length, false)
		if err != nil {
			return syscallErr(err)
		}
		returnData = make([]byte, length)
		copy(returnData, returnDataBytes)
	}

	txCtx := transactionCtx(vm)
	ixCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return syscallErr(err)
	}

	programId, err := ixCtx.LastProgramKey(txCtx)
	if err != nil {
		return syscallErr(err)
	}

	txCtx.SetReturnData(programId, returnData)

	return syscallSuccess(0)
}

var SyscallSetReturnData = sbpf.SyscallFunc2(SyscallSetReturnDataImpl)

// SyscallGetProcessedSiblingInstructionImpl is an implementation of the sol_get_processed_sibling_instruction syscall
func SyscallGetProcessedSiblingInstructionImpl(vm sbpf.VM, index, metaAddr, programIdAddr, dataAddr, accountsAddr uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallGetProcessedSiblingInstruction")

	execCtx := executionCtx(vm)
	txCtx := transactionCtx(vm)

	err := execCtx.ComputeMeter.Consume(cu.CUSyscallBaseCost)
	if err != nil {
		return syscallCuErr()
	}

	stackHeight := execCtx.StackHeight()
	instrTraceLen := txCtx.InstructionTraceLength()

	if instrTraceLen == 0 {
		return syscallSuccess(0)
	}

	var reverseIndexAtStackHeight uint64
	var instrCtxFound *InstructionCtx

	traceIdxs := make([]uint64, 0)
	for i := range instrTraceLen {
		traceIdxs = append(traceIdxs, i)
	}
	slices.Reverse(traceIdxs)

	for _, indexInTrace := range traceIdxs {
		instrCtx, err := txCtx.InstructionCtxAtIndexInTrace(indexInTrace)
		if err != nil {
			return syscallErr(err)
		}

		if instrCtx.StackHeight() < stackHeight {
			break
		}

		if instrCtx.StackHeight() == stackHeight {
			if safemath.SaturatingAddU64(index, 1) == reverseIndexAtStackHeight {
				instrCtxFound = instrCtx
				break
			}
			reverseIndexAtStackHeight = safemath.SaturatingAddU64(reverseIndexAtStackHeight, 1)
		}
	}

	if instrCtxFound != nil {
		resultsHeaderBytes, err := vm.Translate(metaAddr, ProcessedSiblingInstructionSize, false)
		if err != nil {
			return syscallErr(err)
		}

		reader := bytes.NewReader(resultsHeaderBytes)

		var resultHeader ProcessedSiblingInstruction
		err = resultHeader.Unmarshal(reader)
		if err != nil {
			return syscallErr(err)
		}

		if resultHeader.DataLen == uint64(len(instrCtxFound.Data)) &&
			resultHeader.AccountsLen == instrCtxFound.NumberOfInstructionAccounts() {

			programIdBytes, err := vm.Translate(programIdAddr, solana.PublicKeyLength, true)
			if err != nil {
				return syscallErr(err)
			}

			data, err := vm.Translate(dataAddr, resultHeader.DataLen, true)
			if err != nil {
				return syscallErr(err)
			}

			accountMetaDataSize := safemath.SaturatingMulU64(resultHeader.AccountsLen, AccountMetaSize)
			accountMetaSliceBytes, err := vm.Translate(accountsAddr, accountMetaDataSize, true)
			if err != nil {
				return syscallErr(err)
			}

			if !isNonOverlapping(metaAddr, ProcessedSiblingInstructionSize, programIdAddr, solana.PublicKeyLength) ||
				!isNonOverlapping(metaAddr, ProcessedSiblingInstructionSize, accountsAddr, accountMetaDataSize) ||
				!isNonOverlapping(metaAddr, ProcessedSiblingInstructionSize, dataAddr, resultHeader.DataLen) ||
				!isNonOverlapping(programIdAddr, solana.PublicKeyLength, dataAddr, resultHeader.DataLen) ||
				!isNonOverlapping(programIdAddr, solana.PublicKeyLength, accountsAddr, accountMetaDataSize) ||
				!isNonOverlapping(dataAddr, resultHeader.DataLen, accountsAddr, accountMetaDataSize) {
				return syscallErr(SyscallErrCopyOverlapping)
			}

			pk, err := instrCtxFound.LastProgramKey(transactionCtx(vm))
			if err != nil {
				return syscallErr(err)
			}

			// copy out programID pubkey
			copy(programIdBytes, pk[:])

			// copy out instruction data
			copy(data, instrCtxFound.Data)

			// build AccountMetas, serialize them and then copy them out
			writer := new(bytes.Buffer)

			for instrAcctIdx := uint64(0); instrAcctIdx < instrCtxFound.NumberOfInstructionAccounts(); instrAcctIdx++ {
				idx, err := instrCtxFound.IndexOfInstructionAccountInTransaction(instrAcctIdx)
				if err != nil {
					return syscallErr(err)
				}
				key, err := txCtx.KeyOfAccountAtIndex(idx)
				if err != nil {
					return syscallErr(err)
				}

				isSigner, err := instrCtxFound.IsInstructionAccountSigner(instrAcctIdx)
				if err != nil {
					return syscallErr(err)
				}

				isWritable, err := instrCtxFound.IsInstructionAccountWritable(instrAcctIdx)
				if err != nil {
					return syscallErr(err)
				}

				acctMeta := AccountMeta{Pubkey: key, IsSigner: isSigner, IsWritable: isWritable}
				acctBytes := acctMeta.Marshal()
				writer.Write(acctBytes)
			}

			// copy AccountMetas serialized above out
			copy(accountMetaSliceBytes, writer.Bytes())
		}

		// build a new result header, serialize it & copy it out
		var resultHeaderOut ProcessedSiblingInstruction
		resultHeaderOut.DataLen = uint64(len(instrCtxFound.Data))
		resultHeaderOut.AccountsLen = instrCtxFound.NumberOfInstructionAccounts()
		resultHeaderOutBytes := resultHeaderOut.Marshal()
		copy(resultsHeaderBytes, resultHeaderOutBytes)

		return syscallSuccess(1) // true
	}

	return syscallSuccess(0) // false
}

var SyscallGetProcessedSiblingInstruction = sbpf.SyscallFunc5(SyscallGetProcessedSiblingInstructionImpl)
