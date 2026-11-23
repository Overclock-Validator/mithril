package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// These cases follow Agave v4.4.1 program-runtime/src/cpi.rs's
// test_update_{caller,callee}_account_* cases. The VM is built from Mithril's
// actual ABI v1 serialization so the length header, copied backing, and VASA
// account-data region share the same addresses used by a running program.
func agaveCPITestAccount(t *testing.T, mapping string, data []byte, externalOwner bool) (*ExecutionCtx, *sbpf.Interpreter, *CallerAccount, []sbpf.InputRegion) {
	t.Helper()
	ctx := serializationTestContext(t, 1)
	ctx.TransactionContext.Accounts.Accounts[1].Data = bytes.Clone(data)
	if externalOwner {
		ctx.TransactionContext.Accounts.Accounts[1].Owner = solana.PublicKey{0xee}
	}
	if mapping != "legacy" {
		ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	}
	if mapping == "direct" {
		ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
	}
	input, _, metas, regions, err := serializeParametersAligned(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, metas)
	ctx.ComputeMeter = cu.NewComputeMeter(100_000)
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{
		Input: input, InputRegions: regions, Context: ctx, ComputeMeter: &ctx.ComputeMeter,
		HeapMax: 32 * 1024, DisableStackFrameGaps: true,
	})
	t.Cleanup(vm.Finish)
	meta := metas[0]
	dataOffset := int(meta.vmDataAddr - sbpf.VaddrInput)
	lamportsOffset := int(meta.vmLamportsAddr - sbpf.VaddrInput)
	ownerOffset := int(meta.vmOwnerAddr - sbpf.VaddrInput)
	caller := &CallerAccount{
		Lamports:        input[lamportsOffset : lamportsOffset+8],
		Owner:           input[ownerOffset : ownerOffset+32],
		OriginalDataLen: uint64(len(data)),
		VmDataAddr:      meta.vmDataAddr,
		RefToLenInVm:    input[dataOffset-8 : dataOffset],
	}
	if mapping != "direct" {
		caller.SerializedData = input[dataOffset : dataOffset+len(data)]
	}
	return ctx, vm, caller, regions
}

func agaveCPIBorrowAccount(t *testing.T, ctx *ExecutionCtx) *BorrowedAccount {
	t.Helper()
	ix, err := ctx.TransactionContext.CurrentInstructionCtx()
	require.NoError(t, err)
	account, err := ix.BorrowInstructionAccount(ctx.TransactionContext, 0)
	require.NoError(t, err)
	return account
}

func agaveCPISetCallerData(t *testing.T, vm *sbpf.Interpreter, caller *CallerAccount, mapping string, data []byte) {
	t.Helper()
	if mapping == "direct" {
		if len(data) != 0 {
			require.NoError(t, vm.Write(caller.VmDataAddr, data))
		}
	} else {
		backing, err := vm.TranslateInput(caller.VmDataAddr, uint64(len(data)))
		require.NoError(t, err)
		copy(backing, data)
		caller.SerializedData = backing
	}
	binary.LittleEndian.PutUint64(caller.RefToLenInVm, uint64(len(data)))
}

func agaveCPIUpdateCallee(t *testing.T, ctx *ExecutionCtx, vm *sbpf.Interpreter, caller *CallerAccount, mapping string) (bool, error) {
	t.Helper()
	account := agaveCPIBorrowAccount(t, ctx)
	defer account.Drop()
	refresh, err := updateCalleeAccount(vm, ctx, *caller, account)
	if err == nil && refresh && mapping != "legacy" {
		require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
	}
	return refresh, err
}

func agaveCPIRustAccountInfo(t *testing.T, ctx *ExecutionCtx, vm *sbpf.Interpreter, caller *CallerAccount) SolAccountInfoRust {
	t.Helper()
	// The ABI v1 header puts these three fields immediately before data_len.
	meta := serializedAcctMetadata{
		originalDataLen: caller.OriginalDataLen,
		vmDataAddr:      caller.VmDataAddr,
		vmKeyAddr:       caller.VmDataAddr - 80,
		vmOwnerAddr:     caller.VmDataAddr - 48,
		vmLamportsAddr:  caller.VmDataAddr - 16,
	}
	ctx.serializedAccountMetadataStack = append(ctx.serializedAccountMetadataStack, []serializedAcctMetadata{meta})
	lamportsBox := sbpf.VaddrHeap
	dataBox := sbpf.VaddrHeap + 64
	require.NoError(t, vm.Write64(lamportsBox+24, meta.vmLamportsAddr))
	require.NoError(t, vm.Write64(dataBox+24, meta.vmDataAddr))
	require.NoError(t, vm.Write64(dataBox+32, caller.OriginalDataLen))
	return SolAccountInfoRust{
		PubkeyAddr: meta.vmKeyAddr, LamportsBoxAddr: lamportsBox,
		DataBoxAddr: dataBox, OwnerAddr: meta.vmOwnerAddr,
	}
}

func TestAgaveCPICallerAccountFromAccountInfo(t *testing.T) {
	ctx, vm, caller, _ := agaveCPITestAccount(t, "copied", []byte("foo"), false)
	ctx.Features.EnableFeature(features.SyscallParameterAddressRestrictions, 0)
	info := agaveCPIRustAccountInfo(t, ctx, vm, caller)
	got, err := callerAccountFromAccountInfoRust(vm, ctx, 0, info, false)
	require.NoError(t, err)
	require.Equal(t, caller.OriginalDataLen, got.OriginalDataLen)
	require.Equal(t, caller.VmDataAddr, got.VmDataAddr)
	require.Equal(t, caller.Lamports, got.Lamports)
	require.Equal(t, caller.Owner, got.Owner)
	require.EqualValues(t, 3, binary.LittleEndian.Uint64(got.RefToLenInVm))
	require.Equal(t, []byte("foo"), got.SerializedData)
}

func TestAgaveCPIGetSerializedDataRejectsPastReserved(t *testing.T) {
	for _, mapping := range []string{"copied", "direct"} {
		for _, restricted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restricted=%v", mapping, restricted), func(t *testing.T) {
				ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foo"), false)
				if restricted {
					ctx.Features.EnableFeature(features.SyscallParameterAddressRestrictions, 0)
				}
				info := agaveCPIRustAccountInfo(t, ctx, vm, caller)
				require.NoError(t, vm.Write64(info.DataBoxAddr+32, 3+MaxPermittedDataIncrease+1))
				_, err := callerAccountFromAccountInfoRust(vm, ctx, 0, info, false)
				require.ErrorIs(t, err, InstrErrInvalidRealloc, "Agave rejects before translating serialized account bytes")
			})
		}
	}
}

func TestAgaveCPIDeprecatedCallerRejectsCalleeGrowth(t *testing.T) {
	for _, mapping := range []string{"copied", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foo"), false)
			account := agaveCPIBorrowAccount(t, ctx)
			defer account.Drop()
			require.NoError(t, account.SetData(ctx.Features, []byte("foobar")))
			require.ErrorIs(t, updateCallerAccount(vm, caller, account, true), InstrErrInvalidRealloc,
				"the deprecated caller has no realloc area even with VASA and SIMD-0459 off")
		})
	}
}

func TestAgaveCPIRejectsAccountInfoLengthPointerInInput(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		for _, abi := range []string{"rust", "c"} {
			t.Run(mapping+"/"+abi, func(t *testing.T) {
				ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foo"), false)
				ctx.Features.EnableFeature(features.SyscallParameterAddressRestrictions, 0)
				rustInfo := agaveCPIRustAccountInfo(t, ctx, vm, caller)
				if abi == "rust" {
					rustInfo.DataBoxAddr = caller.VmDataAddr // RC box and its length would be in account data.
					_, err := callerAccountFromAccountInfoRust(vm, ctx, 0, rustInfo, false)
					require.ErrorIs(t, err, SyscallErrInvalidPointer)
					return
				}
				cInfo := SolAccountInfoC{
					KeyAddr: caller.VmDataAddr - 80, LamportsAddr: caller.VmDataAddr - 16,
					OwnerAddr: caller.VmDataAddr - 48, DataAddr: caller.VmDataAddr,
					DataLen: caller.OriginalDataLen,
				}
				_, err := callerAccountFromAccountInfoC(vm, ctx, 0, 0, cInfo, caller.VmDataAddr, false)
				require.ErrorIs(t, err, SyscallErrInvalidPointer)
			})
		}
	}
}

// Agave's test_cpi_invalid_account_info_pointers mutates each of the four
// account-address fields. Rust stores the lamports and data addresses inside
// their RC boxes; C stores them directly in SolAccountInfo.
func TestAgaveCPIRejectsWrongAccountInfoPointers(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		for _, field := range []string{"key", "lamports", "owner", "data"} {
			for _, abi := range []string{"rust", "c"} {
				t.Run(mapping+"/"+abi+"/"+field, func(t *testing.T) {
					ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foo"), false)
					ctx.Features.EnableFeature(features.SyscallParameterAddressRestrictions, 0)
					rustInfo := agaveCPIRustAccountInfo(t, ctx, vm, caller)
					if abi == "rust" {
						_, err := callerAccountFromAccountInfoRust(vm, ctx, 0, rustInfo, false)
						require.NoError(t, err)
						switch field {
						case "key":
							rustInfo.PubkeyAddr++
						case "lamports":
							require.NoError(t, vm.Write64(rustInfo.LamportsBoxAddr+24, caller.VmDataAddr-15))
						case "owner":
							rustInfo.OwnerAddr++
						case "data":
							require.NoError(t, vm.Write64(rustInfo.DataBoxAddr+24, caller.VmDataAddr+1))
						}
						_, err = callerAccountFromAccountInfoRust(vm, ctx, 0, rustInfo, false)
						require.ErrorIs(t, err, SyscallErrInvalidPointer)
						return
					}

					cInfo := SolAccountInfoC{
						KeyAddr: caller.VmDataAddr - 80, LamportsAddr: caller.VmDataAddr - 16,
						OwnerAddr: caller.VmDataAddr - 48, DataAddr: caller.VmDataAddr,
						DataLen: caller.OriginalDataLen,
					}
					cInfoAddr := sbpf.VaddrHeap + 128
					require.NoError(t, vm.Write64(cInfoAddr+solAccountInfoCDataLenOffset, cInfo.DataLen))
					_, err := callerAccountFromAccountInfoC(vm, ctx, 0, 0, cInfo, cInfoAddr, false)
					require.NoError(t, err)
					switch field {
					case "key":
						cInfo.KeyAddr++
					case "lamports":
						cInfo.LamportsAddr++
					case "owner":
						cInfo.OwnerAddr++
					case "data":
						cInfo.DataAddr++
					}
					_, err = callerAccountFromAccountInfoC(vm, ctx, 0, 0, cInfo, cInfoAddr, false)
					require.ErrorIs(t, err, SyscallErrInvalidPointer)
				})
			}
		}
	}
}

// Agave's test_cpi_invalid_account_info_rc copies either RC box into the
// realloc padding of an externally owned zero-length account, then points the
// corresponding AccountInfo field there. VASA rejects the copy as an account
// write; legacy accepts the copy but rejects the forged input-space RC pointer.
func TestAgaveCPIRejectsRCBoxInExternalAccountData(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		for _, rc := range []string{"lamports", "data"} {
			t.Run(mapping+"/"+rc, func(t *testing.T) {
				ctx := serializationTestContext(t, 2)
				ctx.Features.EnableFeature(features.SyscallParameterAddressRestrictions, 0)
				if mapping != "legacy" {
					ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
				}
				if mapping == "direct" {
					ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
				}
				ctx.TransactionContext.Accounts.Accounts[1].Owner = solana.PublicKey{0xee}
				ctx.TransactionContext.Accounts.Accounts[1].Data = nil
				input, _, metas, regions, err := serializeParametersAligned(ctx)
				require.NoError(t, err)
				ctx.serializedAccountMetadataStack = append(ctx.serializedAccountMetadataStack, metas)
				ctx.ComputeMeter = cu.NewComputeMeter(100_000)
				vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{
					Input: input, InputRegions: regions, Context: ctx, ComputeMeter: &ctx.ComputeMeter,
					HeapMax: 32 * 1024, DisableStackFrameGaps: true,
				})
				t.Cleanup(vm.Finish)
				target := metas[1]
				info := SolAccountInfoRust{
					PubkeyAddr: target.vmKeyAddr, OwnerAddr: target.vmOwnerAddr,
					LamportsBoxAddr: sbpf.VaddrHeap, DataBoxAddr: sbpf.VaddrHeap + 64,
				}
				require.NoError(t, vm.Write64(info.LamportsBoxAddr+24, target.vmLamportsAddr))
				require.NoError(t, vm.Write64(info.DataBoxAddr+24, target.vmDataAddr))
				require.NoError(t, vm.Write64(info.DataBoxAddr+32, target.originalDataLen))
				_, err = callerAccountFromAccountInfoRust(vm, ctx, 1, info, false)
				require.NoError(t, err)

				rcAddr := info.LamportsBoxAddr
				if rc == "data" {
					rcAddr = info.DataBoxAddr
				}
				box, err := vm.Translate(rcAddr, 32, false)
				require.NoError(t, err)
				accountDataAddr := metas[0].vmDataAddr
				err = vm.Write(accountDataAddr, bytes.Clone(box))
				if mapping != "legacy" {
					require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, err, regions), InstrErrExternalAccountDataModified)
					require.Empty(t, ctx.TransactionContext.Accounts.Accounts[1].Data)
					return
				}
				require.NoError(t, err)
				if rc == "lamports" {
					info.LamportsBoxAddr = accountDataAddr
				} else {
					info.DataBoxAddr = accountDataAddr
				}
				_, err = callerAccountFromAccountInfoRust(vm, ctx, 1, info, false)
				require.ErrorIs(t, err, SyscallErrInvalidPointer)
			})
		}
	}
}

// Agave's test_cpi_account_ownership_writability checks both ways ownership
// can change across CPI and six writes at the first/last data and realloc
// bytes. ABI v1 ignores writes into legacy realloc padding; VASA rejects them
// as writes to an account the caller no longer owns.
func TestAgaveCPIOwnershipChangeWritePermissions(t *testing.T) {
	positions := []struct {
		accountSize, byteIndex int
	}{
		{0, 0}, {0, MaxPermittedDataIncrease - 1},
		{2, 0}, {2, 1}, {2, 3}, {2, 2 + MaxPermittedDataIncrease - 1},
	}
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		for _, site := range []string{"callee", "caller"} {
			for _, pos := range positions {
				t.Run(fmt.Sprintf("%s/%s/size=%d/byte=%d", mapping, site, pos.accountSize, pos.byteIndex), func(t *testing.T) {
					ctx, vm, caller, regions := agaveCPITestAccount(t, mapping, make([]byte, pos.accountSize), false)
					newOwner := solana.PublicKey{0x42}
					if site == "callee" {
						account := agaveCPIBorrowAccount(t, ctx)
						require.NoError(t, account.SetOwner(ctx.Features, newOwner))
						require.NoError(t, updateCallerAccount(vm, caller, account, false))
						if mapping != "legacy" {
							require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
						}
						account.Drop()
					} else {
						copy(caller.Owner, newOwner[:])
						_, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
						require.NoError(t, err)
					}
					require.Equal(t, [32]byte(newOwner), ctx.TransactionContext.Accounts.Accounts[1].Owner)

					addr := caller.VmDataAddr + uint64(pos.byteIndex)
					err := vm.Write8(addr, 0x2a)
					if mapping == "legacy" {
						require.NoError(t, err)
						_, err = agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
						if pos.byteIndex < pos.accountSize {
							require.ErrorIs(t, err, InstrErrExternalAccountDataModified)
						} else {
							require.NoError(t, err, "legacy realloc padding does not modify the account")
						}
					} else {
						require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, err, regions), InstrErrExternalAccountDataModified)
					}
					require.Equal(t, make([]byte, pos.accountSize), ctx.TransactionContext.Accounts.Accounts[1].Data)
				})
			}
		}
	}
}

func TestAgaveCPIRegainedOwnershipAllowsGrowth(t *testing.T) {
	for _, mapping := range []string{"copied", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, nil, true)
			program := ctx.TransactionContext.Accounts.Accounts[0].Key
			current, err := ctx.TransactionContext.Accounts.Touch(1)
			require.NoError(t, err)
			current.Owner = program // Inner program gives the account to the caller.
			account := agaveCPIBorrowAccount(t, ctx)
			require.NoError(t, updateCallerAccount(vm, caller, account, false))
			require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
			account.Drop()
			require.NoError(t, vm.Write8(caller.VmDataAddr, 0x2a))
			if mapping == "copied" {
				caller.SerializedData, err = vm.TranslateInput(caller.VmDataAddr, 1)
				require.NoError(t, err)
			}
			binary.LittleEndian.PutUint64(caller.RefToLenInVm, 1)
			_, err = agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
			require.NoError(t, err)
			require.Equal(t, []byte{0x2a}, ctx.TransactionContext.Accounts.Accounts[1].Data)
		})
	}
}

// Agave's test_clone_account_data has three cases with an account owned by
// the inner program. Cloning and copying identical bytes is permitted by the
// legacy flat input, while VASA catches that attempted write immediately.
func TestAgaveCPIStaleClonedExternalData(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		for _, tc := range []struct {
			name                    string
			writeBefore, writeAfter bool
		}{
			{"callee_only_changes", false, false},
			{"caller_changes_before_cpi", true, false},
			{"callee_and_caller_same_change", false, true},
		} {
			t.Run(mapping+"/"+tc.name, func(t *testing.T) {
				initial := make([]byte, 10240)
				for i := range initial {
					initial[i] = byte(i)
				}
				ctx, vm, caller, regions := agaveCPITestAccount(t, mapping, initial, true)
				clone := bytes.Clone(initial)
				copyErr := vm.Write(caller.VmDataAddr, clone)
				if mapping != "legacy" {
					require.ErrorIs(t, mapVirtualAddressSpaceRunErr(ctx, copyErr, regions), InstrErrExternalAccountDataModified)
					require.Equal(t, initial, ctx.TransactionContext.Accounts.Accounts[1].Data)
					return
				}
				require.NoError(t, copyErr)
				if tc.writeBefore {
					require.NoError(t, vm.Write8(caller.VmDataAddr+8190, clone[8190]^0xe5))
				}
				_, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
				if tc.writeBefore {
					require.ErrorIs(t, err, InstrErrExternalAccountDataModified)
					require.Equal(t, initial, ctx.TransactionContext.Accounts.Accounts[1].Data)
					return
				}
				require.NoError(t, err)

				// The inner program owns this account and changes one byte.
				current, err := ctx.TransactionContext.Accounts.Touch(1)
				require.NoError(t, err)
				current.Data[8190] ^= 0xe5
				account := agaveCPIBorrowAccount(t, ctx)
				require.NoError(t, updateCallerAccount(vm, caller, account, false))
				account.Drop()
				if tc.writeAfter {
					// The stale clone happens to produce the callee's value.
					require.NoError(t, vm.Write8(caller.VmDataAddr+8190, clone[8190]^0xe5))
					_, err = agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
					require.NoError(t, err)
				}
				expected := bytes.Clone(initial)
				expected[8190] ^= 0xe5
				require.Equal(t, expected, ctx.TransactionContext.Accounts.Accounts[1].Data)
			})
		}
	}
}

func TestAgaveCPIUpdateCallerLamportsOwner(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, nil, false)
			account := agaveCPIBorrowAccount(t, ctx)
			require.NoError(t, account.SetLamports(42, ctx.Features))
			newOwner := solana.PublicKey{0x42}
			require.NoError(t, account.SetOwner(ctx.Features, newOwner))
			require.NoError(t, updateCallerAccount(vm, caller, account, false))
			if mapping != "legacy" {
				require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
			}
			account.Drop()
			require.EqualValues(t, 42, binary.LittleEndian.Uint64(caller.Lamports))
			require.Equal(t, newOwner[:], caller.Owner)
			if mapping != "legacy" {
				require.Error(t, vm.Write8(caller.VmDataAddr, 1), "ownership change must revoke data writes")
			}
		})
	}
}

func TestAgaveCPIUpdateCallerDataAndReallocLimit(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foobar"), false)
			reserved := len("foobar") + MaxPermittedDataIncrease
			for _, value := range [][]byte{[]byte("foo"), []byte("foobaz"), []byte("foobazbad")} {
				account := agaveCPIBorrowAccount(t, ctx)
				require.NoError(t, account.SetData(ctx.Features, value))
				require.NoError(t, updateCallerAccount(vm, caller, account, false))
				if mapping != "legacy" {
					require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
				}
				account.Drop()
				require.EqualValues(t, len(value), binary.LittleEndian.Uint64(caller.RefToLenInVm))
				serializedLen, err := vm.Read64(caller.VmDataAddr - 8)
				require.NoError(t, err)
				require.EqualValues(t, len(value), serializedLen)
				read := make([]byte, len(value))
				err = vm.Read(caller.VmDataAddr, read)
				require.NoError(t, err)
				require.Equal(t, value, read)
				if mapping != "direct" {
					require.Equal(t, value, caller.SerializedData)
					backing, err := vm.TranslateInput(caller.VmDataAddr, uint64(reserved))
					require.NoError(t, err)
					require.Equal(t, make([]byte, reserved-len(value)), backing[len(value):], "released data and realloc padding stay zeroed")
				}
				if mapping != "legacy" {
					_, err := vm.Read8(caller.VmDataAddr + uint64(len(value)))
					require.Error(t, err, "data after the current length is not readable")
				}
			}

			account := agaveCPIBorrowAccount(t, ctx)
			require.NoError(t, account.SetDataLength(uint64(reserved), ctx.Features))
			require.NoError(t, updateCallerAccount(vm, caller, account, false))
			if mapping != "legacy" {
				require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
			}
			account.Drop()
			require.EqualValues(t, reserved, binary.LittleEndian.Uint64(caller.RefToLenInVm))

			account = agaveCPIBorrowAccount(t, ctx)
			require.NoError(t, account.SetDataLength(uint64(reserved+1), ctx.Features))
			require.ErrorIs(t, updateCallerAccount(vm, caller, account, false), InstrErrInvalidRealloc)
			account.Drop()

			account = agaveCPIBorrowAccount(t, ctx)
			require.NoError(t, account.SetDataLength(0, ctx.Features))
			newOwner := solana.PublicKey{0x42}
			require.NoError(t, account.SetOwner(ctx.Features, newOwner))
			require.NoError(t, updateCallerAccount(vm, caller, account, false))
			if mapping != "legacy" {
				require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
			}
			account.Drop()
			require.Zero(t, binary.LittleEndian.Uint64(caller.RefToLenInVm))
			require.Equal(t, newOwner[:], caller.Owner)
			if mapping != "direct" {
				backing, err := vm.TranslateInput(caller.VmDataAddr, uint64(reserved))
				require.NoError(t, err)
				require.Equal(t, make([]byte, reserved), backing)
			}
		})
	}
}

func TestAgaveCPIUpdateCalleeLamportsOwner(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, nil, false)
			binary.LittleEndian.PutUint64(caller.Lamports, 42)
			newOwner := solana.PublicKey{0x42}
			copy(caller.Owner, newOwner[:])
			refresh, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
			require.NoError(t, err)
			require.True(t, refresh, "owner changes require a caller refresh in every mode")
			account := ctx.TransactionContext.Accounts.Accounts[1]
			require.EqualValues(t, 42, account.Lamports)
			require.Equal(t, [32]byte(newOwner), account.Owner)
			if mapping != "legacy" {
				require.Error(t, vm.Write8(caller.VmDataAddr, 1), "ownership change must revoke data writes")
			}
		})
	}
}

func TestAgaveCPIUpdateCalleeDataWritable(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foobar"), false)
			for _, value := range [][]byte{[]byte("boobar"), []byte("foobarbaz"), []byte("baz"), nil} {
				agaveCPISetCallerData(t, vm, caller, mapping, value)
				if len(value) == 0 {
					newOwner := solana.PublicKey{0x42}
					copy(caller.Owner, newOwner[:])
				}
				refresh, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
				require.NoError(t, err)
				wantRefresh := len(value) != len("foobar") && mapping != "legacy"
				if len(value) == 0 {
					wantRefresh = true // The final iteration also changes owner.
				}
				require.Equal(t, wantRefresh, refresh)
				require.True(t, bytes.Equal(value, ctx.TransactionContext.Accounts.Accounts[1].Data))
				if mapping == "copied" && len(value) < len("foobarbaz") {
					backing, err := vm.TranslateInput(caller.VmDataAddr, uint64(len("foobarbaz")))
					require.NoError(t, err)
					require.Equal(t, make([]byte, len("foobarbaz")-len(value)), backing[len(value):len("foobarbaz")], "truncated bytes must be cleared")
				}
				if mapping != "legacy" {
					_, err := vm.Read8(caller.VmDataAddr + uint64(len(value)))
					require.Error(t, err)
				}
			}
			require.Equal(t, [32]byte(solana.PublicKey{0x42}), ctx.TransactionContext.Accounts.Accounts[1].Owner)
			if mapping != "legacy" {
				require.Error(t, vm.Write8(caller.VmDataAddr, 1), "closed account is no longer writable")
			}
		})
	}
}

func TestAgaveCPIUpdateCalleeDataExternalOwner(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foobar"), true)
			if mapping == "legacy" {
				agaveCPISetCallerData(t, vm, caller, mapping, []byte("boobar"))
				_, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
				require.ErrorIs(t, err, InstrErrExternalAccountDataModified)
				require.Equal(t, []byte("foobar"), ctx.TransactionContext.Accounts.Accounts[1].Data)
			}
			for _, value := range [][]byte{[]byte("foobarbaz"), []byte("baz")} {
				if mapping == "direct" {
					binary.LittleEndian.PutUint64(caller.RefToLenInVm, uint64(len(value)))
				} else {
					agaveCPISetCallerData(t, vm, caller, mapping, value)
				}
				_, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
				require.ErrorIs(t, err, InstrErrExternalAccountDataModified, "Agave rejects both growth and truncation with this error")
				require.Equal(t, []byte("foobar"), ctx.TransactionContext.Accounts.Accounts[1].Data)
			}
		})
	}
}

// Agave's test_cpi_account_data_updates has the callee grow and shrink an
// account owned by the callee. The caller is not allowed to write that account,
// but must still see the new data, length, and zeroed former contents.
func TestAgaveCPICalleeResizeExternalToCaller(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foo"), true)
			for _, value := range [][]byte{[]byte("foobar"), []byte("foob")} {
				// Model the inner program's successful write and resize. This
				// account is external to the current (outer) instruction.
				current, err := ctx.TransactionContext.Accounts.Touch(1)
				require.NoError(t, err)
				ctx.TransactionContext.AccountsResizeDelta += int64(len(value) - len(current.Data))
				current.Data = bytes.Clone(value)
				account := agaveCPIBorrowAccount(t, ctx)
				require.NoError(t, updateCallerAccount(vm, caller, account, false))
				if mapping != "legacy" {
					require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
				}
				account.Drop()

				require.EqualValues(t, len(value), binary.LittleEndian.Uint64(caller.RefToLenInVm))
				serializedLen, err := vm.Read64(caller.VmDataAddr - 8)
				require.NoError(t, err)
				require.EqualValues(t, len(value), serializedLen)
				read := make([]byte, len(value))
				require.NoError(t, vm.Read(caller.VmDataAddr, read))
				require.Equal(t, value, read)
				if mapping != "direct" {
					backing, err := vm.TranslateInput(caller.VmDataAddr, 6)
					require.NoError(t, err)
					require.Equal(t, make([]byte, 6-len(value)), backing[len(value):6])
				}
				if mapping != "legacy" {
					require.Error(t, vm.Write8(caller.VmDataAddr, 1), "the caller cannot write a callee-owned account")
				}
			}
		})
	}
}

// Agave's nested CPI case grows "foo" to "foobazbad" in the caller, then
// shrinks it in the callee. Cover final lengths on both sides of the original
// length so the returned mapping and unused serialized data are checked.
func TestAgaveCPICallerGrowthThenCalleeShrink(t *testing.T) {
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		for _, final := range [][]byte{[]byte("foobazb"), []byte("f")} {
			name := mapping + "/above_original"
			if len(final) < len("foo") {
				name = mapping + "/below_original"
			}
			t.Run(name, func(t *testing.T) {
				ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, []byte("foo"), false)
				agaveCPISetCallerData(t, vm, caller, mapping, []byte("foobazbad"))
				refresh, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
				require.NoError(t, err)
				require.Equal(t, mapping != "legacy", refresh)
				require.Equal(t, []byte("foobazbad"), ctx.TransactionContext.Accounts.Accounts[1].Data)

				account := agaveCPIBorrowAccount(t, ctx)
				require.NoError(t, account.SetData(ctx.Features, final))
				require.NoError(t, updateCallerAccount(vm, caller, account, false))
				if mapping != "legacy" {
					require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
				}
				account.Drop()
				require.EqualValues(t, len(final), binary.LittleEndian.Uint64(caller.RefToLenInVm))
				serializedLen, err := vm.Read64(caller.VmDataAddr - 8)
				require.NoError(t, err)
				require.EqualValues(t, len(final), serializedLen)
				read := make([]byte, len(final))
				require.NoError(t, vm.Read(caller.VmDataAddr, read))
				require.Equal(t, final, read)
				if mapping != "direct" {
					backing, err := vm.TranslateInput(caller.VmDataAddr, 9)
					require.NoError(t, err)
					require.Equal(t, make([]byte, 9-len(final)), backing[len(final):9])
				}
				if mapping != "legacy" {
					_, err := vm.Read8(caller.VmDataAddr + uint64(len(final)))
					require.Error(t, err)
				}
			})
		}
	}
}

// Agave's test_update_callee_account runs these nested resize/write sequences
// against 10 KiB accounts. Each CPI return must leave the caller's region
// pointing at the latest account data, including after a clone or two no-op
// nested calls. The read-only inner account variant uses the same no-op return
// path here; read-only region permissions are covered by the external-owner
// tests above.
func TestAgaveCPINestedAccountUpdates(t *testing.T) {
	cases := []struct {
		name                         string
		outerLen, outerPre, innerLen int
		innerWrite, noops, outerPost int
		cloneData                    bool
	}{
		{"no_op_nested_calls", 20480, 0, 0, 0, 2, 16384, false},
		{"nested_shrink", 20480, 16384, 19480, 8129, 0, 0, false},
		{"nested_grow", 16384, 0, 20480, 16385, 0, 16384, false},
		{"no_op_calls_then_grow", 16384, 0, 20480, 16385, 2, 16384, false},
		{"cloned_data_before_and_after_cpi", 10240, 8190, 0, 8191, 0, 8190, true},
	}
	for _, mapping := range []string{"legacy", "copied", "direct"} {
		for _, tc := range cases {
			t.Run(mapping+"/"+tc.name, func(t *testing.T) {
				initial := make([]byte, 10240)
				for i := range initial {
					initial[i] = byte(i)
				}
				ctx, vm, caller, _ := agaveCPITestAccount(t, mapping, initial, false)
				outer := make([]byte, tc.outerLen)
				copy(outer, initial)
				if tc.outerPre != 0 {
					outer[tc.outerPre] ^= 0xe5
				}
				agaveCPISetCallerData(t, vm, caller, mapping, outer)
				_, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
				require.NoError(t, err)

				for range tc.noops {
					account := agaveCPIBorrowAccount(t, ctx)
					require.NoError(t, updateCallerAccount(vm, caller, account, false))
					if mapping != "legacy" {
						require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
					}
					account.Drop()
				}

				if tc.innerLen != 0 || tc.innerWrite != 0 {
					account := agaveCPIBorrowAccount(t, ctx)
					if tc.innerLen != 0 {
						require.NoError(t, account.SetDataLength(uint64(tc.innerLen), ctx.Features))
					}
					if tc.innerWrite != 0 {
						data, err := account.DataMutable(ctx.Features)
						require.NoError(t, err)
						data[tc.innerWrite] ^= 0xe5
					}
					require.NoError(t, updateCallerAccount(vm, caller, account, false))
					if mapping != "legacy" {
						require.NoError(t, updateCallerAccountRegion(vm, ctx, caller, account, false))
					}
					account.Drop()
				}

				if tc.outerPost != 0 {
					post := byte(0xe5)
					if tc.outerPost < len(initial) {
						post = initial[tc.outerPost] ^ 0xe5
					}
					if !tc.cloneData {
						current, err := vm.Read8(caller.VmDataAddr + uint64(tc.outerPost))
						require.NoError(t, err)
						post = current ^ 0xe5
					}
					require.NoError(t, vm.Write8(caller.VmDataAddr+uint64(tc.outerPost), post))
					_, err := agaveCPIUpdateCallee(t, ctx, vm, caller, mapping)
					require.NoError(t, err)
				}

				finalLen := tc.outerLen
				if tc.innerLen != 0 {
					finalLen = tc.innerLen
				}
				expected := make([]byte, finalLen)
				copy(expected, initial)
				if tc.outerPre != 0 && tc.outerPre < finalLen {
					expected[tc.outerPre] ^= 0xe5
				}
				if tc.innerWrite != 0 {
					expected[tc.innerWrite] ^= 0xe5
				}
				if tc.outerPost != 0 {
					if tc.cloneData {
						expected[tc.outerPost] = initial[tc.outerPost] ^ 0xe5
					} else {
						expected[tc.outerPost] ^= 0xe5
					}
				}
				require.Equal(t, expected, ctx.TransactionContext.Accounts.Accounts[1].Data)
				require.EqualValues(t, finalLen, binary.LittleEndian.Uint64(caller.RefToLenInVm))
				read := make([]byte, finalLen)
				require.NoError(t, vm.Read(caller.VmDataAddr, read))
				require.Equal(t, expected, read)
				if tc.innerLen != 0 && tc.innerLen < tc.outerLen && mapping != "direct" {
					backing, err := vm.TranslateInput(caller.VmDataAddr, uint64(tc.outerLen))
					require.NoError(t, err)
					require.Equal(t, make([]byte, tc.outerLen-tc.innerLen), backing[tc.innerLen:tc.outerLen])
				}
			})
		}
	}
}
