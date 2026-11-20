package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Agave v4.4.1 program-runtime/src/serialization.rs tests both the account
// limit and the direct-account-pointer flag with VASA enabled and disabled.
func TestAgave0460SerializeAccountCountLimit(t *testing.T) {
	for _, vasa := range []bool{false, true} {
		for _, pointers := range []bool{false, true} {
			for _, tc := range []struct {
				name     string
				unique   int
				keepDupe bool
				wantErr  bool
			}{
				{"max_unique", MaxInstructionAccounts, false, false},
				{"too_many_unique", MaxInstructionAccounts + 1, false, true},
				{"max_plus_duplicate", MaxInstructionAccounts, true, true},
			} {
				t.Run(fmt.Sprintf("vasa=%t/pointers=%t/%s", vasa, pointers, tc.name), func(t *testing.T) {
					ctx := serializationTestContext(t, tc.unique)
					ix, err := ctx.TransactionContext.CurrentInstructionCtx()
					require.NoError(t, err)
					if !tc.keepDupe {
						ix.InstructionAccounts = ix.InstructionAccounts[:len(ix.InstructionAccounts)-1]
					}
					if vasa {
						ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
					}
					if pointers {
						ctx.Features.EnableFeature(features.DirectAccountPointersInProgramInput, 0)
					}
					data, _, metas, _, err := serializeParametersAligned(ctx)
					if tc.wantErr {
						require.ErrorIs(t, err, InstrErrMaxAccountsExceeded)
						return
					}
					require.NoError(t, err)
					require.Len(t, metas, MaxInstructionAccounts)
					require.EqualValues(t, MaxInstructionAccounts, binary.LittleEndian.Uint64(data[:8]))
				})
			}
		}
	}
}

// Agave's serialize_parameters and mask_out_rent_epoch tests use six distinct
// accounts with lengths 5, 9, 0, 5, 9, 0 and the instruction-account order
// [1, 1, 2, 3, 4, 4, 5, 6]. Exercise both ABIs and both feature flags, then
// verify every serialized field and the no-write deserialization round trip.
func TestAgave0460SerializeRoundTripMixedAccounts(t *testing.T) {
	for _, aligned := range []bool{false, true} {
		for _, vasa := range []bool{false, true} {
			for _, pointers := range []bool{false, true} {
				for _, maskRent := range []bool{false, true} {
					t.Run(fmt.Sprintf("aligned=%t/vasa=%t/pointers=%t/mask_rent=%t", aligned, vasa, pointers, maskRent), func(t *testing.T) {
						ctx := serializationTestContext(t, 6)
						tx := ctx.TransactionContext
						dataByAccount := [][]byte{
							{1, 2, 3, 4, 5}, {11, 12, 13, 14, 15, 16, 17, 18, 19}, {},
							{1, 2, 3, 4, 5}, {11, 12, 13, 14, 15, 16, 17, 18, 19}, {},
						}
						for i, account := range tx.Accounts.Accounts[1:] {
							account.Data = dataByAccount[i]
							account.Lamports = uint64(i + 1)
							account.RentEpoch = []uint64{100, 200, 3100, 100, 200, 3100}[i]
							account.Executable = i == 1 || i == 4
						}
						ix, err := tx.CurrentInstructionCtx()
						require.NoError(t, err)
						order := []int{1, 1, 2, 3, 4, 4, 5, 6}
						ixMetas := make([]AccountMeta, len(order))
						for i, index := range order {
							ixMetas[i] = AccountMeta{
								Pubkey:     tx.Accounts.Accounts[index].Key,
								IsSigner:   index%2 == 0,
								IsWritable: index >= 4,
							}
						}
						ix.InstructionAccounts = InstructionAcctsFromAccountMetas(ixMetas, tx.Accounts)
						ix.Data = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
						if vasa {
							ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
						}
						if pointers {
							ctx.Features.EnableFeature(features.DirectAccountPointersInProgramInput, 0)
						}
						if maskRent {
							ctx.Features.EnableFeature(features.MaskOutRentEpochInVmSerialization, 0)
						}
						original := make([]struct {
							data       []byte
							lamports   uint64
							owner      [32]byte
							executable bool
							rentEpoch  uint64
						}, len(tx.Accounts.Accounts))
						for i, account := range tx.Accounts.Accounts {
							original[i].data = bytes.Clone(account.Data)
							original[i].lamports = account.Lamports
							original[i].owner = account.Owner
							original[i].executable = account.Executable
							original[i].rentEpoch = account.RentEpoch
						}
						var data []byte
						var instructionDataOffset uint64
						var metas []serializedAcctMetadata
						if aligned {
							data, instructionDataOffset, metas, _, err = serializeParametersAligned(ctx)
						} else {
							data, instructionDataOffset, metas, _, err = serializeParametersUnaligned(ctx)
						}
						require.NoError(t, err)
						require.Len(t, metas, len(order))
						require.EqualValues(t, len(order), binary.LittleEndian.Uint64(data[:8]))
						require.Equal(t, metas[0], metas[1], "first duplicate points to account 0")
						require.Equal(t, metas[4], metas[5], "second duplicate points to account 4")
						cursor := uint64(8)
						firstSeen := make(map[int]int)
						for i, index := range order {
							if first, duplicate := firstSeen[index]; duplicate {
								require.Equal(t, byte(first), data[cursor])
								if aligned {
									require.Equal(t, make([]byte, 7), data[cursor+1:cursor+8])
									cursor += 8
								} else {
									cursor++
								}
								continue
							}
							firstSeen[index] = i
							account := tx.Accounts.Accounts[index]
							meta := metas[i]
							require.Equal(t, byte(0xff), data[cursor])
							require.Equal(t, byte(boolByte(ixMetas[i].IsSigner)), data[cursor+1])
							require.Equal(t, byte(boolByte(ixMetas[i].IsWritable)), data[cursor+2])
							require.Equal(t, account.Key[:], data[meta.vmKeyAddr-sbpf.VaddrInput:meta.vmKeyAddr-sbpf.VaddrInput+solana.PublicKeyLength])
							require.Equal(t, account.Owner[:], data[meta.vmOwnerAddr-sbpf.VaddrInput:meta.vmOwnerAddr-sbpf.VaddrInput+solana.PublicKeyLength])
							require.Equal(t, account.Lamports, binary.LittleEndian.Uint64(data[meta.vmLamportsAddr-sbpf.VaddrInput:]))
							require.EqualValues(t, len(account.Data), binary.LittleEndian.Uint64(data[meta.vmDataAddr-sbpf.VaddrInput-8:]))
							require.Equal(t, account.Data, data[meta.vmDataAddr-sbpf.VaddrInput:meta.vmDataAddr-sbpf.VaddrInput+uint64(len(account.Data))])
							wantRent := account.RentEpoch
							if maskRent {
								wantRent = ^uint64(0)
							}
							if aligned {
								require.Equal(t, byte(boolByte(account.Executable)), data[cursor+3])
								require.Equal(t, make([]byte, 4), data[cursor+4:cursor+8])
								require.Zero(t, meta.vmLamportsAddr%8)
								require.Zero(t, meta.vmDataAddr%8)
								rentOffset := meta.vmDataAddr - sbpf.VaddrInput + uint64(len(account.Data)) + MaxPermittedDataIncrease
								rentOffset += (8 - uint64(len(account.Data))%8) % 8
								require.Equal(t, wantRent, binary.LittleEndian.Uint64(data[rentOffset:]))
								cursor = rentOffset + 8
							} else {
								executableOffset := meta.vmOwnerAddr - sbpf.VaddrInput + solana.PublicKeyLength
								require.Equal(t, byte(boolByte(account.Executable)), data[executableOffset])
								require.Equal(t, wantRent, binary.LittleEndian.Uint64(data[executableOffset+1:]))
								cursor = executableOffset + 1 + 8
							}
						}
						require.Equal(t, cursor+8, instructionDataOffset)
						require.EqualValues(t, len(ix.Data), binary.LittleEndian.Uint64(data[cursor:]))
						require.Equal(t, ix.Data, data[instructionDataOffset:instructionDataOffset+uint64(len(ix.Data))])
						programOffset := instructionDataOffset + uint64(len(ix.Data))
						require.Equal(t, tx.Accounts.Accounts[0].Key[:], data[programOffset:programOffset+solana.PublicKeyLength])
						if aligned && pointers {
							pointerStart := (programOffset + solana.PublicKeyLength + 7) &^ 7
							require.Equal(t, make([]byte, pointerStart-programOffset-solana.PublicKeyLength), data[programOffset+solana.PublicKeyLength:pointerStart])
							for i, meta := range metas {
								require.Equal(t, meta.vmKeyAddr-8, binary.LittleEndian.Uint64(data[pointerStart+8*uint64(i):]))
							}
							require.Equal(t, pointerStart+8*uint64(len(metas)), uint64(len(data)))
						} else {
							require.Equal(t, programOffset+solana.PublicKeyLength, uint64(len(data)))
						}
						if aligned {
							err = deserializeParametersAligned(ctx, data, metas)
						} else {
							err = deserializeParametersUnaligned(ctx, data, metas)
						}
						require.NoError(t, err)
						for i, account := range tx.Accounts.Accounts {
							require.Truef(t, bytes.Equal(original[i].data, account.Data), "account %d data changed", i)
							require.Equal(t, original[i].lamports, account.Lamports, "account %d lamports", i)
							require.Equal(t, original[i].owner, account.Owner, "account %d owner", i)
							require.Equal(t, original[i].executable, account.Executable, "account %d executable", i)
							require.Equal(t, original[i].rentEpoch, account.RentEpoch, "account %d rent epoch", i)
						}
					})
				}
			}
		}
	}
}

func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
