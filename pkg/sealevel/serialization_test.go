package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func serializationTestContext(tb testing.TB, count int) *ExecutionCtx {
	tb.Helper()
	program := solana.PublicKey{0xff, 0xff}
	accts := []accounts.Account{{Key: program, Owner: a.BpfLoader2Addr, Executable: true, Lamports: 1}}
	metas := make([]AccountMeta, 0, count+1)
	for i := range count {
		key := solana.PublicKey{byte(i), byte(i >> 8)}
		accts = append(accts, accounts.Account{Key: key, Owner: program, Lamports: 1,
			Data: []byte{1, 2, 3, 4, 5}, RentEpoch: 123})
		metas = append(metas, AccountMeta{Pubkey: key, IsSigner: true, IsWritable: true})
	}
	metas = append(metas, metas[0]) // Duplicate encoding must also remain zero padded.
	tx := NewTransactionCtx(*NewTransactionAccounts(accts), 8, 128)
	ix, err := tx.NextInstructionCtx()
	require.NoError(tb, err)
	ix.Configure([]uint64{0}, InstructionAcctsFromAccountMetas(metas, tx.Accounts), []byte{9, 8, 7})
	require.NoError(tb, tx.Push())
	return &ExecutionCtx{TransactionContext: tx, SlotCtx: &SlotCtx{}, Features: *features.NewFeaturesDefault()}
}

func TestSerializeParametersAlignedZeroPadding(t *testing.T) {
	for _, mapping := range []string{"flat", "vasa", "direct"} {
		for _, allocation := range []string{"heap", "arena", "arena_exhausted", "simulation"} {
			t.Run(mapping+"/"+allocation, func(t *testing.T) {
				ctx := serializationTestContext(t, 2)
				if mapping != "flat" {
					ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
				}
				if mapping == "direct" {
					ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
				}
				golden, _, _, _, err := serializeParametersAligned(ctx)
				require.NoError(t, err)
				golden = bytes.Clone(golden)
				var parameterArena *arena.Arena[byte]
				if allocation != "heap" {
					parameterArena = arena.New[byte](uint64(len(golden)))
					ctx.SlotCtx.SerializedParameterArena = parameterArena
				}
				ctx.IsSimulation = allocation == "simulation"
				for round := range 3 {
					if parameterArena != nil {
						// Model guest writes into an earlier invocation's input.
						old, inArena := parameterArena.AllocN(uint64(len(golden)))
						require.True(t, inArena)
						for i := range old {
							old[i] = 0xa5
						}
						if allocation != "arena_exhausted" {
							parameterArena.Reset()
						}
					}
					// Exercise both rent-epoch feature settings with reused storage.
					if round == 1 {
						ctx.Features.EnableFeature(features.MaskOutRentEpochInVmSerialization, 0)
					} else {
						ctx.Features.DisableFeature(features.MaskOutRentEpochInVmSerialization)
					}
					data, _, metas, _, err := serializeParametersAligned(ctx)
					lengths := originalDataLens(metas)
					require.NoError(t, err)
					require.Equal(t, []uint64{5, 5, 5}, lengths)
					offset := 8
					for range 2 {
						require.Equal(t, byte(0xff), data[offset])
						require.Equal(t, make([]byte, 4), data[offset+4:offset+8])
						payload := 8
						if mapping != "direct" {
							payload = 8 + MaxPermittedDataIncrease
							require.Equal(t, []byte{1, 2, 3, 4, 5}, data[offset+88:offset+93])
							require.Equal(t, make([]byte, MaxPermittedDataIncrease+3), data[offset+93:offset+88+payload])
						} else {
							require.Equal(t, make([]byte, 8), data[offset+88:offset+96])
						}
						wantRent := uint64(123)
						if round == 1 {
							wantRent = ^uint64(0)
						}
						require.Equal(t, wantRent, binary.LittleEndian.Uint64(data[offset+88+payload:]))
						offset += 88 + payload + 8
					}
					require.Equal(t, make([]byte, 8), data[offset:offset+8])
					if round != 1 {
						require.Equal(t, golden, data)
					}
					if parameterArena != nil {
						parameterArena.Reset()
					}
				}
			})
		}
	}
}

// Serialization plus reusable-arena cleanup, independent of guest execution.
func BenchmarkSerializeParametersAligned(b *testing.B) {
	for _, count := range []int{1, 128} {
		for _, allocation := range []string{"heap", "arena"} {
			b.Run(fmt.Sprintf("accounts=%d/%s", count, allocation), func(b *testing.B) {
				ctx := serializationTestContext(b, count)
				if allocation == "arena" {
					ctx.SlotCtx.SerializedParameterArena = arena.New[byte](8 << 20)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					data, _, metas, _, err := serializeParametersAligned(ctx)
					lengths := originalDataLens(metas)
					if err != nil || len(lengths) != count+1 || len(data) == 0 {
						b.Fatalf("serialization: %v, accounts=%d, bytes=%d", err, len(lengths), len(data))
					}
					if ctx.SlotCtx.SerializedParameterArena != nil {
						ctx.SlotCtx.SerializedParameterArena.Reset()
					}
				}
			})
		}
	}
}

func originalDataLens(metas []serializedAcctMetadata) []uint64 {
	lens := make([]uint64, len(metas))
	for i, m := range metas {
		lens[i] = m.originalDataLen
	}
	return lens
}

// SIMD-0449: the input ends with the 8-byte aligned VM address of every
// instruction account's header; a duplicate points at the account it repeats.
func TestSerializeParametersAlignedAccountPointers(t *testing.T) {
	for _, mapping := range []string{"flat", "vasa", "direct"} {
		t.Run(mapping, func(t *testing.T) {
			ctx := serializationTestContext(t, 2)
			if mapping != "flat" {
				ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
			}
			if mapping == "direct" {
				ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
			}
			without, _, _, _, err := serializeParametersAligned(ctx)
			require.NoError(t, err)
			without = bytes.Clone(without)

			ctx.Features.EnableFeature(features.DirectAccountPointersInProgramInput, 0)
			data, instrDataOffset, metas, regions, err := serializeParametersAligned(ctx)
			require.NoError(t, err)
			require.Len(t, metas, 3)

			// The program id ends 3 bytes past an 8-byte boundary (data len 3), so 5 bytes
			// of zero padding precede the pointers.
			padding := 8 - len(without)%8
			require.Equal(t, 5, padding)
			require.Equal(t, without, data[:len(without)])
			require.Equal(t, make([]byte, padding), data[len(without):len(without)+padding])
			pointers := data[len(without)+padding:]
			require.Len(t, pointers, 8*len(metas))
			for i, meta := range metas {
				require.Equal(t, meta.vmKeyAddr-8, binary.LittleEndian.Uint64(pointers[8*i:]), "account %d", i)
			}
			require.Equal(t, binary.LittleEndian.Uint64(pointers[0:]), binary.LittleEndian.Uint64(pointers[16:]),
				"the duplicate points at the account it repeats")
			// The pointers follow the instruction data (3 bytes) and program id;
			// offsets are relative to the start of the input region.
			pointersOffset := instrDataOffset + 3 + solana.PublicKeyLength + uint64(padding)
			require.Zero(t, pointersOffset%8)
			require.Equal(t, metas[0].vmKeyAddr-8, sbpf.VaddrInput+8)

			if mapping == "flat" {
				require.Equal(t, pointersOffset, uint64(len(without)+padding))
				for _, meta := range metas {
					require.Equal(t, byte(0xff), data[meta.vmKeyAddr-8-sbpf.VaddrInput])
				}
				return
			}
			// The last region must map the pointer array, or guests fault reading it.
			last := regions[len(regions)-1]
			require.Equal(t, pointersOffset+8*uint64(len(metas)), last.Offset+last.RegionSize)
			require.Equal(t, uint64(len(data)), last.HostOffset+last.RegionSize)
		})
	}
}
