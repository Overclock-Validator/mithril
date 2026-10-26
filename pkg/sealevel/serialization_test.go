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
				golden, _, _, _, _, err := serializeParametersAligned(ctx)
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
					data, lengths, _, _, _, err := serializeParametersAligned(ctx)
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
					data, lengths, _, _, _, err := serializeParametersAligned(ctx)
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
