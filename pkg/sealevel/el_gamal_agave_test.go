package sealevel

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type elGamalVector struct {
	Name        string                  `json:"name"`
	Instruction ElGamalProofInstruction `json:"instruction"`
	Valid       bool                    `json:"valid"`
	ContextSize int                     `json:"context_size"`
	DataHex     string                  `json:"data_hex"`
}

// Program tests port the generic Agave process_transaction.rs helpers to the
// Mithril instruction runtime. The account indices are program, proof data,
// context, authority, destination, and incorrect authority, respectively.
func elGamalTestRuntime(v elGamalVector, wire []byte) *ExecutionCtx {
	accts := []accounts.Account{{Key: a.ZkElgamalProofProgramAddr, Owner: a.NativeLoaderAddr, Executable: true, Lamports: 1}}
	for i := byte(1); i <= 5; i++ {
		accts = append(accts, accounts.Account{Key: solana.PublicKey{i}, Owner: a.SystemProgramAddr, Lamports: 1_000_000})
	}
	accts[1].Data = append(bytes.Repeat([]byte{0xab}, 7), wire[1:]...)
	accts[2].Owner = a.ZkElgamalProofProgramAddr
	accts[2].Data = make([]byte, 33+v.ContextSize)
	return &ExecutionCtx{
		TransactionContext: NewTransactionCtx(*NewTransactionAccounts(accts), 5, 64),
		ComputeMeter:       cu.NewComputeMeter(2_000_000), Features: *features.NewFeaturesDefault(),
	}
}

func elGamalTestInvoke(t *testing.T, ctx *ExecutionCtx, wire []byte, metas ...AccountMeta) error {
	t.Helper()
	err := ctx.ProcessInstruction(wire, InstructionAcctsFromAccountMetas(metas, ctx.TransactionContext.Accounts), []uint64{0})
	for i := range ctx.TransactionContext.Accounts.Accounts {
		require.False(t, ctx.TransactionContext.Accounts.IsLocked(uint64(i)), "account %d left borrowed", i)
	}
	return err
}

func elGamalTestMeta(index byte, writable, signer bool) AccountMeta {
	return AccountMeta{Pubkey: solana.PublicKey{index}, IsWritable: writable, IsSigner: signer}
}

func elGamalValidVectors(t *testing.T) []elGamalVector {
	var result []elGamalVector
	seen := make(map[ElGamalProofInstruction]bool)
	for _, v := range elGamalAgaveVectors(t) {
		if v.Valid && !seen[v.Instruction] {
			result = append(result, v)
			seen[v.Instruction] = true
		}
	}
	require.Len(t, result, 12)
	return result
}

func TestElGamalAgaveProgram(t *testing.T) {
	// These values are the upstream program's constants, deliberately independent
	// of Mithril's CU table so accidental changes are detected.
	costs := [...]uint64{3300, 6000, 8000, 6400, 2600, 6500, 111000, 200000, 368000, 6400, 13000, 8100, 16400}
	for _, v := range elGamalValidVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			wire, err := hex.DecodeString(v.DataHex)
			require.NoError(t, err)
			for _, fromAccount := range []bool{false, true} {
				t.Run(fmt.Sprintf("proof_account=%v", fromAccount), func(t *testing.T) {
					data := wire
					var prefix []AccountMeta
					if fromAccount {
						data = binary.LittleEndian.AppendUint32([]byte{byte(v.Instruction)}, 7)
						prefix = []AccountMeta{elGamalTestMeta(1, false, false)}
					}
					run := func(t *testing.T, ctx *ExecutionCtx, metas ...AccountMeta) error {
						return elGamalTestInvoke(t, ctx, data, append(append([]AccountMeta{}, prefix...), metas...)...)
					}
					t.Run("verify_without_context", func(t *testing.T) {
						ctx := elGamalTestRuntime(v, wire)
						require.NoError(t, run(t, ctx))
						require.Equal(t, costs[v.Instruction], ctx.ComputeMeter.Used())
						// A lone extra account is ignored, including an invalid context.
						require.NoError(t, run(t, ctx, elGamalTestMeta(5, false, false)))
					})
					t.Run("invalid_proof", func(t *testing.T) {
						bad := bytes.Clone(wire)
						for i := len(bad) - 32; i < len(bad); i++ {
							bad[i] = 0xff
						}
						ctx := elGamalTestRuntime(v, bad)
						badData := bad
						if fromAccount {
							badData = data
						}
						require.ErrorIs(t, elGamalTestInvoke(t, ctx, badData, prefix...), InstrErrInvalidInstructionData)
					})
					for _, selfAuthority := range []bool{false, true} {
						t.Run(fmt.Sprintf("context/self_authority=%v", selfAuthority), func(t *testing.T) {
							ctx := elGamalTestRuntime(v, wire)
							authority := byte(3)
							if selfAuthority {
								authority = 2
							}
							metas := []AccountMeta{elGamalTestMeta(2, true, false), elGamalTestMeta(authority, false, false)}
							require.NoError(t, run(t, ctx, metas...))
							state := ctx.TransactionContext.Accounts.Accounts[2]
							want := append(solana.PublicKey{authority}.Bytes(), byte(v.Instruction))
							want = append(want, wire[1:1+v.ContextSize]...)
							require.Equal(t, want, state.Data)
							require.ErrorIs(t, run(t, ctx, metas...), InstrErrAccountAlreadyInitialized)
							// Bad authority, then the correct close in the same runtime.
							require.ErrorIs(t, elGamalTestInvoke(t, ctx, []byte{0}, elGamalTestMeta(2, true, false), elGamalTestMeta(4, true, false), elGamalTestMeta(5, false, true)), InstrErrInvalidAccountOwner)
							require.NoError(t, elGamalTestInvoke(t, ctx, []byte{0}, elGamalTestMeta(2, true, false), elGamalTestMeta(4, true, false), elGamalTestMeta(authority, false, true)))
							require.Empty(t, state.Data)
							require.Zero(t, state.Lamports)
							require.Equal(t, a.SystemProgramAddr, state.Owner)
							require.Equal(t, uint64(2_000_000), ctx.TransactionContext.Accounts.Accounts[4].Lamports)
						})
					}
					for _, tc := range []struct {
						name     string
						mutate   func(*accounts.Account)
						writable bool
						want     error
					}{
						{"wrong_owner", func(a *accounts.Account) { a.Owner = solana.PublicKey{99} }, true, InstrErrInvalidAccountOwner},
						{"short_metadata", func(a *accounts.Account) { a.Data = make([]byte, 32) }, true, InstrErrInvalidAccountData},
						{"short_context", func(a *accounts.Account) { a.Data = a.Data[:len(a.Data)-1] }, true, InstrErrInvalidAccountData},
						{"long_context", func(a *accounts.Account) { a.Data = append(a.Data, 0) }, true, InstrErrInvalidAccountData},
						{"initialized_unknown_tag", func(a *accounts.Account) { a.Data[32] = 255 }, true, InstrErrAccountAlreadyInitialized},
						{"readonly_context", func(a *accounts.Account) {}, false, InstrErrReadonlyDataModified},
					} {
						t.Run(tc.name, func(t *testing.T) {
							ctx := elGamalTestRuntime(v, wire)
							tc.mutate(ctx.TransactionContext.Accounts.Accounts[2])
							require.ErrorIs(t, run(t, ctx, elGamalTestMeta(2, tc.writable, false), elGamalTestMeta(3, false, false)), tc.want)
						})
					}
				})
			}
			t.Run("wrong_instruction_type", func(t *testing.T) {
				for id := byte(1); id <= 12; id++ {
					if id == byte(v.Instruction) {
						continue
					}
					bad := bytes.Clone(wire)
					bad[0] = id
					require.ErrorIs(t, elGamalTestInvoke(t, elGamalTestRuntime(v, wire), bad), InstrErrInvalidInstructionData)
				}
			})
			t.Run("compute_budget", func(t *testing.T) {
				ctx := elGamalTestRuntime(v, wire)
				ctx.ComputeMeter = cu.NewComputeMeter(costs[v.Instruction] - 1)
				require.ErrorIs(t, elGamalTestInvoke(t, ctx, wire), InstrErrComputationalBudgetExceeded)
			})
		})
	}
}

func TestElGamalAccountAndInstructionEdges(t *testing.T) {
	v := elGamalValidVectors(t)[0]
	wire, err := hex.DecodeString(v.DataHex)
	require.NoError(t, err)
	for _, offset := range []uint32{8, math.MaxUint32} {
		ctx := elGamalTestRuntime(v, wire)
		data := binary.LittleEndian.AppendUint32([]byte{byte(v.Instruction)}, offset)
		require.ErrorIs(t, elGamalTestInvoke(t, ctx, data, elGamalTestMeta(1, false, false)), InstrErrInvalidAccountData)
	}
	for _, data := range [][]byte{nil, {255}, {byte(v.Instruction)}, wire[:len(wire)-1], append(bytes.Clone(wire), 0)} {
		require.ErrorIs(t, elGamalTestInvoke(t, elGamalTestRuntime(v, wire), data), InstrErrInvalidInstructionData)
	}
	ctx := elGamalTestRuntime(v, wire)
	ctx.Features.EnableFeature(features.DisableZkElgamalProofProgram, 0)
	require.ErrorIs(t, elGamalTestInvoke(t, ctx, wire), InstrErrInvalidInstructionData)
	require.Zero(t, ctx.ComputeMeter.Used())
	ctx.Features.EnableFeature(features.ReenableZkElgamalProofProgram, 0)
	require.NoError(t, elGamalTestInvoke(t, ctx, wire))
	// Proof data and output context may alias: validation must finish before the
	// output is borrowed. The wrong output size wins over a borrow error.
	ctx = elGamalTestRuntime(v, wire)
	ctx.TransactionContext.Accounts.Accounts[1].Owner = a.ZkElgamalProofProgramAddr
	ctx.TransactionContext.Accounts.Accounts[1].Data = append(make([]byte, 33), wire[1:]...)
	data := binary.LittleEndian.AppendUint32([]byte{byte(v.Instruction)}, 33)
	require.ErrorIs(t, elGamalTestInvoke(t, ctx, data, elGamalTestMeta(1, true, false), elGamalTestMeta(1, true, false), elGamalTestMeta(3, false, false)), InstrErrInvalidAccountData)
}

func TestElGamalCloseContext(t *testing.T) {
	v := elGamalValidVectors(t)[0]
	wire, err := hex.DecodeString(v.DataHex)
	require.NoError(t, err)
	for _, tc := range []struct {
		name        string
		mutate      func(*ExecutionCtx)
		destination byte
		signer      bool
		want        error
	}{
		{"success", func(*ExecutionCtx) {}, 4, true, nil},
		{"authority_is_destination", func(*ExecutionCtx) {}, 3, true, nil},
		{"context_is_destination", func(*ExecutionCtx) {}, 2, true, InstrErrInvalidInstructionData},
		{"missing_signature", func(*ExecutionCtx) {}, 4, false, InstrErrMissingRequiredSignature},
		{"wrong_owner", func(c *ExecutionCtx) { c.TransactionContext.Accounts.Accounts[2].Owner = a.SystemProgramAddr }, 4, true, InstrErrInvalidAccountOwner},
		{"short_metadata", func(c *ExecutionCtx) { c.TransactionContext.Accounts.Accounts[2].Data = make([]byte, 32) }, 4, true, InstrErrInvalidAccountData},
		{"uninitialized", func(c *ExecutionCtx) { c.TransactionContext.Accounts.Accounts[2].Data[32] = 0 }, 4, true, InstrErrUninitializedAccount},
		{"nonzero_unknown_tag", func(c *ExecutionCtx) { c.TransactionContext.Accounts.Accounts[2].Data[32] = 255 }, 4, true, nil},
		{"overflow", func(c *ExecutionCtx) { c.TransactionContext.Accounts.Accounts[4].Lamports = math.MaxUint64 }, 4, true, InstrErrArithmeticOverflow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := elGamalTestRuntime(v, wire)
			require.NoError(t, elGamalTestInvoke(t, ctx, wire, elGamalTestMeta(2, true, false), elGamalTestMeta(3, false, false)))
			tc.mutate(ctx)
			ctx.ComputeMeter = cu.NewComputeMeter(3300)
			err := elGamalTestInvoke(t, ctx, []byte{0}, elGamalTestMeta(2, true, false), elGamalTestMeta(tc.destination, true, false), elGamalTestMeta(3, false, tc.signer))
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			require.Equal(t, uint64(3300), ctx.ComputeMeter.Used())
		})
	}
}

func elGamalAgaveVectors(t *testing.T) []elGamalVector {
	t.Helper()
	raw, err := os.ReadFile("testdata/elgamal_agave_vectors.json")
	require.NoError(t, err)
	var fixture struct {
		Cases []elGamalVector `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.NotEmpty(t, fixture.Cases)
	return fixture.Cases
}

// These bytes and expected outcomes come from Agave's pinned solana-zk-sdk,
// using the inputs from its program tests. See conformance/elgamal-fixtures.
func TestElGamalAgaveProofVectors(t *testing.T) {
	for _, v := range elGamalAgaveVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			wire, err := hex.DecodeString(v.DataHex)
			require.NoError(t, err)
			err = processElGamalProofInstr(wire, nil)
			if v.Valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, InstrErrInvalidInstructionData)
			}
		})
	}
}
