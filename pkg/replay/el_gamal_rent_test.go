package replay

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

// Agave checks rent at the transaction boundary, outside the proof program.
// Port its insufficient-rent and create/verify/close-in-one-transaction cases
// through Mithril's transaction runtime, for every proof type.
func TestElGamalAgaveContextRent(t *testing.T) {
	raw, err := os.ReadFile("../sealevel/testdata/elgamal_agave_vectors.json")
	require.NoError(t, err)
	var fixture struct {
		Cases []struct {
			Name        string `json:"name"`
			Instruction byte   `json:"instruction"`
			Valid       bool   `json:"valid"`
			ContextSize uint64 `json:"context_size"`
			DataHex     string `json:"data_hex"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	seen := make(map[byte]bool)
	for _, v := range fixture.Cases {
		if !v.Valid || seen[v.Instruction] {
			continue
		}
		seen[v.Instruction] = true
		t.Run(v.Name, func(t *testing.T) {
			wire, err := hex.DecodeString(v.DataHex)
			require.NoError(t, err)
			for _, mode := range []string{"rent_exempt", "insufficient_rent", "close_without_rent"} {
				t.Run(mode, func(t *testing.T) {
					env := newGroupExecutionEnv(t, 0, 1_000_000_000)
					defer env.cleanup()
					program := &accounts.Account{Key: addresses.ZkElgamalProofProgramAddr, Owner: addresses.NativeLoaderAddr, Executable: true, Lamports: 1}
					require.NoError(t, env.source.mem.SetAccountWithoutLock(program.Key, program))
					budget := &accounts.Account{Key: solana.ComputeBudget, Owner: addresses.NativeLoaderAddr, Executable: true, Lamports: 1}
					require.NoError(t, env.source.mem.SetAccountWithoutLock(budget.Key, budget))
					context := solana.PublicKey{0xec, v.Instruction}
					proofAccount := solana.PublicKey{0xed, v.Instruction}
					// Large range proofs plus account creation exceed packet size
					// inline. Exercise the proof-account form at the tx boundary.
					require.NoError(t, env.source.mem.SetAccountWithoutLock(proofAccount, &accounts.Account{
						Key: proofAccount, Owner: addresses.SystemProgramAddr, Lamports: 10_000_000, Data: wire[1:],
					}))
					payer := txfixture.PayerPubkey()
					rent := sealevel.NewDefaultRentSysvar()
					funds := rent.MinimumBalance(33 + v.ContextSize)
					if mode == "insufficient_rent" {
						funds--
					}
					if mode == "close_without_rent" {
						funds = 1
					}
					instructions := []solana.Instruction{
						computebudget.NewSetComputeUnitLimitInstruction(500_000).Build(),
						system.NewCreateAccountInstruction(funds, 33+v.ContextSize, program.Key, payer, context).Build(),
						solana.NewInstruction(program.Key, solana.AccountMetaSlice{
							{PublicKey: proofAccount},
							{PublicKey: context, IsWritable: true}, {PublicKey: payer},
						}, binary.LittleEndian.AppendUint32([]byte{v.Instruction}, 0)),
					}
					if mode == "close_without_rent" {
						instructions = append(instructions, solana.NewInstruction(program.Key, solana.AccountMetaSlice{
							{PublicKey: context, IsWritable: true}, {PublicKey: payer, IsWritable: true}, {PublicKey: payer, IsSigner: true},
						}, []byte{0}))
					}
					tx, err := solana.NewTransaction(instructions, txfixture.TestBlockhash(), solana.TransactionPayer(payer))
					require.NoError(t, err)
					// Signature cryptography is outside this transaction-runtime test.
					tx.Signatures = make([]solana.Signature, tx.Message.Header.NumRequiredSignatures)
					for _, key := range tx.Message.AccountKeys {
						acct, err := env.source.GetAccount(42, key)
						require.NoError(t, err)
						require.NoError(t, env.parent.SetAccountWithoutLock(key, acct))
					}
					out := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{SlotCtx: env.exec.slotCtx, Transaction: tx})
					if mode == "insufficient_rent" {
						require.NotNil(t, out.ProcessingResult.TransactionError)
						require.Equal(t, TransactionErrorInsufficientFundsForRent, out.ProcessingResult.TransactionError.ErrorType)
					} else {
						require.Nil(t, out.ProcessingResult.TransactionError, "%+v", out.ProcessingResult.TransactionError)
						for _, acct := range out.ExecCtx.TransactionContext.Accounts.Accounts {
							if acct.Key != context {
								continue
							}
							if mode == "close_without_rent" {
								require.Zero(t, acct.Lamports)
								require.Empty(t, acct.Data)
							} else {
								require.Equal(t, wire[1:1+v.ContextSize], acct.Data[33:])
							}
						}
					}
				})
			}
		})
	}
	require.Len(t, seen, 12)
}
