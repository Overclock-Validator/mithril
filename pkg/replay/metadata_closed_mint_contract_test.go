package replay

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	token2022 "github.com/gagliardetto/solana-go/programs/token-2022"
	"github.com/stretchr/testify/require"
)

// Exercise real Token-2022 initialization and close instructions. Closing a
// zero-supply mint does not close its zero-balance token accounts.
func TestTransactionMetadataClosedMintAccountClose(t *testing.T) {
	dir := os.Getenv("MITHRIL_PROGRAM_BENCH_DIR")
	if dir == "" {
		t.Skip("set MITHRIL_PROGRAM_BENCH_DIR to the pinned external program fixtures")
	}
	elf, err := os.ReadFile(filepath.Join(dir, "token2022.so"))
	require.NoError(t, err)
	require.Equal(t, "a794161408080f690dac00832f45b3c3e2b71f1339586667ad1f979cf91d5b68", fmt.Sprintf("%x", sha256.Sum256(elf)))
	var baseline []*accounts.Account
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("capture=%t", enabled), func(t *testing.T) {
			ctx, _, _ := transactionMetadataCPIFixture(t, false)
			put := func(key, owner solana.PublicKey, data []byte, executable bool) {
				require.NoError(t, ctx.SetAccount(key, &accounts.Account{
					Key: key, Owner: owner, Lamports: 100_000_000, Data: data,
					Executable: executable, RentEpoch: ^uint64(0),
				}))
			}
			payer := txfixture.PayerPubkey()
			mint, tokenAccount := solana.PublicKey{0x71}, solana.PublicKey{0x73}
			put(solana.Token2022ProgramID, addresses.BpfLoader2Addr, elf, true)
			put(mint, solana.Token2022ProgramID, make([]byte, 202), false)
			put(tokenAccount, solana.Token2022ProgramID, make([]byte, 165), false)
			setup := signLifecycleInstructions(t,
				token2022.NewInitializeMintCloseAuthorityInstruction(payer, mint).Build(),
				token2022.NewInitializeMint2Instruction(6, payer, payer, mint).Build(),
				token2022.NewInitializeAccount3Instruction(payer, tokenAccount, mint).Build())
			_, _, err := processTransaction(ctx, &sync.WaitGroup{}, setup, nil, nil, nil, false, nil)
			require.NoError(t, err)
			closeMint := signLifecycleInstructions(t, token2022.NewCloseAccountInstruction(mint, payer, payer, nil).Build())
			_, _, err = processTransaction(ctx, &sync.WaitGroup{}, closeMint, nil, nil, nil, false, nil)
			require.NoError(t, err, "zero-supply mint closes through the real program")
			closed, err := ctx.GetAccount(mint)
			require.NoError(t, err)
			require.Zero(t, closed.Lamports)
			remaining, err := ctx.GetAccount(tokenAccount)
			require.NoError(t, err)
			require.NotZero(t, remaining.Lamports)
			require.Equal(t, byte(1), remaining.Data[108])
			tx := signLifecycleInstructions(t, token2022.NewCloseAccountInstruction(tokenAccount, payer, payer, nil).Build())
			var capture *blockMetadataCapture
			var group *transactionMetadataGroup
			if enabled {
				capture = &blockMetadataCapture{}
				group = capture.beginGroup(ctx, []*solana.Transaction{tx}, 1)
				require.NoError(t, capture.Err)
			}
			fee, _, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, group.transaction(0))
			require.NoError(t, err, "remaining zero-balance token account closes successfully")
			require.Equal(t, uint64(5000), fee.TotalFee)
			state := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
			if !enabled {
				baseline = state
				return
			}
			require.Equal(t, baseline, state, "metadata must not change execution")
			require.NoError(t, group.captures[0].Err)
			require.JSONEq(t, "null", string(group.captures[0].Record.Error))
			capture.finishGroup(group)
			require.NoError(t, capture.Err, "known absent mint omits token-balance metadata, not the whole successful transaction")
			require.Len(t, capture.records, 1)
			require.Empty(t, capture.records[0].PreTokenBalances)
			require.Empty(t, capture.records[0].PostTokenBalances)
		})
	}
}
