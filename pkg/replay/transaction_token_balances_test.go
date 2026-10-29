package replay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func tokenCaptureMint(key, program solana.PublicKey, decimals byte, size int) *accounts.Account {
	account := &accounts.Account{Key: key, Owner: program, Lamports: 1, Data: make([]byte, size)}
	account.Data[44], account.Data[45] = decimals, 1
	if size > 165 {
		account.Data[165] = 1
	}
	return account
}

func tokenCaptureAccount(key, mint, program solana.PublicKey, amount uint64, size int) *accounts.Account {
	account := &accounts.Account{Key: key, Owner: program, Lamports: 1, Data: make([]byte, size)}
	copy(account.Data[:32], mint[:])
	account.Data[32] = 99
	binary.LittleEndian.PutUint64(account.Data[64:72], amount)
	account.Data[108] = 1
	if size > 165 {
		account.Data[165] = 2
	}
	return account
}

func tokenCaptureContext(t *testing.T, initial ...*accounts.Account) *sealevel.SlotCtx {
	t.Helper()
	ctx := &sealevel.SlotCtx{
		Accounts: accounts.NewMemAccounts(),
		UnrootedRead: metadataAccountReader(func(uint64, solana.PublicKey) (*accounts.Account, error) {
			return nil, accountsdb.ErrNoAccount
		}),
	}
	for _, account := range initial {
		require.NoError(t, ctx.SetAccount(account.Key, account))
	}
	return ctx
}

// These transactions test the capture contract; they are not executed programs.
func tokenCaptureTransaction(keys ...solana.PublicKey) *solana.Transaction {
	keys = append(keys, solana.TokenProgramID)
	return &solana.Transaction{
		Signatures: []solana.Signature{{}},
		Message: solana.Message{
			Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
			AccountKeys:  keys,
			Instructions: []solana.CompiledInstruction{{ProgramIDIndex: uint16(len(keys) - 1), Accounts: []uint16{0}}},
		},
	}
}

func TestTransactionTokenBaseLayouts(t *testing.T) {
	for _, program := range []solana.PublicKey{solana.TokenProgramID, solana.Token2022ProgramID} {
		for _, size := range []int{0, 44, 81, 82, 83, 164, 165, 166, 200, 354, 355, 356} {
			t.Run(fmt.Sprintf("%s/size%d", program, size), func(t *testing.T) {
				account := &accounts.Account{Owner: program, Lamports: 1, Data: make([]byte, size)}
				if size > 45 {
					account.Data[45] = 1
				}
				if size > 108 {
					account.Data[108] = 1
				}
				if size > 165 {
					account.Data[165] = 1
				}
				mint := decodeTokenMint(account)
				extended := program == solana.Token2022ProgramID && size > 165 && size != 355
				require.Equal(t, size == 82 || extended, mint.Valid)
				require.Equal(t, extended, mint.HasExtensions)
				if size > 165 {
					account.Data[165] = 2
					require.False(t, decodeTokenMint(account).Valid)
				}
				_, ok := decodeTokenAccount(account)
				require.Equal(t, size == 165 || extended, ok)
				account.Owner = solana.SystemProgramID
				require.False(t, decodeTokenMint(account).Valid)
				_, ok = decodeTokenAccount(account)
				require.False(t, ok)
			})
		}
	}
	mint := tokenCaptureMint(solana.PublicKey{1}, solana.TokenProgramID, 0, 82)
	require.True(t, decodeTokenMint(mint).Valid, "zero decimals is a real value")
	mint.Data[45] = 0
	require.False(t, decodeTokenMint(mint).Valid)
	mint.Data[45], mint.Lamports = 1, 0
	require.False(t, decodeTokenMint(mint).Valid, "zero-lamport mint is a tombstone")
	account := tokenCaptureAccount(solana.PublicKey{2}, mint.Key, solana.TokenProgramID, math.MaxUint64, 165)
	fact, ok := decodeTokenAccount(account)
	require.True(t, ok)
	require.Equal(t, "18446744073709551615", strconv.FormatUint(fact.Amount, 10))
	account.Data[0], account.Data[32], account.Data[64] = 9, 9, 0
	require.Equal(t, mint.Key, fact.Mint)
	require.Equal(t, byte(99), fact.Owner[0])
	require.Equal(t, uint64(math.MaxUint64), fact.Amount)
	account.Data[108] = 0
	_, ok = decodeTokenAccount(account)
	require.False(t, ok)
	account.Data[108], account.Lamports = 1, 0
	_, ok = decodeTokenAccount(account)
	require.False(t, ok)
	// The target collector uses nonzero state bytes, not Token Pack enum checks.
	for _, state := range []byte{2, 255} {
		mint.Data[45], mint.Lamports = state, 1
		require.True(t, decodeTokenMint(mint).Valid)
		account.Data[108], account.Lamports = state, 1
		_, ok = decodeTokenAccount(account)
		require.True(t, ok)
	}
}

func TestTransactionTokenOrderedExternalMint(t *testing.T) {
	payerA, payerB, key, mintKey := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}, solana.PublicKey{4}
	mint := tokenCaptureMint(mintKey, solana.TokenProgramID, 6, 82)
	account := tokenCaptureAccount(key, mintKey, solana.TokenProgramID, math.MaxUint64, 165)
	ctx := tokenCaptureContext(t, mint, account)
	transfer := tokenCaptureTransaction(payerA, key) // The mint is deliberately absent.
	changeMint := tokenCaptureTransaction(payerB, mintKey)
	group, err := prepareTransactionTokenGroup(ctx, []*solana.Transaction{transfer, changeMint})
	require.NoError(t, err)
	preTransfer, err := captureTransactionTokenSnapshot(ctx, transfer)
	require.NoError(t, err)
	preMint, err := captureTransactionTokenSnapshot(ctx, changeMint)
	require.NoError(t, err)
	// The later-index mint writer completes first, as the declared keys permit.
	require.NoError(t, ctx.SetAccount(mintKey, tokenCaptureMint(mintKey, solana.TokenProgramID, 9, 82)))
	postMint, err := captureTransactionTokenSnapshot(ctx, changeMint)
	require.NoError(t, err)
	require.NoError(t, ctx.SetAccount(key, tokenCaptureAccount(key, mintKey, solana.TokenProgramID, 7, 165)))
	postTransfer, err := captureTransactionTokenSnapshot(ctx, transfer)
	require.NoError(t, err)
	before, after, err := group.resolve(preTransfer, postTransfer)
	require.NoError(t, err)
	require.Equal(t, uint8(6), before[0].Decimals)
	require.Equal(t, uint8(6), after[0].Decimals, "live group-final mint must not leak into earlier tx")
	require.Equal(t, uint64(math.MaxUint64), before[0].Amount)
	require.Equal(t, uint64(7), after[0].Amount)
	_, _, err = group.resolve(preMint, postMint)
	require.NoError(t, err)
	require.Equal(t, uint8(9), group.mints[mintKey].Decimals)
	_, _, err = group.resolve(preMint, postMint)
	require.ErrorContains(t, err, "out of order")
}

func TestTransactionTokenMintLifecycleEveryGroupSplit(t *testing.T) {
	payer, key, mintKey := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	transactions := []*solana.Transaction{
		tokenCaptureTransaction(payer, mintKey),
		tokenCaptureTransaction(payer, key),
		tokenCaptureTransaction(payer, mintKey),
		tokenCaptureTransaction(payer, mintKey),
		tokenCaptureTransaction(payer, key),
		tokenCaptureTransaction(payer, mintKey),
	}
	// This is an ordered account-publication model, not a substitute for program
	// execution tests. The final step models a failed write's committed rollback.
	for splits := 0; splits < 1<<(len(transactions)-1); splits++ {
		t.Run(fmt.Sprintf("splits%05b", splits), func(t *testing.T) {
			ctx := tokenCaptureContext(t, tokenCaptureAccount(key, mintKey, solana.TokenProgramID, 10, 165))
			for start := 0; start < len(transactions); {
				end := start + 1
				for end < len(transactions) && splits&(1<<(end-1)) == 0 {
					end++
				}
				group, err := prepareTransactionTokenGroup(ctx, transactions[start:end])
				require.NoError(t, err)
				var before, after []transactionTokenSnapshot
				for i := start; i < end; i++ {
					pre, err := captureTransactionTokenSnapshot(ctx, transactions[i])
					require.NoError(t, err)
					switch i {
					case 0:
						require.NoError(t, ctx.SetAccount(mintKey, tokenCaptureMint(mintKey, solana.TokenProgramID, 0, 82)))
					case 1, 4:
						require.NoError(t, ctx.SetAccount(key, tokenCaptureAccount(key, mintKey, solana.TokenProgramID, uint64(10+i), 165)))
					case 2:
						require.NoError(t, ctx.SetAccount(mintKey, &accounts.Account{Key: mintKey}))
					case 3:
						require.NoError(t, ctx.SetAccount(mintKey, tokenCaptureMint(mintKey, solana.TokenProgramID, 9, 82)))
					case 5:
						// No speculative mint reaches the committed account view.
					}
					post, err := captureTransactionTokenSnapshot(ctx, transactions[i])
					require.NoError(t, err)
					before, after = append(before, pre), append(after, post)
				}
				for i := range before {
					pre, post, err := group.resolve(before[i], after[i])
					require.NoError(t, err)
					if index := start + i; index == 1 || index == 4 {
						want := uint8(0)
						if index == 4 {
							want = 9
						}
						require.Equal(t, want, pre[0].Decimals)
						require.Equal(t, want, post[0].Decimals)
					} else {
						require.Empty(t, pre)
						require.Empty(t, post)
					}
				}
				start = end
			}
		})
	}
}

func TestTransactionTokenUnavailableAndNativeMint(t *testing.T) {
	payer, key := solana.PublicKey{1}, solana.PublicKey{2}
	tx := tokenCaptureTransaction(payer, key)
	ctx := tokenCaptureContext(t, tokenCaptureAccount(key, solana.WrappedSol, solana.TokenProgramID, 10, 165))
	group, err := prepareTransactionTokenGroup(ctx, []*solana.Transaction{tx})
	require.NoError(t, err)
	snapshot, err := captureTransactionTokenSnapshot(ctx, tx)
	require.NoError(t, err)
	pre, postBalances, err := group.resolve(snapshot, snapshot)
	require.NoError(t, err)
	require.Empty(t, pre, "known absent native mint does not imply invented decimals")
	require.Empty(t, postBalances)
	require.NoError(t, ctx.SetAccount(solana.WrappedSol, tokenCaptureMint(solana.WrappedSol, solana.TokenProgramID, 9, 82)))
	group, err = prepareTransactionTokenGroup(ctx, []*solana.Transaction{tx})
	require.NoError(t, err)
	pre, _, err = group.resolve(snapshot, snapshot)
	require.NoError(t, err)
	require.Equal(t, uint8(9), pre[0].Decimals)
	post := snapshot
	post.Balances = append([]transactionTokenFact(nil), snapshot.Balances...)
	post.Balances[0].Mint = solana.PublicKey{99}
	_, _, err = group.resolve(snapshot, post)
	require.ErrorContains(t, err, "unavailable", "unknown post-only mint must not trigger a live lookup")
	require.Equal(t, uint8(9), group.mints[solana.WrappedSol].Decimals, "failed resolution leaves shadow unchanged")
	group.mints[solana.WrappedSol] = decodeTokenMint(tokenCaptureMint(solana.WrappedSol, solana.Token2022ProgramID, 9, 166))
	pre, postBalances, err = group.resolve(snapshot, snapshot)
	require.NoError(t, err)
	require.Empty(t, pre, "known mint owner mismatch is not a token balance")
	require.Empty(t, postBalances)
	lookupErr := errors.New("mint storage read failed")
	ctx = tokenCaptureContext(t, tokenCaptureAccount(key, solana.WrappedSol, solana.TokenProgramID, 10, 165))
	ctx.UnrootedRead = metadataAccountReader(func(_ uint64, requested solana.PublicKey) (*accounts.Account, error) {
		if requested == solana.WrappedSol {
			return nil, lookupErr
		}
		return nil, accountsdb.ErrNoAccount
	})
	_, err = prepareTransactionTokenGroup(ctx, []*solana.Transaction{tx})
	require.ErrorIs(t, err, lookupErr)
	ctx = tokenCaptureContext(t,
		tokenCaptureAccount(key, solana.WrappedSol, solana.Token2022ProgramID, math.MaxUint64, 166),
		tokenCaptureMint(solana.WrappedSol, solana.Token2022ProgramID, 255, 166))
	tx.Message.AccountKeys[2] = solana.Token2022ProgramID
	group, err = prepareTransactionTokenGroup(ctx, []*solana.Transaction{tx})
	require.NoError(t, err)
	snapshot, err = captureTransactionTokenSnapshot(ctx, tx)
	require.NoError(t, err)
	pre, _, err = group.resolve(snapshot, snapshot)
	require.NoError(t, err)
	require.True(t, pre[0].HasExtensions, "raw capture does not pretend to convert extended mint UI amounts")
	require.Equal(t, uint8(255), pre[0].Decimals)
	require.Equal(t, uint64(math.MaxUint64), pre[0].Amount)
}

func TestTransactionTokenKnownInvalidMintKeepsValidSibling(t *testing.T) {
	payer, invalidKey, validKey := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	invalidMint, validMint := solana.PublicKey{4}, solana.PublicKey{5}
	for _, kind := range []string{"absent", "uninitialized", "owner-mismatch"} {
		t.Run(kind, func(t *testing.T) {
			ctx := tokenCaptureContext(t,
				tokenCaptureAccount(invalidKey, invalidMint, solana.TokenProgramID, 0, 165),
				tokenCaptureAccount(validKey, validMint, solana.TokenProgramID, 42, 165),
				tokenCaptureMint(validMint, solana.TokenProgramID, 6, 82))
			if kind != "absent" {
				mint := tokenCaptureMint(invalidMint, solana.TokenProgramID, 9, 82)
				if kind == "uninitialized" {
					mint.Data[45] = 0
				} else {
					mint.Owner = solana.Token2022ProgramID
				}
				require.NoError(t, ctx.SetAccount(invalidMint, mint))
			}
			tx := tokenCaptureTransaction(payer, invalidKey, validKey)
			group, err := prepareTransactionTokenGroup(ctx, []*solana.Transaction{tx})
			require.NoError(t, err)
			snapshot, err := captureTransactionTokenSnapshot(ctx, tx)
			require.NoError(t, err)
			pre, post, err := group.resolve(snapshot, snapshot)
			require.NoError(t, err)
			for _, balances := range [][]transactionTokenBalance{pre, post} {
				require.Len(t, balances, 1)
				require.Equal(t, validKey, tx.Message.AccountKeys[balances[0].AccountIndex])
				require.Equal(t, validMint, balances[0].Mint)
				require.Equal(t, uint64(42), balances[0].Amount)
				require.Equal(t, uint8(6), balances[0].Decimals)
			}
		})
	}
}

func TestTransactionTokenFiltersAndResolvedLookups(t *testing.T) {
	payer, key, mintKey, table := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}, solana.PublicKey{4}
	ctx := tokenCaptureContext(t, tokenCaptureAccount(key, mintKey, solana.TokenProgramID, 10, 165))
	tx := tokenCaptureTransaction(payer, key)
	tx.Message.AccountKeys[2] = solana.SystemProgramID
	snapshot, err := captureTransactionTokenSnapshot(ctx, tx)
	require.NoError(t, err)
	require.Empty(t, snapshot.Balances, "no token program in keys")
	tx = tokenCaptureTransaction(payer, key)
	tx.Message.Instructions[0].ProgramIDIndex = 1
	snapshot, err = captureTransactionTokenSnapshot(ctx, tx)
	require.NoError(t, err)
	require.Empty(t, snapshot.Balances, "invoked token-shaped account is not a token balance")
	tx = tokenCaptureTransaction(payer)
	tx.Message.SetVersion(solana.MessageVersionV0)
	tx.Message.AddressTableLookups = []solana.MessageAddressTableLookup{{AccountKey: table, WritableIndexes: []uint8{0}}}
	tx.Message.Instructions[0].Accounts = []uint16{2}
	_, err = captureTransactionTokenSnapshot(ctx, tx)
	require.ErrorContains(t, err, "requires resolved address lookups")
	require.NoError(t, tx.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{table: {key}}))
	require.NoError(t, tx.Message.ResolveLookups())
	snapshot, err = captureTransactionTokenSnapshot(ctx, tx)
	require.NoError(t, err)
	require.Len(t, snapshot.Balances, 1)
	require.Equal(t, uint16(2), snapshot.Balances[0].AccountIndex)
	_, ok := snapshot.Mints[key]
	require.True(t, ok, "ALT writable keys must carry invalidation state")
}
