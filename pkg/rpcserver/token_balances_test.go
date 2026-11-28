package rpcserver

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTokenAccount builds a 165-byte SPL Token account payload.
func makeTokenAccount(t *testing.T, mint, owner solana.PublicKey, amount uint64) *accounts.Account {
	t.Helper()
	data := make([]byte, tokenAccountSize)
	copy(data[tokenAccountMintOffset:], mint[:])
	copy(data[tokenAccountOwnerOffset:], owner[:])
	binary.LittleEndian.PutUint64(data[tokenAccountAmountOffset:], amount)
	var ownerArr [32]byte
	copy(ownerArr[:], splTokenProgramID[:])
	return &accounts.Account{Owner: ownerArr, Data: data}
}

func TestExtractTokenBalances_DecodesMintOwnerAmount(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
	owner := solana.MustPublicKeyFromBase58("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")
	tokenAcct := makeTokenAccount(t, mint, owner, 1234567)

	got := extractTokenBalances(
		[]*accounts.Account{tokenAcct},
		map[solana.PublicKey]uint8{mint: 6},
	)
	require.Len(t, got, 1)
	assert.Equal(t, mint.String(), got[0].Mint)
	assert.Equal(t, owner.String(), got[0].Owner)
	assert.Equal(t, splTokenProgramID.String(), got[0].ProgramId)
	assert.Equal(t, "1234567", got[0].UiTokenAmount.Amount)
	assert.Equal(t, uint8(6), got[0].UiTokenAmount.Decimals)
	require.NotNil(t, got[0].UiTokenAmount.UiAmount)
	assert.InDelta(t, 1.234567, *got[0].UiTokenAmount.UiAmount, 1e-9)
	assert.Equal(t, "1.234567", got[0].UiTokenAmount.UiAmountString)
}

func TestExtractTokenBalances_SkipsNonTokenAccounts(t *testing.T) {
	wallet := &accounts.Account{Owner: [32]byte{1, 2, 3}, Data: make([]byte, 165)}
	got := extractTokenBalances([]*accounts.Account{wallet}, map[solana.PublicKey]uint8{})
	assert.Empty(t, got)
}

func TestExtractTokenBalances_EmptyInputReturnsEmpty(t *testing.T) {
	got := extractTokenBalances(nil, nil)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestExtractTokenBalances_TruncatedDataIsSkipped(t *testing.T) {
	var ownerArr [32]byte
	copy(ownerArr[:], splTokenProgramID[:])
	short := &accounts.Account{Owner: ownerArr, Data: make([]byte, 100)}
	got := extractTokenBalances([]*accounts.Account{short}, nil)
	assert.Empty(t, got)
}

func TestExtractTokenBalances_MissingDecimalsDefaultsToZero(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
	owner := solana.MustPublicKeyFromBase58("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")
	tokenAcct := makeTokenAccount(t, mint, owner, 42)

	got := extractTokenBalances([]*accounts.Account{tokenAcct}, nil)
	require.Len(t, got, 1)
	assert.Equal(t, "42", got[0].UiTokenAmount.Amount)
	assert.Equal(t, uint8(0), got[0].UiTokenAmount.Decimals)
	assert.Equal(t, "42", got[0].UiTokenAmount.UiAmountString)
}

func TestExtractTokenBalances_Token2022ProgramIdDistinguished(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
	owner := solana.MustPublicKeyFromBase58("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")
	tokenAcct := makeTokenAccount(t, mint, owner, 1)
	var ownerArr [32]byte
	copy(ownerArr[:], splToken2022ProgramID[:])
	tokenAcct.Owner = ownerArr

	got := extractTokenBalances([]*accounts.Account{tokenAcct}, map[solana.PublicKey]uint8{mint: 9})
	require.Len(t, got, 1)
	assert.Equal(t, splToken2022ProgramID.String(), got[0].ProgramId)
}

func TestUiAmountStringForRaw_TrimsTrailingZeros(t *testing.T) {
	assert.Equal(t, "1.5", uiAmountStringForRaw(15_000_000, 7))
	assert.Equal(t, "1.234567", uiAmountStringForRaw(1_234_567, 6))
	assert.Equal(t, "1", uiAmountStringForRaw(1_000_000, 6))
	assert.Equal(t, "0.0001", uiAmountStringForRaw(100, 6))
	assert.Equal(t, "0", uiAmountStringForRaw(0, 6))
}

func TestUiAmountStringForRaw_ZeroDecimals(t *testing.T) {
	assert.Equal(t, "12345", uiAmountStringForRaw(12345, 0))
}

// High-decimals (>= 20) cases — Token-2022 doesn't bound decimals at the
// protocol level so the renderer must stay precise even past float64
// representable range. Float-based division saturates at uint64(1e20).
func TestUiAmountStringForRaw_HighDecimalsPrecise(t *testing.T) {
	const maxU64 = uint64(18446744073709551615)
	cases := []struct {
		amount   uint64
		decimals uint8
		want     string
	}{
		{maxU64, 20, "0.18446744073709551615"},
		{maxU64, 25, "0.0000018446744073709551615"},
		{1, 20, "0.00000000000000000001"},
		{0, 20, "0"},
		{0, 0, "0"},
		{1, 0, "1"},
	}
	for _, c := range cases {
		got := uiAmountStringForRaw(c.amount, c.decimals)
		assert.Equal(t, c.want, got, "amount=%d decimals=%d", c.amount, c.decimals)
	}
}

func TestUiAmountStringForRaw_LargeAmountModerateDecimals(t *testing.T) {
	// 9007199254740993 is 2^53 + 1, the first integer not exactly
	// representable in float64. The string path must still be exact.
	assert.Equal(t, "9007199254.740993", uiAmountStringForRaw(9007199254740993, 6))
}
