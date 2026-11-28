package rpcserver

import (
	"encoding/binary"
	"slices"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// SPL Token program IDs.
var (
	splTokenProgramID     = solana.MustPublicKeyFromBase58("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
	splToken2022ProgramID = solana.MustPublicKeyFromBase58("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
)

// SPL Token Account is exactly 165 bytes; offsets are stable across both
// the legacy program and Token-2022 (Token-2022 stores extension data
// past byte 165, but the leading account state has the same layout).
const (
	tokenAccountSize         = 165
	tokenAccountMintOffset   = 0
	tokenAccountOwnerOffset  = 32
	tokenAccountAmountOffset = 64
	// Mint account layout: 82 bytes for legacy SPL Token. decimals lives at
	// offset 44 (mintAuthorityOption 4 + mintAuthority 32 + supply 8 = 44).
	mintAccountSize    = 82
	mintDecimalsOffset = 44
)

type tokenMintDecimals struct {
	program  solana.PublicKey
	decimals uint8
}

type tokenBalanceIndices []int

func simulationTokenBalanceIndices(msg *solana.Message) tokenBalanceIndices {
	if !slices.Contains(msg.AccountKeys, solana.TokenProgramID) && !slices.Contains(msg.AccountKeys, solana.Token2022ProgramID) {
		return nil
	}
	var invoked [256]bool
	for _, ix := range msg.Instructions {
		if int(ix.ProgramIDIndex) < len(invoked) {
			invoked[ix.ProgramIDIndex] = true
		}
	}
	var indices tokenBalanceIndices
	for i, key := range msg.AccountKeys {
		if i < len(invoked) && !invoked[i] && key != solana.TokenProgramID && key != solana.Token2022ProgramID {
			indices = append(indices, i)
		}
	}
	return indices
}

// Only balances backed by an initialized mint of the same token program
// are reported. AccountIndex is the original transaction index.
func (indices tokenBalanceIndices) extractTokenBalances(
	txAccts []*accounts.Account,
	mintDecimals map[solana.PublicKey]tokenMintDecimals,
) []TokenBalancePayload {
	if len(txAccts) == 0 {
		return []TokenBalancePayload{}
	}
	out := make([]TokenBalancePayload, 0)
	for _, idx := range indices {
		if idx >= len(txAccts) {
			continue
		}
		acct := txAccts[idx]
		if !sealevel.IsInitializedTokenAccount(acct) {
			continue
		}
		mint := publicKeyFromOffset(acct.Data, tokenAccountMintOffset)
		info, ok := mintDecimals[mint]
		if !ok || info.program != acct.Owner {
			continue
		}
		owner := publicKeyFromOffset(acct.Data, tokenAccountOwnerOffset)
		amount := binary.LittleEndian.Uint64(acct.Data[tokenAccountAmountOffset : tokenAccountAmountOffset+8])

		decimals := info.decimals

		out = append(out, TokenBalancePayload{
			AccountIndex: uint8(idx),
			Mint:         mint.String(),
			Owner:        owner.String(),
			ProgramId:    info.program.String(),
			UiTokenAmount: UiTokenAmountPayload{
				Amount:         strconv.FormatUint(amount, 10),
				Decimals:       decimals,
				UiAmount:       uiAmountForRaw(amount, decimals),
				UiAmountString: uiAmountStringForRaw(amount, decimals),
			},
		})
	}
	return out
}

// fetchMintDecimals collects the unique mints referenced by token
// accounts in txAccts and looks each one up in the captured bank to extract its
// decimals byte. A transaction-local mint takes precedence over the bank.
func (indices tokenBalanceIndices) fetchMintDecimals(
	txAccts []*accounts.Account,
	reader sealevel.AccountReader,
	slot uint64,
) map[solana.PublicKey]tokenMintDecimals {
	out := make(map[solana.PublicKey]tokenMintDecimals)
	byKey := make(map[solana.PublicKey]*accounts.Account, len(txAccts))
	for _, acct := range txAccts {
		if acct != nil {
			byKey[acct.Key] = acct
		}
	}
	seen := make(map[solana.PublicKey]struct{})
	for _, idx := range indices {
		if idx >= len(txAccts) {
			continue
		}
		acct := txAccts[idx]
		if !sealevel.IsInitializedTokenAccount(acct) {
			continue
		}
		mint := publicKeyFromOffset(acct.Data, tokenAccountMintOffset)
		if _, ok := seen[mint]; ok {
			continue
		}
		seen[mint] = struct{}{}

		mintAcct := byKey[mint]
		if mintAcct == nil && reader != nil {
			var err error
			mintAcct, err = reader.GetAccount(slot, mint)
			if err != nil {
				continue
			}
		}
		if !sealevel.IsInitializedTokenMint(mintAcct) {
			continue
		}
		out[mint] = tokenMintDecimals{mintAcct.Owner, mintAcct.Data[mintDecimalsOffset]}
	}
	return out
}

func publicKeyFromOffset(data []byte, off int) solana.PublicKey {
	var pk solana.PublicKey
	copy(pk[:], data[off:off+32])
	return pk
}

// uiAmountForRaw can lose precision above 2^53. It returns nil if 10^decimals exceeds uint64.
func uiAmountForRaw(amount uint64, decimals uint8) *float64 {
	if decimals >= 20 {
		return nil
	}
	if decimals == 0 {
		v := float64(amount)
		return &v
	}
	v := float64(amount) / pow10Float(decimals)
	return &v
}

// uiAmountStringForRaw renders amount/10^decimals as a decimal string
// with trailing zeros trimmed, matching Agave's UiAmountString. Pure
// string manipulation so precision is preserved for any decimals up to
// uint8 max (255) — Token-2022 doesn't bound decimals at the protocol
// level, and float64-based division saturates uint64 cast at decimals≥20.
func uiAmountStringForRaw(amount uint64, decimals uint8) string {
	if amount == 0 {
		return "0"
	}
	digits := strconv.FormatUint(amount, 10)
	if decimals == 0 {
		return digits
	}
	d := int(decimals)
	if d >= len(digits) {
		// Whole part is zero; pad fractional with leading zeros.
		frac := strings.Repeat("0", d-len(digits)) + digits
		frac = strings.TrimRight(frac, "0")
		if frac == "" {
			return "0"
		}
		return "0." + frac
	}
	whole := digits[:len(digits)-d]
	frac := strings.TrimRight(digits[len(digits)-d:], "0")
	if frac == "" {
		return whole
	}
	return whole + "." + frac
}

// pow10Float returns 10^n as a float64. Used only by uiAmountForRaw
// (the *float64 path); the string path uses index math instead.
func pow10Float(n uint8) float64 {
	out := 1.0
	for i := uint8(0); i < n; i++ {
		out *= 10
	}
	return out
}

// An unexecuted transaction has null balances. An executed transaction
// without any reportable token accounts has an empty array.
func (indices tokenBalanceIndices) tokenBalancesFromAccounts(txAccts []*accounts.Account, reader sealevel.AccountReader, slot uint64) []TokenBalancePayload {
	if txAccts == nil {
		return nil
	}
	if len(indices) == 0 || len(txAccts) == 0 {
		return []TokenBalancePayload{}
	}
	mintDecimals := indices.fetchMintDecimals(txAccts, reader, slot)
	return indices.extractTokenBalances(txAccts, mintDecimals)
}
