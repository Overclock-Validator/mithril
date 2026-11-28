package rpcserver

import (
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
)

// SPL Token program IDs.
var (
	splTokenProgramID    = solana.MustPublicKeyFromBase58("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
	splToken2022ProgramID = solana.MustPublicKeyFromBase58("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
)

// SPL Token Account is exactly 165 bytes; offsets are stable across both
// the legacy program and Token-2022 (Token-2022 stores extension data
// past byte 165, but the leading account state has the same layout).
const (
	tokenAccountSize       = 165
	tokenAccountMintOffset = 0
	tokenAccountOwnerOffset = 32
	tokenAccountAmountOffset = 64
	// Mint account layout: 82 bytes for legacy SPL Token. decimals lives at
	// offset 44 (mintAuthorityOption 4 + mintAuthority 32 + supply 8 = 44).
	mintAccountSize     = 82
	mintDecimalsOffset  = 44
)

// isTokenProgramOwner reports whether an account is owned by either the
// legacy SPL Token program or Token-2022. Both programs use the same
// account layout for the leading 165 bytes.
func isTokenProgramOwner(owner [32]byte) bool {
	return owner == [32]byte(splTokenProgramID) || owner == [32]byte(splToken2022ProgramID)
}

// extractTokenBalances walks the supplied transaction accounts and emits
// a TokenBalancePayload for each one that is recognized as an SPL Token
// account. mintDecimals is consulted for the per-mint decimal count;
// callers populate it from accountsdb. Missing decimals fall back to 0
// rather than skipping the account so pre/post arrays stay aligned with
// the transaction's account index.
func extractTokenBalances(
	txAccts []*accounts.Account,
	mintDecimals map[solana.PublicKey]uint8,
) []TokenBalancePayload {
	if len(txAccts) == 0 {
		return []TokenBalancePayload{}
	}
	out := make([]TokenBalancePayload, 0)
	for idx, acct := range txAccts {
		if acct == nil || !isTokenProgramOwner(acct.Owner) {
			continue
		}
		if len(acct.Data) < tokenAccountSize {
			continue
		}
		mint := publicKeyFromOffset(acct.Data, tokenAccountMintOffset)
		owner := publicKeyFromOffset(acct.Data, tokenAccountOwnerOffset)
		amount := binary.LittleEndian.Uint64(acct.Data[tokenAccountAmountOffset : tokenAccountAmountOffset+8])

		decimals := mintDecimals[mint]
		programID := splTokenProgramID
		if acct.Owner == [32]byte(splToken2022ProgramID) {
			programID = splToken2022ProgramID
		}

		out = append(out, TokenBalancePayload{
			AccountIndex: uint8(idx),
			Mint:         mint.String(),
			Owner:        owner.String(),
			ProgramId:    programID.String(),
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
// accounts in txAccts and looks each one up in accountsdb to extract its
// decimals byte. Mints that fail the size check or aren't found are
// omitted; callers default missing entries to 0.
func fetchMintDecimals(
	txAccts []*accounts.Account,
	db *accountsdb.AccountsDb,
	slot uint64,
) map[solana.PublicKey]uint8 {
	out := make(map[solana.PublicKey]uint8)
	if db == nil {
		return out
	}
	seen := make(map[solana.PublicKey]struct{})
	for _, acct := range txAccts {
		if acct == nil || !isTokenProgramOwner(acct.Owner) {
			continue
		}
		if len(acct.Data) < tokenAccountSize {
			continue
		}
		mint := publicKeyFromOffset(acct.Data, tokenAccountMintOffset)
		if _, ok := seen[mint]; ok {
			continue
		}
		seen[mint] = struct{}{}

		mintAcct, err := db.GetAccount(slot, mint)
		if err != nil || mintAcct == nil || len(mintAcct.Data) < mintAccountSize {
			continue
		}
		out[mint] = mintAcct.Data[mintDecimalsOffset]
	}
	return out
}

func publicKeyFromOffset(data []byte, off int) solana.PublicKey {
	var pk solana.PublicKey
	copy(pk[:], data[off:off+32])
	return pk
}

// uiAmountForRaw returns the raw token amount divided by 10^decimals as
// a float64. Matches Agave's lossy float conversion; precision suffers
// for amounts > 2^53 but this is the wire contract.
func uiAmountForRaw(amount uint64, decimals uint8) *float64 {
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

// tokenBalancesFromAccounts wraps mint-decimal lookup + decoding so the
// simulate handler can build pre/post arrays from a slice of accounts.
// Returns nil when txAccts is nil (no accounts ever loaded — e.g. sanitize
// failure path) so the response emits JSON null, matching Agave's
// behavior on early-fail paths. Returns an empty slice for explicitly
// empty input or non-token transactions.
func tokenBalancesFromAccounts(txAccts []*accounts.Account, db *accountsdb.AccountsDb, slot uint64) []TokenBalancePayload {
	if txAccts == nil {
		return nil
	}
	if len(txAccts) == 0 {
		return []TokenBalancePayload{}
	}
	mintDecimals := fetchMintDecimals(txAccts, db, slot)
	return extractTokenBalances(txAccts, mintDecimals)
}

