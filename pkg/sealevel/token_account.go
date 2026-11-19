package sealevel

import (
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

// IsInitializedTokenAccount distinguishes token accounts from mints and
// multisigs, including Token-2022 accounts with extensions.
func IsInitializedTokenAccount(account *accounts.Account) bool {
	if account == nil || account.Lamports == 0 || (account.Owner != solana.TokenProgramID && account.Owner != solana.Token2022ProgramID) {
		return false
	}
	data := account.Data
	validSize := len(data) == 165 || (account.Owner == solana.Token2022ProgramID && len(data) > 165 && len(data) != 355 && data[165] == 2)
	return validSize && data[108] != 0
}
