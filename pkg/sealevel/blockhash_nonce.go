package sealevel

import (
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

func IsNonceInstr(instr Instruction) bool {
	if instr.ProgramId != a.SystemProgramAddr {
		return false
	}

	if len(instr.Data) < 4 {
		return false
	}

	if len(instr.Accounts) < 1 {
		return false
	}

	decoder := bin.NewBinDecoder(instr.Data)
	instructionType, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return false
	}

	if instructionType != SystemProgramInstrTypeAdvanceNonceAccount {
		return false
	}

	return true
}

func MaybeAdvanceNonceAccountForFailedTx(slotCtx *SlotCtx, tx *solana.Transaction, instr Instruction) (solana.PublicKey, bool) {
	if !IsNonceInstr(instr) {
		return solana.PublicKey{}, false
	}

	// we don't need to advance the durable nonce under error conditions if the blockhash is of valid age.
	// we check against the latest 151 blockhashes (hence slotCtx.LatestEvictedBlockhash) instead of 150
	// because of a (known) quirk in Agave's blockhash queue implementation.
	// see https://github.com/anza-xyz/agave/blame/992a398fe8ea29ec4f04d081ceef7664960206f4/accounts-db/src/blockhash_queue.rs#L222
	recentBlockhashes, ok := recentBlockhashesForSlot(slotCtx)
	if !ok {
		return solana.PublicKey{}, false
	}
	if recentBlockhashes.IsBlockhashAgeValid(tx.Message.RecentBlockhash) {
		return solana.PublicKey{}, false
	} else if tx.Message.RecentBlockhash == slotCtx.LatestEvictedBlockhash {
		return solana.PublicKey{}, false
	}

	noncePk := instr.Accounts[0].Pubkey
	nonceAcct, err := slotCtx.GetAccount(noncePk)
	if err != nil {
		return solana.PublicKey{}, false
	}

	nonceStateVersions, err := UnmarshalNonceStateVersions(nonceAcct.Data)
	if err != nil {
		return solana.PublicKey{}, false
	}

	if nonceStateVersions.Type == NonceVersionLegacy {
		return solana.PublicKey{}, false
	}

	state := nonceStateVersions.State()
	if !state.IsInitialized {
		return solana.PublicKey{}, false
	}

	var nonceInstrSigners []solana.PublicKey
	for _, acct := range instr.Accounts {
		if acct.IsSigner {
			nonceInstrSigners = append(nonceInstrSigners, acct.Pubkey)
		}
	}

	if !state.IsSignerAuthority(nonceInstrSigners) {
		return solana.PublicKey{}, false
	}

	rbh := slotCtx.LastBlockhash
	nextDurableNonce := durableNonce(rbh)
	if state.DurableNonce == nextDurableNonce {
		return solana.PublicKey{}, false
	}

	if nonceStateVersions.Type == NonceVersionCurrent {
		state.DurableNonce = nextDurableNonce
		state.FeeCalculator.LamportsPerSignature = slotCtx.FeeRateGovernor.PrevLamportsPerSignature
	} else {
		nonceStateVersions.Upgrade()
		upgradedState := nonceStateVersions.State()
		upgradedState.DurableNonce = nextDurableNonce
		upgradedState.FeeCalculator.LamportsPerSignature = slotCtx.FeeRateGovernor.PrevLamportsPerSignature
	}

	newData, err := nonceStateVersions.Marshal()
	if err != nil {
		return solana.PublicKey{}, false
	}

	copy(nonceAcct.Data, newData)
	slotCtx.SetAccount(noncePk, nonceAcct)

	return noncePk, true
}

func IsTransactionAgeValid(tx *solana.Transaction, instrs []Instruction, slotCtx *SlotCtx) bool {
	// check the most recent 151 blockhashes against the tx's blockhash. if we have a match, then we return success
	// we check against the latest 151 blockhashes (hence slotCtx.LatestEvictedBlockhash) instead of 150
	// because of a (known) quirk in Agave's blockhash queue implementation.
	// see https://github.com/anza-xyz/agave/blame/992a398fe8ea29ec4f04d081ceef7664960206f4/accounts-db/src/blockhash_queue.rs#L222

	recentBlockhashes, ok := recentBlockhashesForSlot(slotCtx)
	if !ok {
		return false
	}
	if recentBlockhashes.IsBlockhashAgeValid(tx.Message.RecentBlockhash) || tx.Message.RecentBlockhash == slotCtx.LatestEvictedBlockhash {
		return true
	}

	// if the tx's blockhash doesn't match the latest 151 blockhashes, it may still be valid if it's a durable
	// nonce tx. the following logic verifies whether that's the case here.

	nextDurableNonce := durableNonce(slotCtx.LastBlockhash)
	if tx.Message.RecentBlockhash == nextDurableNonce {
		return false
	}

	if len(instrs) == 0 {
		return false
	}

	instr := instrs[0]
	if instr.ProgramId != a.SystemProgramAddr {
		return false
	}

	decoder := bin.NewBinDecoder(instr.Data)
	instructionType, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return false
	}
	if instructionType != SystemProgramInstrTypeAdvanceNonceAccount {
		return false
	}

	if len(instr.Accounts) == 0 {
		return false
	}

	if !instr.Accounts[0].IsWritable {
		return false
	}

	noncePk := instr.Accounts[0].Pubkey
	nonceAcct, err := slotCtx.GetAccount(noncePk)
	if err != nil && slotCtx.AccountsDb != nil {
		// Per-slot MemAccounts only holds accounts referenced by the
		// current block's txs. On the simulate path the nonce account
		// is usually absent there — fall back to accountsdb so durable
		// nonce txs validate against on-chain state.
		nonceAcct, err = slotCtx.GetAccountFromAccountsDb(noncePk)
	}
	if err != nil {
		return false
	}

	if nonceAcct.Owner != a.SystemProgramAddr {
		return false
	}

	nonceStateVersions, err := UnmarshalNonceStateVersions(nonceAcct.Data)
	if err != nil {
		return false
	}

	if nonceStateVersions.Type == NonceVersionLegacy {
		return false
	}

	state := nonceStateVersions.State()
	if !state.IsInitialized {
		return false
	}

	if state.DurableNonce != tx.Message.RecentBlockhash {
		return false
	}

	var nonceInstrSigners []solana.PublicKey
	for _, acct := range instr.Accounts {
		if acct.IsSigner {
			nonceInstrSigners = append(nonceInstrSigners, acct.Pubkey)
		}
	}

	if !state.IsSignerAuthority(nonceInstrSigners) {
		return false
	}

	return true
}

// IsRecentBlockhashTransaction distinguishes ordinary blockhash transactions
// from durable-nonce transactions after age validation. SIMD-0290 permits an
// invalid fee payer to become a no-op only for the former.
func IsRecentBlockhashTransaction(tx *solana.Transaction, slotCtx *SlotCtx) bool {
	if tx == nil || slotCtx == nil {
		return false
	}
	recentBlockhashes, ok := recentBlockhashesForSlot(slotCtx)
	if !ok {
		return false
	}
	return recentBlockhashes.IsBlockhashAgeValid(tx.Message.RecentBlockhash) ||
		tx.Message.RecentBlockhash == slotCtx.LatestEvictedBlockhash
}

func recentBlockhashesForSlot(slotCtx *SlotCtx) (SysvarRecentBlockhashes, bool) {
	if slotCtx != nil {
		if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
			recentBlockhashes, ok := bankSysvars.RecentBlockhashes()
			if !ok {
				return nil, false
			}
			return recentBlockhashes, true
		}
	}
	if SysvarCache.RecentBlockHashes.Sysvar == nil {
		return nil, false
	}
	return *SysvarCache.RecentBlockHashes.Sysvar, true
}
