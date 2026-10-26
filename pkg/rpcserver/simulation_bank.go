package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

const (
	maxSimulationAccountBytes = 80 << 20
	maxSimulationAccountKeys  = 2048
)

var errSimulationViewChanged = errors.New("simulation account view changed")

type simulationAccountSource interface {
	GetAccountWithDataLimit(uint64, solana.PublicKey, uint64) (*accounts.Account, error)
}

// simulationAccounts owns the finite read set before execution starts. Once
// sealed, even an unexpected runtime lookup cannot fall through to live state.
type simulationAccounts struct {
	bank      *replay.ChainTipSnapshot
	source    simulationAccountSource
	accounts  map[solana.PublicKey]*accounts.Account
	remaining uint64
	sealed    bool
	err       error
}

func (view *simulationAccounts) capture(ctx context.Context, key solana.PublicKey) {
	if view.err != nil {
		return
	}
	if view.sealed {
		view.err = fmt.Errorf("simulation account view is sealed")
		return
	}
	if err := ctx.Err(); err != nil {
		view.err = err
		return
	}
	if _, exists := view.accounts[key]; exists {
		return
	}
	if len(view.accounts) >= maxSimulationAccountKeys {
		view.err = fmt.Errorf("simulation exceeds %d account keys", maxSimulationAccountKeys)
		return
	}
	var acct *accounts.Account
	var err error
	if sealevel.IsBankSysvarAccount(key) {
		acct, _ = view.bank.BankSysvars.CloneAccount(key)
	} else {
		acct, err = view.source.GetAccountWithDataLimit(view.bank.Slot, key, view.remaining)
	}
	if errors.Is(err, accountsdb.ErrNoAccount) || errors.Is(err, accounts.ErrAccountNotFound) {
		view.accounts[key] = nil
		return
	}
	if err != nil {
		view.err = fmt.Errorf("capture simulation account %s: %w", key, err)
		return
	}
	if acct == nil && !sealevel.IsBankSysvarAccount(key) {
		view.err = fmt.Errorf("account reader returned no value or absence for %s", key)
		return
	}
	if acct != nil {
		if uint64(len(acct.Data)) > view.remaining {
			view.err = accountsdb.ErrAccountDataLimit
			return
		}
		view.remaining -= uint64(len(acct.Data))
	}
	view.accounts[key] = acct
}

func (view *simulationAccounts) GetAccount(slot uint64, key solana.PublicKey) (*accounts.Account, error) {
	if view.err != nil {
		return nil, view.err
	}
	acct, exists := view.accounts[key]
	if slot != view.bank.Slot || !exists {
		view.err = fmt.Errorf("account %s was not captured for simulation slot %d", key, view.bank.Slot)
		return nil, view.err
	}
	if acct == nil {
		return nil, accountsdb.ErrNoAccount
	}
	return acct.Clone(), nil
}

func (view *simulationAccounts) GetAccountWithDataLimit(slot uint64, key solana.PublicKey, limit uint64) (*accounts.Account, error) {
	if acct := view.accounts[key]; acct != nil && uint64(len(acct.Data)) > limit {
		return nil, accountsdb.ErrAccountDataLimit
	}
	return view.GetAccount(slot, key)
}

func (server *RpcServer) acquireSimulation(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	server.simulationOnce.Do(func() { server.simulationCalls = make(chan struct{}, 2) })
	select {
	case server.simulationCalls <- struct{}{}:
		return func() { <-server.simulationCalls }, nil
	default:
		return nil, fmt.Errorf("transaction simulation capacity is exhausted")
	}
}

func validateTransactionCommitment(commitment string) error {
	switch commitment {
	case "processed", "confirmed", "finalized":
		return nil
	default:
		return &InvalidParamsError{Message: "invalid commitment"}
	}
}

// selectSimulationBank returns a short-lived validation fence, not a lock held
// through account I/O or VM execution. Confirmed and finalized share our durable
// root; neither may silently use the newer processed account overlay.
func (server *RpcServer) selectSimulationBank(commitment string, minimum *uint64) (*replay.ChainTipSnapshot, simulationAccountSource, func() bool, error) {
	if server.acctsDb == nil {
		return nil, nil, nil, fmt.Errorf("node has no simulation accounts database")
	}
	server.slotCtxMu.RLock()
	generation, invalidated := server.executionGeneration, server.executionInvalidated
	completed, ready := server.slotCtx, server.healthReady
	server.slotCtxMu.RUnlock()
	if invalidated {
		return nil, nil, nil, fmt.Errorf("node is recovering its execution state")
	}
	readyGeneration := func() bool {
		server.historyRecoveryMu.RLock()
		pending := server.historyRecoveryPending
		server.historyRecoveryMu.RUnlock()
		server.slotCtxMu.RLock()
		defer server.slotCtxMu.RUnlock()
		return !pending && !server.executionInvalidated && generation == server.executionGeneration
	}
	root := server.rootedBank.Load()
	var bank *replay.ChainTipSnapshot
	var source simulationAccountSource = server.acctsDb
	var version uint64
	var valid func() bool
	if commitment == "processed" {
		tip := replay.ChainTipParentContext()
		if !ready || completed == nil || tip.Generation == 0 || tip.BankSysvars == nil || tip.Slot != completed.Slot || tip.BankSysvars != completed.BankSysvars() || tip.LastBlockhash != solana.Hash(completed.Blockhash) {
			return nil, nil, nil, fmt.Errorf("node has no processed execution bank")
		}
		bank = &tip
		// Promotion can prune the overlay after publishing this tip. The exact
		// same completed bank remains usable through its now-durable root.
		if root != nil && root.executionBank != nil && root.Slot == tip.Slot && root.executionBank.LastBlockhash == tip.LastBlockhash && root.executionBank.Bankhash == tip.Bankhash {
			bank = root.executionBank
		} else {
			if !tip.AccountVersionsReady {
				return nil, nil, nil, errSimulationViewChanged
			}
			version = tip.AccountVersion
			if tip.UnrootedRead != nil {
				var ok bool
				source, ok = tip.UnrootedRead.(simulationAccountSource)
				if !ok {
					return nil, nil, nil, fmt.Errorf("processed account reader does not support bounded reads")
				}
			}
			valid = func() bool {
				latest, stable := server.acctsDb.CommittedAccountVersion()
				current := replay.ChainTipParentContext()
				if !readyGeneration() || !stable || latest != version || current.Generation != tip.Generation {
					return false
				}
				if reader, ok := tip.UnrootedRead.(interface{ AccountReadVersion() uint64 }); ok {
					return reader.AccountReadVersion() == tip.UnrootedVersion
				}
				return tip.UnrootedRead == nil
			}
		}
	} else if root != nil {
		bank = root.executionBank
	}
	if bank == nil {
		return nil, nil, nil, fmt.Errorf("node has no rooted execution bank")
	}
	if minimum != nil && bank.Slot < *minimum {
		return nil, nil, nil, &MinContextSlotNotReachedError{ContextSlot: bank.Slot}
	}
	if valid == nil {
		version = root.executionVersion
		if !root.executionReady {
			var stable bool
			version, stable = server.legacySimulationVersion(bank)
			if !stable {
				return nil, nil, nil, errSimulationViewChanged
			}
		}
		if server.rootedPublicationPending(bank.Slot) {
			return nil, nil, nil, errSimulationViewChanged
		}
		valid = func() bool {
			latest, stable := server.acctsDb.CommittedAccountVersion()
			if !root.executionReady {
				latest, stable = server.legacySimulationVersion(bank)
			}
			return readyGeneration() && stable && latest == version && server.rootedBank.Load() == root && !server.rootedPublicationPending(bank.Slot)
		}
	}
	if !valid() {
		return nil, nil, nil, errSimulationViewChanged
	}
	return bank, source, valid, nil
}

// Legacy replay queues disk writes before publishing its completed bank. Once
// they drain, require that exact slot and canonical bankhash; a stable current
// version alone could belong to a newer bank or a same-slot replacement.
func (server *RpcServer) legacySimulationVersion(bank *replay.ChainTipSnapshot) (uint64, bool) {
	if server.acctsDb.RootedDurable || bank.Bankhash == (solana.Hash{}) {
		return 0, false
	}
	return server.acctsDb.CommittedLegacyBankVersion(bank.Slot, [32]byte(bank.Bankhash))
}

func (server *RpcServer) prepareSimulation(ctx context.Context, tx *solana.Transaction, commitment string, minimum *uint64, responseKeys []solana.PublicKey, captureExecution bool) (*sealevel.SlotCtx, *solana.Transaction, *simulationAccounts, *replay.ChainTipSnapshot, *replay.TransactionError, error) {
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		bank, source, valid, err := server.selectSimulationBank(commitment, minimum)
		if err == errSimulationViewChanged {
			continue
		}
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		view := &simulationAccounts{bank: bank, source: source, accounts: make(map[solana.PublicKey]*accounts.Account), remaining: maxSimulationAccountBytes}
		slotCtx, err := newSimulationBankContext(bank, server.acctsDb, view)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		slotCtx.TraceCtx = ctx
		for _, lookup := range tx.Message.AddressTableLookups {
			view.capture(ctx, lookup.AccountKey)
		}
		resolved, txErr := replay.ResolveAddressTableLookups(slotCtx, tx)
		if txErr == nil && captureExecution {
			for _, key := range append(append([]solana.PublicKey(nil), resolved.Message.AccountKeys...), responseKeys...) {
				view.capture(ctx, key)
			}
			for _, key := range resolved.Message.AccountKeys {
				acct := view.accounts[key]
				if acct == nil {
					continue
				}
				if acct.Owner != addresses.NativeLoaderAddr {
					view.capture(ctx, solana.PublicKey(acct.Owner))
				}
				if acct.Owner == addresses.BpfLoaderUpgradeableAddr {
					state, decodeErr := sealevel.UnmarshalUpgradeableLoaderState(acct.Data)
					if decodeErr == nil && state.Type == sealevel.UpgradeableLoaderStateTypeProgram {
						view.capture(ctx, state.Program.ProgramDataAddress)
					}
				}
				if sealevel.IsInitializedTokenAccount(acct) {
					view.capture(ctx, publicKeyFromOffset(acct.Data, tokenAccountMintOffset))
				}
			}
		}
		if !valid() {
			continue
		}
		if view.err != nil {
			return nil, nil, nil, nil, nil, view.err
		}
		if txErr == nil && captureExecution {
			// Legacy account loading reads the slot's preloaded account set.
			// Match replay's absent-account representation without consulting live
			// state again; the reader still retains absence for response lookups.
			for key, acct := range view.accounts {
				if acct == nil {
					acct = &accounts.Account{Key: key, RentEpoch: math.MaxUint64}
				}
				if err := slotCtx.SetAccount(key, acct); err != nil {
					return nil, nil, nil, nil, nil, err
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		view.sealed = true
		view.source = nil
		return slotCtx, resolved, view, bank, txErr, nil
	}
	return nil, nil, nil, nil, nil, errSimulationViewChanged
}

// newSimulationBankContext uses completed-bank parameters with request-local
// writable state. The caller must fence reader for the selected account view.
func newSimulationBankContext(bank *replay.ChainTipSnapshot, db *accountsdb.AccountsDb, reader sealevel.AccountReader) (*sealevel.SlotCtx, error) {
	if bank == nil || bank.Features == nil || bank.PrevFeeGovernor == nil || bank.BankSysvars == nil || db == nil || reader == nil {
		return nil, fmt.Errorf("simulation bank is unavailable")
	}
	if bank.BankSysvars.Slot() != bank.Slot {
		return nil, fmt.Errorf("simulation bank sysvars do not match slot %d", bank.Slot)
	}
	if err := bank.BankSysvars.ValidateForExecution(); err != nil {
		return nil, err
	}
	clock, ok := bank.BankSysvars.Clock()
	if !ok || clock.Slot != bank.Slot {
		return nil, fmt.Errorf("simulation bank clock does not match slot %d", bank.Slot)
	}
	recent, ok := bank.BankSysvars.RecentBlockhashes()
	if !ok || len(recent) == 0 || recent[0].Blockhash != [32]byte(bank.LastBlockhash) || bank.LastBlockhash == (solana.Hash{}) || recent[0].FeeCalculator.LamportsPerSignature != bank.PrevFeeGovernor.LamportsPerSignature {
		return nil, fmt.Errorf("simulation bank blockhash and fee are unavailable at slot %d", bank.Slot)
	}
	fees := *bank.PrevFeeGovernor
	// Replay's previous fee belongs to its parent. Simulating a completed bank
	// advances nonces with that bank's queue-head hash and matching fee instead.
	fees.PrevLamportsPerSignature = recent[0].FeeCalculator.LamportsPerSignature
	ctx := &sealevel.SlotCtx{
		Slot: bank.Slot, Epoch: clock.Epoch,
		Blockhash: [32]byte(bank.LastBlockhash), LastBlockhash: [32]byte(bank.LastBlockhash),
		LatestEvictedBlockhash: bank.LatestEvictedBlockhash,
		AccountsDb:             db, UnrootedRead: reader,
		Accounts: accounts.NewMemAccounts(), ParentAccts: accounts.NewMemAccounts(),
		Features: bank.Features.Clone(), FeeRateGovernor: &fees,
		VoteAccts: bank.EpochStakes, TotalEpochStake: bank.TotalEpochStake,
		AcctMapsMu: &sync.Mutex{}, VoteTimestampMu: &sync.Mutex{},
		ModifiedAccts: make(map[solana.PublicKey]bool), WritableAccts: make(map[solana.PublicKey]bool),
		VoteTimestamps: make(map[solana.PublicKey]sealevel.BlockTimestamp),
	}
	if err := ctx.PublishBankSysvars(bank.BankSysvars); err != nil {
		return nil, err
	}
	return ctx, nil
}
