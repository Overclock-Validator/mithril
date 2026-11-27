package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

const (
	maxSimulationAccountBytes = 80 << 20
	maxSimulationAccountKeys  = 2048
)

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

// simulationAdmissionWait is how long a request queues for an execution slot
// before it is refused. A burst of a few calls then waits its turn; sustained
// overload is still refused quickly.
const simulationAdmissionWait = 250 * time.Millisecond

// simulationCapacity bounds concurrent simulated executions, which compete
// with replay for cores: a quarter of them, and at least two.
func simulationCapacity() int {
	return max(2, runtime.GOMAXPROCS(0)/4)
}

func (server *RpcServer) acquireSimulation(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	server.simulationOnce.Do(func() { server.simulationCalls = make(chan struct{}, simulationCapacity()) })
	release := func() { <-server.simulationCalls }
	select {
	case server.simulationCalls <- struct{}{}:
		return release, nil
	default:
	}
	wait := time.NewTimer(simulationAdmissionWait)
	defer wait.Stop()
	select {
	case server.simulationCalls <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-wait.C:
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

// viewSimulationSource reads simulation accounts at one bank view's slot.
// The view read's validation, not this source, decides whether the captured
// set is coherent.
type viewSimulationSource struct{ r *bankview.Read }

func (source viewSimulationSource) GetAccountWithDataLimit(_ uint64, key solana.PublicKey, limit uint64) (*accounts.Account, error) {
	acct, err := source.r.Account(key)
	if err != nil {
		return nil, err
	}
	if acct == nil {
		return nil, accountsdb.ErrNoAccount
	}
	if uint64(len(acct.Data)) > limit {
		return nil, accountsdb.ErrAccountDataLimit
	}
	out := acct.Clone()
	out.Key = key
	return out, nil
}

// viewExecutionBank builds the execution parameters of the bank r selects, as
// Agave simulates against a frozen bank: its sysvars, features, fee governor,
// blockhash queue and epoch stakes. Sysvars are read as accounts at the view's
// slot, so views retain no sysvar snapshot.
func viewExecutionBank(ctx context.Context, r *bankview.Read) (*replay.ChainTipSnapshot, error) {
	view := r.View
	if view.Features == nil {
		return nil, fmt.Errorf("bank %d has no feature set", view.Slot)
	}
	var keys []solana.PublicKey
	_ = sealevel.RangeBankSysvarAddresses(func(key solana.PublicKey) error {
		keys = append(keys, key)
		return nil
	})
	found, err := r.Accounts(ctx, keys)
	if err != nil {
		return nil, err
	}
	present := make([]*accounts.Account, 0, len(found))
	for i, acct := range found {
		if acct == nil {
			continue // for example the deprecated Fees sysvar
		}
		owned := acct.Clone()
		owned.Key = keys[i]
		present = append(present, owned)
	}
	sysvars, err := sealevel.NewBankSysvarsOwned(view.Slot, present...)
	if err != nil {
		return nil, fmt.Errorf("bank %d sysvars: %w", view.Slot, err)
	}
	governor := view.FeeRateGovernor
	return &replay.ChainTipSnapshot{
		Slot: view.Slot, Bankhash: view.Bankhash, BlockHeight: view.BlockHeight,
		Features: view.Features, PrevFeeGovernor: &governor,
		LastBlockhash: view.Blockhash, LatestEvictedBlockhash: [32]byte(view.LatestEvictedBlockhash),
		BankSysvars: sysvars, EpochStakes: view.EpochStakes, TotalEpochStake: view.TotalEpochStake,
	}, nil
}

// prepareSimulation captures, inside one validated bank-view read at the
// requested commitment, every account the transaction can load, then seals the
// capture so execution never reads live state. A fork switch or fold that
// overlaps the capture invalidates the read and bankview.Publisher.Do runs
// the capture again.
func (server *RpcServer) prepareSimulation(ctx context.Context, tx *solana.Transaction, commitment string, minimum *uint64, responseKeys []solana.PublicKey, captureExecution bool) (*sealevel.SlotCtx, *solana.Transaction, *simulationAccounts, *replay.ChainTipSnapshot, *replay.TransactionError, error) {
	if server.acctsDb == nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("node has no simulation accounts database")
	}
	conf := commitmentConfig{level: bankview.Finalized}
	if commitment != "" {
		level, err := bankview.ParseCommitment(commitment)
		if err != nil {
			return nil, nil, nil, nil, nil, &InvalidParamsError{Message: "invalid commitment"}
		}
		conf.level = level
	}
	if minimum != nil {
		conf.minContextSlot = *minimum
	}
	var (
		slotCtx  *sealevel.SlotCtx
		resolved *solana.Transaction
		view     *simulationAccounts
		bank     *replay.ChainTipSnapshot
		txErr    *replay.TransactionError
	)
	err := server.readView(ctx, conf, func(r *bankview.Read) error {
		var err error
		if bank, err = viewExecutionBank(ctx, r); err != nil {
			return err
		}
		view = &simulationAccounts{bank: bank, source: viewSimulationSource{r: r}, accounts: make(map[solana.PublicKey]*accounts.Account), remaining: maxSimulationAccountBytes}
		if slotCtx, err = newSimulationBankContext(bank, server.acctsDb, view); err != nil {
			return err
		}
		slotCtx.TraceCtx = ctx
		for _, lookup := range tx.Message.AddressTableLookups {
			view.capture(ctx, lookup.AccountKey)
		}
		resolved, txErr = replay.ResolveAddressTableLookups(slotCtx, tx)
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
		return view.err
	})
	if err != nil {
		return nil, nil, nil, nil, nil, err
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
