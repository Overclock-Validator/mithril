package blockprod

import (
	"math"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
)

// TestEnv wires a minimal leader bank for forge and pipeline tests.
type TestEnv struct {
	SlotCtx    *sealevel.SlotCtx
	Controller *Controller
	Bank       *WorkingBank
	cleanup    func()
}

type TestEnvConfig struct {
	Limits              costmodel.Limits
	EntryHash           solana.Hash
	Sink                BatchSink
	TransactionStatuses *replay.TransactionStatusView
	TPUGated            bool
}

func NewTestEnv(cfg TestEnvConfig) *TestEnv {
	limits := cfg.Limits
	if limits.BlockCost == 0 {
		limits = costmodel.DefaultLimits()
	}
	entryHash := cfg.EntryHash
	if entryHash == (solana.Hash{}) {
		entryHash = solana.Hash{0xab}
	}
	statuses := cfg.TransactionStatuses
	if statuses == nil {
		// Isolated banks have a known-empty ancestor lineage. Live leader banks
		// receive replay's pinned, complete view through ParentContext instead.
		statuses = replay.NewTransactionStatusCache().View()
	}

	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)

	mem := accounts.NewMemAccounts()
	seedTestAccounts(mem)

	slotCtx := &sealevel.SlotCtx{
		Slot:            42,
		Features:        feats,
		Accounts:        mem,
		FeeRateGovernor: &sealevel.FeeRateGovernor{PrevLamportsPerSignature: 5000},
		LastBlockhash:   txfixture.TestBlockhash(),
		AcctMapsMu:      &sync.Mutex{},
		ModifiedAccts:   make(map[solana.PublicKey]bool),
		WritableAccts:   make(map[solana.PublicKey]bool),
	}

	prevRBH := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	rbh := sealevel.SysvarRecentBlockhashes{{Blockhash: txfixture.TestBlockhash(), FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}}}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &rbh

	prevRent := sealevel.SysvarCache.Rent.Sysvar
	rent := sealevel.NewDefaultRentSysvar()
	sealevel.SysvarCache.Rent.Sysvar = &rent

	bank := NewWorkingBank(BankConfig{
		SlotCtx:             slotCtx,
		Slot:                slotCtx.Slot,
		Leader:              txfixture.PayerPubkey(),
		Limits:              limits,
		EntryHash:           entryHash,
		Sink:                cfg.Sink,
		TransactionStatuses: statuses,
		TPUGated:            cfg.TPUGated,
	})
	controller := NewController()
	controller.SetWorkingBank(bank)

	return &TestEnv{
		SlotCtx:    slotCtx,
		Controller: controller,
		Bank:       bank,
		cleanup: func() {
			sealevel.SysvarCache.RecentBlockHashes.Sysvar = prevRBH
			sealevel.SysvarCache.Rent.Sysvar = prevRent
		},
	}
}

func (e *TestEnv) Close() {
	if e.cleanup != nil {
		e.cleanup()
	}
}

// Fund creates a system account at key holding lamports.
func (e *TestEnv) Fund(key solana.PublicKey, lamports uint64) error {
	return e.SlotCtx.SetAccount(key, &accounts.Account{
		Key:       key,
		Lamports:  lamports,
		Owner:     addresses.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	})
}

func seedTestAccounts(mem accounts.MemAccounts) {
	systemAcct := &accounts.Account{
		Key:        addresses.SystemProgramAddr,
		Lamports:   1,
		Owner:      addresses.NativeLoaderAddr,
		Executable: true,
		RentEpoch:  math.MaxUint64,
	}
	_ = mem.SetAccountWithoutLock(addresses.SystemProgramAddr, systemAcct)

	payerAcct := &accounts.Account{
		Key:       txfixture.PayerPubkey(),
		Lamports:  10_000_000_000,
		Owner:     addresses.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	}
	_ = mem.SetAccountWithoutLock(txfixture.PayerPubkey(), payerAcct)

	destAcct := &accounts.Account{
		Key:       txfixture.DestPubkey(),
		Lamports:  10_000_000,
		Owner:     addresses.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	}
	_ = mem.SetAccountWithoutLock(txfixture.DestPubkey(), destAcct)
}
