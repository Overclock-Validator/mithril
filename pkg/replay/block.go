package replay

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/trace"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/forkchoice"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/panjf2000/ants/v2"
)

// SlotCtxSetter is implemented by types that accept a SlotCtx update (e.g. RpcServer).
type SlotCtxSetter interface {
	SetSlotCtx(slotCtx *sealevel.SlotCtx)
}

// BlockFetchOpts contains options for parallel block fetching
type BlockFetchOpts struct {
	MaxRPS          int    // Rate limit (requests per second), 0 = use default
	MaxInflight     int    // Max concurrent workers, 0 = use default
	TipPollMs       int    // Tip poll interval ms, 0 = use default
	TipSafetyMargin uint64 // Don't fetch within N slots of tip, 0 = use default

	// Mode thresholds (hysteresis)
	NearTipThreshold        int // Enter near-tip when gap <= this, 0 = use default
	CatchupThreshold        int // Exit near-tip when gap >= this, 0 = use default
	CatchupTipGateThreshold int // Only apply safety margin when gap > this, 0 = use default

	// Near-tip tuning
	NearTipPollMs    int // Faster poll interval in near-tip, 0 = use default
	NearTipLookahead int // Slots ahead to schedule in near-tip, 0 = use default

	// Optional TPU QUIC endpoint to publish in native turbine gossip.
	TPUQUICAdvertise *net.UDPAddr

	// Shared gossip client for turbine repair, CRDS contact info, and block production.
	// When set, blockstream does not create a second gossip client.
	GossipClient *gossip.Client

	// Optional validator identity and Alpenglow socket for native gossip (TVU/Alpenglow CRDS).
	GossipIdentity           ed25519.PrivateKey
	TurbineAlpenglowBindAddr string

	// HasLocalLeaderCommit returns true once local block production finalized the slot.
	HasLocalLeaderCommit func(slot uint64) bool

	// TurbineRepairOnly disables RPC block fetch for turbine source; blocks come
	// from native turbine/repair only (RPC still used for tip polling).
	TurbineRepairOnly bool
}

var SerializedParameterArena *arena.Arena[byte]

// Commit state tracking for panic recovery
// commitInProgress is set true during the critical window between StoreAccounts and StoreBankHashForSlot.
// If a panic occurs while this is true, AccountsDB may be corrupted (partial writes).
// If a panic occurs while this is false (e.g., divergence during tx loop), AccountsDB is safe.
var commitInProgress atomic.Bool
var commitSlot atomic.Uint64 // The slot currently being committed (for error messages)

// CurrentRunID is a unique identifier for this replay session, used to correlate logs
var CurrentRunID string

// GenerateRunID creates a short random hex string for log correlation
func GenerateRunID() string {
	b := make([]byte, 4) // 8 hex chars
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp-based ID if crypto/rand fails
		return fmt.Sprintf("%08x", time.Now().UnixNano()&0xFFFFFFFF)
	}
	return hex.EncodeToString(b)
}

// IsCommitInProgress returns true if we're in the critical commit window.
// Used by panic recovery to determine if AccountsDB may be corrupted.
func IsCommitInProgress() bool {
	return commitInProgress.Load()
}

// GetCommitSlot returns the slot currently being committed (0 if not in commit)
func GetCommitSlot() uint64 {
	return commitSlot.Load()
}

func formatBlockSourceStatus(fetchStats blockstream.FetchStatsSnapshot) string {
	if fetchStats.SourceStatus == "" || fetchStats.SourceStatus == fetchStats.CurrentSource {
		return fetchStats.CurrentSource
	}
	return fetchStats.SourceStatus
}

// ReplayResult contains the result of a replay operation, including shutdown state
type ReplayResult struct {
	// LastPersistedSlot is the last slot whose state was successfully persisted to AccountsDB
	LastPersistedSlot uint64
	// LastPersistedBankhash is the bankhash of the last persisted slot
	LastPersistedBankhash []byte
	// LastBlockHeight is the block height of the last persisted slot.
	LastBlockHeight uint64
	// WasCancelled indicates whether replay was interrupted by context cancellation
	WasCancelled bool
	// Error contains any error that occurred during replay
	Error error

	// Resume context - populated on graceful shutdown (Ctrl+C) for proper resume
	LastAcctsLtHash          *lthash.LtHash // LtHash at end of last persisted slot
	LastLamportsPerSignature uint64         // FeeRateGovernor.LamportsPerSignature
	LastPrevLamportsPerSig   uint64         // FeeRateGovernor.PrevLamportsPerSignature
	LastNumSignatures        uint64         // SlotCtx.NumSignatures

	// Blockhash context - required because appendvec writes are not fsynced
	LastRecentBlockhashes *sealevel.SysvarRecentBlockhashes // 150 entries, newest first
	LastEvictedBlockhash  [32]byte                          // 151st blockhash
	LastBlockhash         [32]byte                          // blockhash of last replayed slot

	// SlotHashes context - same issue, vote program needs accurate slot→hash mappings
	LastSlotHashes *sealevel.SysvarSlotHashes

	// ReplayCtx fields - for resume independence from stale manifest
	LastCapitalization uint64
	LastSlotsPerYear   float64
	LastInflation      rewards.Inflation

	// ComputedEpochStakes contains epoch stakes computed at boundaries during this run.
	// Key: epoch number (the leader schedule epoch), Value: serialized JSON
	// This must be persisted to state file for correct resume across epoch boundaries.
	ComputedEpochStakes map[uint64][]byte

	// StateWrittenOnCancel indicates that the state file was already written during
	// cancellation handling, so the caller should skip the final write
	StateWrittenOnCancel bool
}

// OnCancelWriteState is a callback that writes state immediately on cancellation.
// This eliminates the timing window between bankhash persistence and state file update.
type OnCancelWriteState func(result *ReplayResult) error

// ResumeState contains the state needed to properly configure the first block when resuming.
// This is passed to ReplayBlocks when resuming from a previous run.
type ResumeState struct {
	// ParentSlot is the slot of the last successfully replayed block (= state.LastSlot)
	ParentSlot uint64
	// ParentEpoch is the epoch containing ParentSlot. It is required when a
	// speculative fork switch restores the durable restart boundary.
	ParentEpoch uint64
	// ParentBlockHeight is the block height of the last successfully replayed block.
	ParentBlockHeight uint64
	// ParentBankhash is the bankhash of the parent slot
	ParentBankhash []byte
	// Alpenglow block identity and chained root are a coherent pair needed to
	// extend the finalized parent after a rooted-durable restart.
	HasAlpenglowIdentity bool
	AlpenglowBlockID     solana.Hash
	AlpenglowChainedRoot solana.Hash
	// AcctsLtHash is the cumulative LtHash at the end of the parent slot
	AcctsLtHash *lthash.LtHash
	// LamportsPerSignature for reconstructing FeeRateGovernor
	LamportsPerSignature uint64
	// PrevLamportsPerSignature for reconstructing FeeRateGovernor
	PrevLamportsPerSignature uint64
	// NumSignatures is the total signature count at end of parent slot
	NumSignatures uint64

	// Blockhash context - required because appendvec writes are not fsynced
	RecentBlockhashes *sealevel.SysvarRecentBlockhashes // 150 entries, newest first
	EvictedBlockhash  [32]byte                          // 151st blockhash
	LastBlockhash     [32]byte                          // blockhash of last slot (parent for next)

	// SlotHashes context - vote program needs accurate slot→hash mappings
	SlotHashes *sealevel.SysvarSlotHashes

	// ReplayCtx fields - so resume uses fresh values instead of stale manifest
	Capitalization          uint64
	SlotsPerYear            float64
	InflationInitial        float64
	InflationTerminal       float64
	InflationTaper          float64
	InflationFoundation     float64
	InflationFoundationTerm float64
	// TransactionCount is exact when non-nil, including an explicit zero.
	TransactionCount *uint64

	// ComputedEpochStakes contains epoch stakes computed at boundaries.
	// Key: epoch number (the leader schedule epoch), Value: serialized JSON
	// Required for correct leader schedule computation on resume.
	ComputedEpochStakes map[uint64][]byte
}

// serializeAllEpochStakes serializes all epoch stakes in the global cache.
// Returns a map of epoch -> serialized JSON bytes.
func serializeAllEpochStakes() map[uint64][]byte {
	epochs := global.GetAllCachedEpochs()
	if len(epochs) == 0 {
		return nil
	}

	result := make(map[uint64][]byte, len(epochs))
	for _, epoch := range epochs {
		data, err := global.SerializeEpochStakes(epoch)
		if err != nil {
			mlog.Log.Warnf("Failed to serialize epoch %d stakes: %v", epoch, err)
			continue
		}
		result[epoch] = data
	}
	return result
}

func resolveAddrTableLookups(accountsDb *accountsdb.AccountsDb, block *b.Block, spec *SpeculativeReplay) error {
	tables := make(map[solana.PublicKey]solana.PublicKeySlice)

	for _, tx := range block.Transactions {
		if !tx.Message.IsVersioned() {
			continue
		}

		for _, addrTableKey := range tx.Message.GetAddressTableLookups().GetTableIDs() {
			tables[addrTableKey] = nil
		}
	}

	tablesSlice := make([]solana.PublicKey, 0, len(tables))
	for t := range tables {
		tablesSlice = append(tablesSlice, t)
	}
	accts := make([]*accounts.Account, len(tablesSlice))
	for i, table := range tablesSlice {
		acct, err := loadAccountForBlockReplay(accountsDb, spec, block.ParentSlot, block.Slot, table)
		if err != nil {
			return err
		}
		accts[i] = acct
	}

	for i := range tablesSlice {
		if accts[i] == nil || len(accts[i].Data) == 0 {
			delete(tables, tablesSlice[i])
			continue
		}
		addrLookupTable, err := sealevel.UnmarshalAddressLookupTable(accts[i].Data)
		if err != nil {
			return err
		}
		tables[tablesSlice[i]] = addrLookupTable.Addresses
	}

txResolveLoop:
	for _, tx := range block.Transactions {
		if !tx.Message.IsVersioned() || tx.Message.AddressTableLookups.NumLookups() == 0 {
			continue
		}
		for _, addrTableKey := range tx.Message.GetAddressTableLookups().GetTableIDs() {
			if _, ok := tables[addrTableKey]; !ok {
				continue txResolveLoop
			}
		}
		err := tx.Message.SetAddressTables(tables)
		if err != nil {
			return err
		}

		err = tx.Message.ResolveLookups()
		if err != nil {
			return err
		}
	}

	return nil
}

// ResolveAddrTableLookupsForTx resolves a single tx's address-table lookups
// against accountsdb at the given slot.
//
// No-op for legacy or empty-lookup versioned txs. Returns wrapped errors so
// callers can map missing/invalid tables to AddressLookupTableNotFound or
// InvalidAddressLookupTableData.
func ResolveAddrTableLookupsForTx(ctx context.Context, accountsDb *accountsdb.AccountsDb, slot uint64, tx *solana.Transaction) error {
	if !tx.Message.IsVersioned() || tx.Message.AddressTableLookups.NumLookups() == 0 {
		return nil
	}

	tableIDs := tx.Message.GetAddressTableLookups().GetTableIDs()
	accts := make([]*accounts.Account, len(tableIDs))
	for i, tableID := range tableIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		acct, err := ResolveActiveAccount(accountsDb, slot, tableID)
		if err != nil {
			return err
		}
		accts[i] = acct
	}

	tables := make(map[solana.PublicKey]solana.PublicKeySlice, len(tableIDs))
	for i, key := range tableIDs {
		if accts[i] == nil || len(accts[i].Data) == 0 {
			return fmt.Errorf("address lookup table %s not found", key)
		}
		addrLookupTable, err := sealevel.UnmarshalAddressLookupTable(accts[i].Data)
		if err != nil {
			return fmt.Errorf("address lookup table %s: invalid data: %w", key, err)
		}
		tables[key] = addrLookupTable.Addresses
	}

	if err := tx.Message.SetAddressTables(tables); err != nil {
		return err
	}
	return tx.Message.ResolveLookups()
}

func extractAndDedupeBlockAccts(block *b.Block) []solana.PublicKey {
	var numPubkeys int
	for _, tx := range block.Transactions {
		numPubkeys += len(tx.Message.AccountKeys)
	}

	numPubkeys += len(block.UpdatedAccts)

	pubkeyMap := make(map[solana.PublicKey]struct{}, numPubkeys)

	for _, tx := range block.Transactions {
		for _, pk := range tx.Message.AccountKeys {
			pubkeyMap[pk] = struct{}{}
		}
	}

	for _, pk := range block.UpdatedAccts {
		pubkeyMap[pk] = struct{}{}
	}

	pubkeys := make([]solana.PublicKey, len(pubkeyMap))
	i := 0
	for pk := range pubkeyMap {
		pubkeys[i] = pk
		i++
	}

	return pubkeys
}

func isNativeProgram(pubkey solana.PublicKey) bool {
	if pubkey == a.SystemProgramAddr || pubkey == a.BpfLoaderUpgradeableAddr ||
		pubkey == a.BpfLoader2Addr || pubkey == a.BpfLoaderDeprecatedAddr ||
		pubkey == a.VoteProgramAddr || pubkey == a.StakeProgramAddr ||
		pubkey == a.ConfigProgramAddr || pubkey == a.StakeProgramConfigAddr ||
		pubkey == a.NativeLoaderAddr {
		return true
	} else {
		return false
	}
}

// cacheFeesSysvar loads SysvarFees when present. Clusters with DisableFeesSysvar
// active omit this account; fee semantics come from block FeeRateGovernor instead.
func cacheFeesSysvar(acctsDb *accountsdb.AccountsDb) {
	acct, err := acctsDb.GetAccount(0, sealevel.SysvarFeesAddr)
	if errors.Is(err, accountsdb.ErrNoAccount) {
		return
	}
	if err != nil {
		panic("unable to get fees sysvar when caching sysvars")
	}
	var fees sealevel.SysvarFees
	decoder := bin.NewBinDecoder(acct.Data)
	fees.MustUnmarshalWithDecoder(decoder)
	sealevel.SysvarCache.Fees.Sysvar = &fees
	sealevel.SysvarCache.Fees.Acct = acct
}

func cacheConstantSysvars(acctsDb *accountsdb.AccountsDb) {
	{
		acct, err := acctsDb.GetAccount(0, sealevel.SysvarEpochScheduleAddr)
		if err != nil {
			panic("unable to get epochschedule when caching sysvars")
		}
		decoder := bin.NewBinDecoder(acct.Data)
		var epochSchedule sealevel.SysvarEpochSchedule
		epochSchedule.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.EpochSchedule.Sysvar = &epochSchedule
		sealevel.SysvarCache.EpochSchedule.Acct = acct
	}

	{
		acct, err := acctsDb.GetAccount(0, sealevel.SysvarRentAddr)
		if err != nil {
			panic("unable to get rent sysvar when caching sysvars")
		}
		var rent sealevel.SysvarRent
		decoder := bin.NewBinDecoder(acct.Data)
		rent.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.Rent.Sysvar = &rent
		sealevel.SysvarCache.Rent.Acct = acct
	}

	cacheFeesSysvar(acctsDb)

	{
		acct, err := acctsDb.GetAccount(0, sealevel.SysvarEpochRewardsAddr)
		if err != nil {
			panic("unable to get epoch rewards sysvar when caching sysvars")
		}
		var rewards sealevel.SysvarEpochRewards
		decoder := bin.NewBinDecoder(acct.Data)
		rewards.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.EpochRewards.Sysvar = &rewards
		sealevel.SysvarCache.EpochRewards.Acct = acct
	}
}

func loadAccountForBlockReplay(
	accountsDb *accountsdb.AccountsDb,
	spec *SpeculativeReplay,
	parentSlot, blockSlot uint64,
	pk solana.PublicKey,
) (*accounts.Account, error) {
	if spec != nil && spec.UseStoreForParent(parentSlot) {
		return spec.Resolve(parentSlot, pk, accountsDb)
	}
	return accountsDb.GetAccountDurable(parentSlot, pk)
}

// loadAccountLiveOrParentForReplay returns the in-slot working account when present,
// otherwise parent state at parentSlot (via speculative store when parent is unfinalized).
func loadAccountLiveOrParentForReplay(
	acctsDb *accountsdb.AccountsDb,
	spec *SpeculativeReplay,
	slotCtx *sealevel.SlotCtx,
	pk solana.PublicKey,
) (*accounts.Account, error) {
	if acct, err := slotCtx.GetAccount(pk); err == nil {
		return acct, nil
	}
	acct, err := loadAccountForBlockReplay(acctsDb, spec, slotCtx.ParentSlot, slotCtx.Slot, pk)
	if err != nil {
		return nil, err
	}
	return acct.Clone(), nil
}

// feeParentAccountLoader returns a fees.ParentAccountLoader that resolves missing
// fee-collector accounts from speculative parent state when needed.
func feeParentAccountLoader(
	acctsDb *accountsdb.AccountsDb,
	spec *SpeculativeReplay,
	slotCtx *sealevel.SlotCtx,
) fees.ParentAccountLoader {
	return func(pk solana.PublicKey) (*accounts.Account, error) {
		acct, err := loadAccountForBlockReplay(acctsDb, spec, slotCtx.ParentSlot, slotCtx.Slot, pk)
		if err != nil {
			return nil, err
		}
		return acct.Clone(), nil
	}
}

func loadBlockAccountsAndUpdateSysvars(
	accountsDb *accountsdb.AccountsDb,
	block *b.Block,
	epochSchedule *sealevel.SysvarEpochSchedule,
	alpenglowClock bool,
	spec *SpeculativeReplay,
) (accounts.Accounts, accounts.Accounts, error) {
	err := resolveAddrTableLookups(accountsDb, block, spec)
	if err != nil {
		return nil, nil, err
	}

	dedupedAccts := extractAndDedupeBlockAccts(block)
	useSpecStore := spec != nil && spec.UseStoreForParent(block.ParentSlot)

	var slotAccts []*accounts.Account
	if useSpecStore {
		slotAccts = make([]*accounts.Account, 0, len(dedupedAccts))
		for _, pk := range dedupedAccts {
			acct, err := spec.Resolve(block.ParentSlot, pk, accountsDb)
			if err != nil {
				return nil, nil, err
			}
			slotAccts = append(slotAccts, acct)
		}
	} else {
		ctx := context.Background()
		var err error
		slotAccts, err = accountsDb.GetAccountsBatch(ctx, block.Slot, dedupedAccts)
		if err != nil {
			return nil, nil, err
		}
	}

	numAccts := uint64(len(slotAccts))
	accts := accounts.NewMemAccountsWithLen(numAccts)
	parentAccts := accounts.NewMemAccountsWithLen(numAccts)

	for _, acct := range slotAccts {
		err = accts.SetAccountWithoutLock(acct.Key, acct)
		if err != nil {
			return nil, nil, err
		}

		err = parentAccts.SetAccountWithoutLock(acct.Key, acct)
		if err != nil {
			return nil, nil, err
		}
	}

	block.FeeRateGovernor = sealevel.NewFeeRateGovernorDerived(block.PrevFeeRateGovernor, block.PrevNumSignatures)
	if block.FeeRateGovernor.PrevLamportsPerSignature == 0 {
		block.FeeRateGovernor.PrevLamportsPerSignature = block.InitialPreviousLamportsPerSignature
	}

	// load sysvar accounts and assign them to the sysvar cache
	{
		// update and cache clock sysvar
		{
			clockAcct, err := loadAccountForBlockReplay(accountsDb, spec, block.ParentSlot, block.Slot, sealevel.SysvarClockAddr)
			if err != nil {
				panic("unable to retrieve clock sysvar when updating clock")
			}
			clockAcct = clockAcct.Clone()

			var clock sealevel.SysvarClock
			if useSpecStore {
				decoder := bin.NewBinDecoder(clockAcct.Data)
				if err := clock.UnmarshalWithDecoder(decoder); err != nil {
					panic("unable to unmarshal clock sysvar")
				}
			} else if sealevel.SysvarCache.Clock.Sysvar != nil {
				clock = *sealevel.SysvarCache.Clock.Sysvar
				newClockBytes := clock.MustMarshal()
				if len(newClockBytes) != len(clockAcct.Data) {
					panic(fmt.Sprintf("Clock data length mismatch: marshaled=%d, account=%d",
						len(newClockBytes), len(clockAcct.Data)))
				}
				copy(clockAcct.Data, newClockBytes)
			} else {
				decoder := bin.NewBinDecoder(clockAcct.Data)
				if err := clock.UnmarshalWithDecoder(decoder); err != nil {
					panic("unable to unmarshal clock sysvar")
				}
			}

			err = parentAccts.SetAccountWithoutLock(sealevel.SysvarClockAddr, clockAcct.Clone())
			if err != nil {
				panic("unable to set clock sysvar to accts")
			}

			err = updateClockSysvarForMode(&clock, block, epochSchedule, alpenglowClock)
			if err != nil {
				panic(fmt.Sprintf("failed to update clock sysvar: %s", err))
			}

			newClockBytes := clock.MustMarshal()
			copy(clockAcct.Data, newClockBytes)
			sealevel.SysvarCache.Clock.Sysvar = &clock
			sealevel.SysvarCache.Clock.Acct = clockAcct

			err = accts.SetAccountWithoutLock(sealevel.SysvarClockAddr, clockAcct)
			if err != nil {
				panic("unable to set clock sysvar to accts")
			}
		}

		// update and cache SlotHashes sysvar
		{
			slotHashesAcct, err := loadAccountForBlockReplay(accountsDb, spec, block.ParentSlot, block.Slot, sealevel.SysvarSlotHashesAddr)
			if err != nil {
				panic("unable to retrieve slothashes sysvar from acctsdb")
			}
			slotHashesAcct = slotHashesAcct.Clone()

			var slotHashes sealevel.SysvarSlotHashes

			if useSpecStore || sealevel.SysvarCache.SlotHashes.Sysvar == nil {
				decoder := bin.NewBinDecoder(slotHashesAcct.Data)
				err = slotHashes.UnmarshalWithDecoder(decoder)
				if err != nil {
					panic("unable to unmarshal slothashes sysvar")
				}
			} else {
				slotHashes = *sealevel.SysvarCache.SlotHashes.Sysvar
				newData := slotHashes.MustMarshal()
				if len(newData) != len(slotHashesAcct.Data) {
					panic(fmt.Sprintf("SlotHashes data length mismatch: marshaled=%d, account=%d",
						len(newData), len(slotHashesAcct.Data)))
				}
				copy(slotHashesAcct.Data, newData)
			}

			// Set parentAccts BEFORE updating slotHashes to ensure LtHash delta is computed correctly
			err = parentAccts.SetAccountWithoutLock(sealevel.SysvarSlotHashesAddr, slotHashesAcct.Clone())
			if err != nil {
				panic("unable to set slothashes sysvar to accountsdb")
			}

			// Now update with the new slot/bankhash
			slotHashes.Update(block.Slot, block.ParentSlot, block.ParentBankhash)
			newSlotHashesBytes := slotHashes.MustMarshal()
			copy(slotHashesAcct.Data, newSlotHashesBytes)
			sealevel.SysvarCache.SlotHashes.Sysvar = &slotHashes
			sealevel.SysvarCache.SlotHashes.Acct = slotHashesAcct

			err = accts.SetAccountWithoutLock(sealevel.SysvarSlotHashesAddr, slotHashesAcct)
			if err != nil {
				panic("unable to set slothashes sysvar to accountsdb")
			}
		}

		// cache RecentBlockhashes sysvar
		{
			recentBlockhashesAcct, err := loadAccountForBlockReplay(accountsDb, spec, block.ParentSlot, block.Slot, sealevel.SysvarRecentBlockHashesAddr)
			if err != nil {
				panic("unable to get recentblockhashes")
			}
			recentBlockhashesAcct = recentBlockhashesAcct.Clone()

			if useSpecStore || sealevel.SysvarCache.RecentBlockHashes.Sysvar == nil {
				decoder := bin.NewBinDecoder(recentBlockhashesAcct.Data)
				var recentBlockhashes sealevel.SysvarRecentBlockhashes
				recentBlockhashes.MustUnmarshalWithDecoder(decoder)
				sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recentBlockhashes
				sealevel.SysvarCache.RecentBlockHashes.Acct = recentBlockhashesAcct

				if !useSpecStore && len(recentBlockhashes) > 0 {
					mlog.Log.Infof("loaded RecentBlockhashes sysvar: %d entries, newest=%x, oldest=%x",
						len(recentBlockhashes), recentBlockhashes[0].Blockhash[:8], recentBlockhashes[len(recentBlockhashes)-1].Blockhash[:8])
				}
			} else {
				recentBlockhashes := sealevel.SysvarCache.RecentBlockHashes.Sysvar
				newData := recentBlockhashes.MustMarshal()
				if len(newData) != len(recentBlockhashesAcct.Data) {
					panic(fmt.Sprintf("RecentBlockhashes data length mismatch: marshaled=%d, account=%d",
						len(newData), len(recentBlockhashesAcct.Data)))
				}
				copy(recentBlockhashesAcct.Data, newData)
				sealevel.SysvarCache.RecentBlockHashes.Acct = recentBlockhashesAcct
			}

			// Set parentAccts before PushLatest at bankhash time.
			err = parentAccts.SetAccountWithoutLock(sealevel.SysvarRecentBlockHashesAddr, recentBlockhashesAcct.Clone())
			if err != nil {
				panic("unable to set recentblockhashes sysvar to accts")
			}

			err = accts.SetAccountWithoutLock(sealevel.SysvarRecentBlockHashesAddr, recentBlockhashesAcct)
			if err != nil {
				panic("unable to set recentblockhashes sysvar to accts")
			}
		}

		// cache SlotHistory sysvar
		{
			slotHistoryAcct, err := loadAccountForBlockReplay(accountsDb, spec, block.ParentSlot, block.Slot, sealevel.SysvarSlotHistoryAddr)
			if err != nil {
				panic("unable to get slothistory")
			}
			slotHistoryAcct = slotHistoryAcct.Clone()

			err = parentAccts.SetAccountWithoutLock(sealevel.SysvarSlotHistoryAddr, slotHistoryAcct.Clone())
			if err != nil {
				panic("unable to set slothistory sysvar to accts")
			}

			var slotHistory sealevel.SysvarSlotHistory
			if useSpecStore || sealevel.SysvarCache.SlotHistory.Sysvar == nil {
				decoder := bin.NewBinDecoder(slotHistoryAcct.Data)
				slotHistory.MustUnmarshalWithDecoder(decoder)
			} else {
				slotHistory = *sealevel.SysvarCache.SlotHistory.Sysvar
				newData := slotHistory.MustMarshal()
				if len(newData) != len(slotHistoryAcct.Data) {
					panic(fmt.Sprintf("SlotHistory data length mismatch: marshaled=%d, account=%d",
						len(newData), len(slotHistoryAcct.Data)))
				}
				copy(slotHistoryAcct.Data, newData)
			}
			sealevel.SysvarCache.SlotHistory.Sysvar = &slotHistory
			sealevel.SysvarCache.SlotHistory.Acct = slotHistoryAcct

			err = accts.SetAccountWithoutLock(sealevel.SysvarSlotHistoryAddr, slotHistoryAcct)
			if err != nil {
				panic("unable to set slothistory sysvar to accts")
			}
		}

		// cache StakeHistory sysvar
		{
			stakeHistoryAcct, err := loadAccountForBlockReplay(accountsDb, spec, block.ParentSlot, block.Slot, sealevel.SysvarStakeHistoryAddr)
			if err != nil {
				panic("unable to get stakehistory")
			}

			var setStakeHistoryParent bool
			if len(block.EpochUpdatedAccts) != 0 {
				for _, a := range block.ParentEpochUpdatedAccts {
					if a != nil {
						if a.Key == sealevel.SysvarStakeHistoryAddr {
							err = parentAccts.SetAccountWithoutLock(sealevel.SysvarStakeHistoryAddr, a.Clone())
							if err != nil {
								panic("unable to set stakehistory sysvar to accts")
							}
							setStakeHistoryParent = true
						}
					}
				}
			}

			if !setStakeHistoryParent {
				err = parentAccts.SetAccountWithoutLock(sealevel.SysvarStakeHistoryAddr, stakeHistoryAcct.Clone())
				if err != nil {
					panic("unable to set stakehistory sysvar to accts")
				}
			}

			decoder := bin.NewBinDecoder(stakeHistoryAcct.Data)
			var stakeHistory sealevel.SysvarStakeHistory
			stakeHistory.MustUnmarshalWithDecoder(decoder)
			sealevel.SysvarCache.StakeHistory.Sysvar = &stakeHistory
			sealevel.SysvarCache.StakeHistory.Acct = stakeHistoryAcct

			err = accts.SetAccountWithoutLock(sealevel.SysvarStakeHistoryAddr, stakeHistoryAcct)
			if err != nil {
				panic("unable to set stakehistory sysvar to accts")
			}
		}

		// cache LastRestartSlot sysvar
		{
			lastRestartSlotAcct, err := loadAccountForBlockReplay(accountsDb, spec, block.ParentSlot, block.Slot, sealevel.SysvarLastRestartSlotAddr)
			if err != nil {
				panic("unable to get last restart slot sysvar acct")
			}

			err = parentAccts.SetAccountWithoutLock(sealevel.SysvarLastRestartSlotAddr, lastRestartSlotAcct.Clone())
			if err != nil {
				panic("unable to set last restart slot sysvar to accts")
			}

			decoder := bin.NewBinDecoder(lastRestartSlotAcct.Data)
			var lastRestartSlot sealevel.SysvarLastRestartSlot
			lastRestartSlot.MustUnmarshalWithDecoder(decoder)
			sealevel.SysvarCache.LastRestartSlot.Sysvar = &lastRestartSlot
			sealevel.SysvarCache.LastRestartSlot.Acct = lastRestartSlotAcct

			err = accts.SetAccountWithoutLock(sealevel.SysvarLastRestartSlotAddr, lastRestartSlotAcct)
			if err != nil {
				panic("unable to set last restart slot sysvar to accts")
			}
		}
	}

	for idx, acct := range block.EpochUpdatedAccts {
		if acct == nil {
			continue
		}

		err = accts.SetAccountWithoutLock(acct.Key, acct.Clone())
		if err != nil {
			panic("unable to setup epoch transition modified acct")
		}

		parentAcct := block.ParentEpochUpdatedAccts[idx].Clone()
		err = parentAccts.SetAccountWithoutLock(acct.Key, parentAcct)
		if err != nil {
			panic("unable to setup epoch transition modified acct")
		}
	}

	return accts, parentAccts, nil
}

// setupInitialVoteAcctsAndStakeAccts populates the vote and stake caches at startup.
//
// For stake accounts, we read ALL delegation fields from AccountsDB rather than trusting
// the manifest's delegation list (which can be stale/incomplete per Firedancer). The flow:
//  1. Load stake pubkeys from stake_pubkeys.idx (built during snapshot processing)
//  2. For each pubkey, read the full stake state from AccountsDB
//  3. Extract delegation fields (VoterPubkey, StakeLamports, epochs, etc.) from AccountsDB
//
// This ensures the stake cache reflects the actual on-chain state, not potentially outdated
// data. Fatal error if index file is missing - indicates corrupt/incomplete AccountsDB.
// NO manifest parameter - derives everything from AccountsDB.
func setupInitialVoteAcctsAndStakeAccts(acctsDb *accountsdb.AccountsDb, block *b.Block) {
	block.VoteTimestamps = make(map[solana.PublicKey]sealevel.BlockTimestamp)
	block.EpochStakesPerVoteAcct = make(map[solana.PublicKey]uint64)

	// Load stake entries from index file built during snapshot processing
	// The index is in the accountsDbDir which is parent of AcctsDir
	// Entries are sorted by (FileId, Offset) for sequential appendvec I/O
	acctsDbDir := filepath.Join(acctsDb.AcctsDir, "..")
	stakeEntries, err := global.LoadStakePubkeyIndex(acctsDbDir)
	if err != nil {
		// Fatal error - stake index is required for resume and must exist
		mlog.Log.Errorf("=======================================================")
		mlog.Log.Errorf("FATAL: stake_pubkeys.idx missing or corrupt: %v", err)
		mlog.Log.Errorf("=======================================================")
		mlog.Log.Errorf("")
		mlog.Log.Errorf("This file is required when resuming from existing AccountsDB.")
		mlog.Log.Errorf("If this is a fresh start, the index should have been created during snapshot loading.")
		mlog.Log.Errorf("")
		mlog.Log.Errorf("To fix, delete AccountsDB and restart from snapshot:")
		mlog.Log.Errorf("  rm -rf %s", acctsDbDir)
		mlog.Log.Errorf("")
		mlog.Log.Errorf("Then set bootstrap.mode = 'new-snapshot' in config.toml")
		mlog.Log.Errorf("=======================================================")
		os.Exit(1)
	}
	mlog.Log.Infof("Loading vote and stake caches (aggregate-only mode, %d stake accounts)", len(stakeEntries))

	var wg sync.WaitGroup

	// Shared aggregated stake totals - built directly from AccountsDB scan
	// Thread-safe: each worker builds local map, then merges under mutex
	voteAcctStakes := make(map[solana.PublicKey]uint64)
	var voteAcctStakesMu sync.Mutex

	// Stake worker pool reads stake accounts and aggregates totals directly
	const maxBatchSize = 2000
	stakeAcctWorkerPool, _ := ants.NewPoolWithFunc(runtime.NumCPU()*2, func(i interface{}) {
		defer wg.Done()

		batch := i.([]accountsdb.StakeIndexEntry)

		// Build local aggregation for this batch
		localStakes := make(map[solana.PublicKey]uint64)

		for _, entry := range batch {
			// Read from AccountsDB
			stakeAcct, err := acctsDb.GetAccount(block.Slot, entry.Pubkey)
			if err != nil {
				continue // Account not found or closed
			}

			stakeState, err := sealevel.UnmarshalStakeState(stakeAcct.Data)
			if err != nil {
				continue // Invalid stake state
			}

			// Only count delegated stake accounts (status must be "Stake")
			if stakeState.Status != sealevel.StakeStateV2StatusStake {
				continue
			}

			// Aggregate stake by vote account
			delegation := stakeState.Stake.Stake.Delegation
			localStakes[delegation.VoterPubkey] += delegation.StakeLamports
		}

		// Merge local aggregation into shared map under lock
		voteAcctStakesMu.Lock()
		for voter, stake := range localStakes {
			voteAcctStakes[voter] += stake
		}
		voteAcctStakesMu.Unlock()
	})

	// Submit FileId-aligned batches for sequential appendvec I/O
	batchStart := 0
	for i := 1; i <= len(stakeEntries); i++ {
		flush := i == len(stakeEntries) ||
			stakeEntries[i].FileId != stakeEntries[batchStart].FileId ||
			i-batchStart >= maxBatchSize
		if flush && i > batchStart {
			wg.Add(1)
			stakeAcctWorkerPool.Invoke(stakeEntries[batchStart:i])
			batchStart = i
		}
	}

	wg.Wait()
	stakeAcctWorkerPool.Release()

	// Load vote accounts from AccountsDB into vote cache
	if err := RebuildVoteCacheFromAccountsDB(acctsDb, block.Slot, voteAcctStakes, 0); err != nil {
		mlog.Log.Warnf("vote cache rebuild had errors: %v", err)
	}
	rebuildAuthorizedVotersFromVoteCache(block.Epoch)

	// Seed EpochStakesPerVoteAcct and TotalEpochStake from the epoch stakes cache,
	// loaded by buildInitialEpochStakesCache() from the manifest. These are
	// epoch-effective stakes (warmup/cooldown applied), matching Agave's
	// get_epoch_stake syscall behavior. The raw AccountsDB scan above uses
	// delegation.StakeLamports which can differ from effective stake.
	epochStakes := global.EpochStakes(block.Epoch)
	if len(epochStakes) == 0 {
		mlog.Log.Errorf("FATAL: no epoch stakes in cache for epoch %d - "+
			"buildInitialEpochStakesCache should have loaded these from manifest", block.Epoch)
		mlog.Log.Errorf("Available cached epochs: %v", global.GetAllCachedEpochs())
		os.Exit(1)
	}
	maps.Copy(block.EpochStakesPerVoteAcct, epochStakes)
	block.TotalEpochStake = global.EpochTotalStake(block.Epoch)

	// Diagnostic: one-time startup comparison of raw scan vs epoch-effective totals
	var rawScanTotal uint64
	for _, stake := range voteAcctStakes {
		rawScanTotal += stake
	}
	if rawScanTotal != block.TotalEpochStake {
		mlog.Log.Infof("startup stake check: rawScanTotal=%d epochEffectiveTotal=%d delta=%d "+
			"(expected difference from warmup/cooldown)",
			rawScanTotal, block.TotalEpochStake, int64(rawScanTotal)-int64(block.TotalEpochStake))
	} else {
		mlog.Log.Infof("startup stake check: rawScanTotal=%d matches epochEffectiveTotal (all stakes fully warmed up)", rawScanTotal)
	}

	// Derive VoteTimestamps from ALL vote accounts in cache (including zero-stake)
	// This matches original manifest behavior where all vote accounts had timestamps populated
	for pk, voteState := range global.VoteCache() {
		if voteState != nil {
			ts := voteState.LastTimestamp()
			if ts != nil {
				block.VoteTimestamps[pk] = *ts
			}
		}
	}
}

func configureInitialBlock(acctsDb *accountsdb.AccountsDb,
	block *b.Block,
	mithrilState *state.MithrilState,
	epochSchedule *sealevel.SysvarEpochSchedule) error {

	// Read from state file manifest_* fields (required)
	if mithrilState.ManifestParentBankhash == "" {
		return fmt.Errorf("state file missing manifest_parent_bankhash - delete AccountsDB and rebuild from snapshot")
	}

	parentBankhash, err := base58.DecodeFromString(mithrilState.ManifestParentBankhash)
	if err != nil {
		return fmt.Errorf("corrupted state file: failed to decode manifest_parent_bankhash: %w", err)
	}
	block.ParentBankhash = parentBankhash
	block.ParentSlot = mithrilState.ManifestParentSlot
	if mithrilState.ManifestLastBlockhash == "" {
		return fmt.Errorf("state file missing manifest_last_blockhash - delete AccountsDB and rebuild from snapshot")
	}
	manifestLastBlockhash, err := base58.DecodeFromString(mithrilState.ManifestLastBlockhash)
	if err != nil {
		return fmt.Errorf("corrupted state file: failed to decode manifest_last_blockhash: %w", err)
	}
	block.LastBlockhash = manifestLastBlockhash

	// LtHash: decode base64, restore with InitWithHash
	if mithrilState.ManifestAcctsLtHash != "" {
		ltHashBytes, err := base64.StdEncoding.DecodeString(mithrilState.ManifestAcctsLtHash)
		if err != nil {
			return fmt.Errorf("corrupted state file: failed to decode manifest_accts_lt_hash: %w", err)
		}
		if len(ltHashBytes) != lthash.HashByteLen {
			return fmt.Errorf("corrupted state file: manifest_accts_lt_hash is %d bytes, want %d", len(ltHashBytes), lthash.HashByteLen)
		}
		block.AcctsLtHash = new(lthash.LtHash).InitWithHash(ltHashBytes)
	}

	block.PrevFeeRateGovernor = reconstructFeeRateGovernor(mithrilState)
	if block.PrevFeeRateGovernor == nil {
		return fmt.Errorf("state file missing manifest_fee_rate_governor - delete AccountsDB and rebuild from snapshot")
	}
	block.PrevNumSignatures = mithrilState.ManifestSignatureCount
	block.InitialPreviousLamportsPerSignature = mithrilState.ManifestLamportsPerSignature

	if mithrilState.ManifestEvictedBlockhash == "" {
		return fmt.Errorf("state file missing manifest_evicted_blockhash - delete AccountsDB and rebuild from snapshot")
	}
	evictedHash, err := base58.DecodeFromString(mithrilState.ManifestEvictedBlockhash)
	if err != nil {
		return fmt.Errorf("corrupted state file: failed to decode manifest_evicted_blockhash: %w", err)
	}
	block.LatestEvictedBlockhash = evictedHash

	if len(mithrilState.ManifestRecentBlockhashes) > 0 {
		recent, err := RecentBlockhashesFromState(mithrilState.ManifestRecentBlockhashes)
		if err != nil {
			return fmt.Errorf("decode manifest RecentBlockhashes: %w", err)
		}
		SeedRecentBlockhashesCache(recent)
	}

	setupInitialVoteAcctsAndStakeAccts(acctsDb, block)
	configureGlobalCtx(block)

	if global.ManageLeaderSchedule() {
		_, err := PrepareLeaderScheduleLocal(block.Epoch, epochSchedule, "")
		if err != nil {
			panic(err)
		}

		var exists bool
		block.Leader, exists = global.LeaderForSlot(block.Slot)
		if !exists {
			return fmt.Errorf("unable to find leader for slot %d", block.Slot)
		}
	}

	setBlockHeight(block)

	return nil
}

// reconstructFeeRateGovernor creates a FeeRateGovernor from state file manifest_* fields
func reconstructFeeRateGovernor(s *state.MithrilState) *sealevel.FeeRateGovernor {
	if s.ManifestFeeRateGovernor == nil {
		return nil
	}
	return &sealevel.FeeRateGovernor{
		TargetLamportsPerSignature: s.ManifestFeeRateGovernor.TargetLamportsPerSignature,
		TargetSignaturesPerSlot:    s.ManifestFeeRateGovernor.TargetSignaturesPerSlot,
		MinLamportsPerSignature:    s.ManifestFeeRateGovernor.MinLamportsPerSignature,
		MaxLamportsPerSignature:    s.ManifestFeeRateGovernor.MaxLamportsPerSignature,
		BurnPercent:                s.ManifestFeeRateGovernor.BurnPercent,
		LamportsPerSignature:       s.ManifestLamportsPerSignature,
		PrevLamportsPerSignature:   s.ManifestLamportsPerSignature, // Initial = current for fresh start
	}
}

func configureBlock(block *b.Block,
	lastSlotCtx *sealevel.SlotCtx,
	epochSchedule *sealevel.SysvarEpochSchedule) error {

	copy(block.ParentBankhash[:], lastSlotCtx.FinalBankhash)
	block.AcctsLtHash = lastSlotCtx.AcctsLtHash
	block.VoteTimestamps = ensureVoteTimestamps(lastSlotCtx.VoteTimestamps)
	block.EpochStakesPerVoteAcct = ensureVoteAccts(lastSlotCtx.VoteAccts)
	block.ParentSlot = lastSlotCtx.Slot
	block.LatestEvictedBlockhash = lastSlotCtx.LatestEvictedBlockhash
	block.PrevFeeRateGovernor = lastSlotCtx.FeeRateGovernor
	block.PrevNumSignatures = lastSlotCtx.NumSignatures
	block.TotalEpochStake = lastSlotCtx.TotalEpochStake

	if global.ManageLeaderSchedule() {
		block.LastBlockhash = lastSlotCtx.Blockhash
	}

	configureGlobalCtx(block)

	if global.ManageLeaderSchedule() {
		// epoch boundary. do not set leader
		if epochSchedule.GetEpoch(block.Slot) == epochSchedule.GetEpoch(lastSlotCtx.Slot) {
			var hasLeader bool
			block.Leader, hasLeader = global.LeaderForSlot(block.Slot)
			if !hasLeader {
				panic(fmt.Sprintf("couldn't find leader for slot %d at epoch boundary", block.Slot))
			}
			var exists bool
			block.Leader, exists = global.LeaderForSlot(block.Slot)
			if !exists {
				return fmt.Errorf("unable to find leader for slot %d", block.Slot)
			}
		}
	}

	setBlockHeight(block)
	return nil
}

// ensureStakeHistorySysvarCached loads StakeHistory sysvar from AccountsDB into cache if not already cached.
// Required on resume because updateEpochStakesAndRefreshVoteCache reads SysvarCache.StakeHistory.Sysvar
// before the first block is processed (which is when sysvars are normally loaded).
func ensureStakeHistorySysvarCached(acctsDb *accountsdb.AccountsDb, slot uint64) error {
	if sealevel.SysvarCache.StakeHistory.Sysvar != nil {
		return nil // Already cached
	}

	stakeHistoryAcct, err := acctsDb.GetAccount(slot, sealevel.SysvarStakeHistoryAddr)
	if err != nil {
		return fmt.Errorf("failed to load StakeHistory sysvar from AccountsDB: %w", err)
	}

	decoder := bin.NewBinDecoder(stakeHistoryAcct.Data)
	var stakeHistory sealevel.SysvarStakeHistory
	stakeHistory.MustUnmarshalWithDecoder(decoder)

	sealevel.SysvarCache.StakeHistory.Sysvar = &stakeHistory
	sealevel.SysvarCache.StakeHistory.Acct = stakeHistoryAcct

	mlog.Log.Debugf("loaded StakeHistory sysvar from AccountsDB for resume (slot %d)", slot)
	return nil
}

// configureInitialBlockFromResume configures the first block when resuming from a previous run.
// Unlike configureInitialBlock which uses snapshot manifest data, this uses the ResumeState
// which contains the actual parent slot/bankhash from the last replayed slot.
func configureInitialBlockFromResume(acctsDb *accountsdb.AccountsDb,
	block *b.Block,
	resumeState *ResumeState,
	mithrilState *state.MithrilState,
	epochSchedule *sealevel.SysvarEpochSchedule) error {

	// Use resume state for parent info (the actual last replayed slot)
	copy(block.ParentBankhash[:], resumeState.ParentBankhash)
	block.ParentSlot = resumeState.ParentSlot
	block.AcctsLtHash = resumeState.AcctsLtHash

	// Reconstruct PrevFeeRateGovernor from state file static fields + resume dynamic fields
	prevFeeRateGovernor := reconstructFeeRateGovernor(mithrilState)
	if prevFeeRateGovernor == nil {
		return fmt.Errorf("cannot resume: state file missing manifest_fee_rate_governor (rebuild AccountsDB required)")
	}
	prevFeeRateGovernor.LamportsPerSignature = resumeState.LamportsPerSignature
	prevFeeRateGovernor.PrevLamportsPerSignature = resumeState.PrevLamportsPerSignature
	block.PrevFeeRateGovernor = prevFeeRateGovernor
	block.PrevNumSignatures = resumeState.NumSignatures

	// Load vote accounts and populate global caches - same as fresh start
	// This seeds both block.VoteAccts/VoteTimestamps AND global.VoteCache() from AccountsDB
	// Required because getTimestampEstimate reads from global.VoteCache()
	setupInitialVoteAcctsAndStakeAccts(acctsDb, block)
	configureGlobalCtx(block)

	// On resume, epoch stakes will be loaded from the persisted state file (not manifest or AccountsDB).
	// The actual loading happens in ReplayBlocks after this function returns.
	// We just need to ensure StakeHistory sysvar is cached for potential epoch boundary handling.
	if err := ensureStakeHistorySysvarCached(acctsDb, block.Slot); err != nil {
		return fmt.Errorf("failed to ensure stake history sysvar cached: %w", err)
	}

	// Handle leader schedule
	if global.ManageLeaderSchedule() {
		_, err := PrepareLeaderScheduleLocal(block.Epoch, epochSchedule, "")
		if err != nil {
			panic(err)
		}
		var exists bool
		block.Leader, exists = global.LeaderForSlot(block.Slot)
		if !exists {
			return fmt.Errorf("unable to find leader for slot %d", block.Slot)
		}
	}

	setBlockHeight(block)

	// Restore blockhash context from ResumeState (required because appendvec writes are not fsynced)
	if resumeState.RecentBlockhashes != nil && len(*resumeState.RecentBlockhashes) > 0 {
		// Validate that EvictedBlockhash and LastBlockhash are also present
		// (they could be zero if decode failed in node.go)
		var zeroHash [32]byte
		if resumeState.EvictedBlockhash == zeroHash {
			mlog.Log.Errorf("FATAL: blockhash context has RecentBlockhashes but EvictedBlockhash is zero")
			mlog.Log.Errorf("State file may be corrupted. Delete AccountsDB directory and restart from snapshot.")
			panic("cannot resume with zero EvictedBlockhash")
		}
		if resumeState.LastBlockhash == zeroHash {
			mlog.Log.Errorf("FATAL: blockhash context has RecentBlockhashes but LastBlockhash is zero")
			mlog.Log.Errorf("State file may be corrupted. Delete AccountsDB directory and restart from snapshot.")
			panic("cannot resume with zero LastBlockhash")
		}

		// Restore SysvarCache.RecentBlockHashes from state file
		sealevel.SysvarCache.RecentBlockHashes.Sysvar = resumeState.RecentBlockhashes
		block.LatestEvictedBlockhash = resumeState.EvictedBlockhash
		block.LastBlockhash = resumeState.LastBlockhash

		// Restore SysvarCache.SlotHashes from state file (vote program needs accurate slot→hash mappings)
		if resumeState.SlotHashes != nil {
			sealevel.SysvarCache.SlotHashes.Sysvar = resumeState.SlotHashes
		}
	} else {
		// No blockhash context in state file - this should not happen with new state files,
		// but could happen with old state files created before blockhash tracking was added.
		// We cannot safely resume without blockhash context because:
		// 1. block.LastBlockhash is needed for durable nonce validation
		// 2. LatestEvictedBlockhash is needed for transaction age validation edge cases
		// 3. RecentBlockhashes sysvar data in AccountsDB may be stale
		mlog.Log.Errorf("FATAL: no blockhash context in state file - cannot safely resume")
		mlog.Log.Errorf("This state file was created before blockhash tracking was added.")
		mlog.Log.Errorf("Please delete the AccountsDB directory and restart from snapshot.")
		panic("cannot resume without blockhash context in state file")
	}

	// Now that block.LastBlockhash is set, update global context
	global.SetLatestBlockHash(block.LastBlockhash)
	return nil
}

func configureGlobalCtx(block *b.Block) {
	global.SetSlot(block.Slot)
	global.SetEpoch(block.Epoch)
	global.SetLatestBlockHash(block.LastBlockhash)
	if block.HasAlpenglowBlockID {
		global.SetAlpenglowBlockID(block.Slot, solana.Hash(block.AlpenglowBlockID))
	}
	if block.HasAlpenglowLastChainedRoot {
		global.SetAlpenglowChainedMerkleRoot(block.Slot, solana.Hash(block.AlpenglowLastChainedRoot))
	}
}

func setBlockHeight(block *b.Block) {
	block.BlockHeight = global.BlockHeight() + 1
}

func initializeBlockHeight(rpcc *rpcclient.RpcClient, mithrilState *state.MithrilState, resumeState *ResumeState) error {
	switch {
	case resumeState != nil:
		if resumeState.ParentSlot > 0 && resumeState.ParentBlockHeight == 0 {
			mlog.Log.Warnf("resume state missing last block height for slot %d; fetching from RPC", resumeState.ParentSlot)
			blockResult, err := rpcc.GetBlockConfirmed(resumeState.ParentSlot)
			if err != nil {
				return fmt.Errorf("failed to fetch block height for resume slot %d: %w", resumeState.ParentSlot, err)
			}
			if blockResult == nil || blockResult.BlockHeight == nil {
				return fmt.Errorf("RPC returned no block height for resume slot %d", resumeState.ParentSlot)
			}
			resumeState.ParentBlockHeight = *blockResult.BlockHeight
		}
		global.SetBlockHeight(resumeState.ParentBlockHeight)
	case mithrilState != nil:
		if mithrilState.ManifestParentSlot > 0 && mithrilState.ManifestBlockHeight == 0 {
			mlog.Log.Warnf("state file missing manifest block height for snapshot slot %d; fetching from RPC", mithrilState.ManifestParentSlot)
			blockResult, err := rpcc.GetBlockConfirmed(mithrilState.ManifestParentSlot)
			if err != nil {
				return fmt.Errorf("failed to fetch block height for snapshot slot %d: %w", mithrilState.ManifestParentSlot, err)
			}
			if blockResult == nil || blockResult.BlockHeight == nil {
				return fmt.Errorf("RPC returned no block height for snapshot slot %d", mithrilState.ManifestParentSlot)
			}
			mithrilState.ManifestBlockHeight = *blockResult.BlockHeight
		}
		global.SetBlockHeight(mithrilState.ManifestBlockHeight)
	}

	return nil
}

// buildInitialEpochStakesCache seeds the epoch stakes cache from state file manifest data.
func buildInitialEpochStakesCache(mithrilState *state.MithrilState, currentEpoch uint64, snapshotEpoch uint64) error {
	seeds, rebased, err := prepareManifestEpochStakesForRuntime(mithrilState, currentEpoch, snapshotEpoch)
	if err != nil {
		return err
	}
	if rebased {
		sourceEpochs := make([]uint64, 0, len(seeds))
		runtimeEpochs := make([]uint64, 0, len(seeds))
		for _, seed := range seeds {
			sourceEpochs = append(sourceEpochs, seed.sourceEpoch)
			runtimeEpochs = append(runtimeEpochs, seed.runtimeEpoch)
		}
		mlog.Log.Warnf("rebasing manifest epoch stakes from snapshot epochs %v to runtime epochs %v (snapshot epoch %d)",
			sourceEpochs, runtimeEpochs, snapshotEpoch)
	}

	for _, seed := range seeds {
		if loadedEpoch, err := global.DeserializeAndLoadEpochStakes(seed.data); err != nil {
			return fmt.Errorf("failed to load manifest epoch %d stakes from state file: %w", seed.sourceEpoch, err)
		} else {
			mlog.Log.Debugf("loaded manifest epoch %d stakes as runtime epoch %d from state file manifest_epoch_stakes",
				seed.sourceEpoch, loadedEpoch)
		}
	}

	// Load EpochAuthorizedVoters from state file (required)
	// Supports multiple authorized voters per vote account (matches original manifest behavior)
	if len(mithrilState.ManifestEpochAuthorizedVoters) == 0 {
		return fmt.Errorf("state file missing manifest_epoch_authorized_voters - delete AccountsDB and rebuild from snapshot")
	}
	for voteAcctStr, authorizedVoterStrs := range mithrilState.ManifestEpochAuthorizedVoters {
		voteAcct, err := base58.DecodeFromString(voteAcctStr)
		if err != nil {
			return fmt.Errorf("corrupted state file: failed to decode epoch_authorized_voters key %s: %w", voteAcctStr, err)
		}
		for _, authorizedVoterStr := range authorizedVoterStrs {
			authorizedVoter, err := base58.DecodeFromString(authorizedVoterStr)
			if err != nil {
				return fmt.Errorf("corrupted state file: failed to decode epoch_authorized_voters value %s: %w", authorizedVoterStr, err)
			}
			global.PutEpochAuthorizedVoter(voteAcct, authorizedVoter)
		}
	}

	return nil
}

type persistedTracker struct {
	mu       sync.Mutex
	slot     uint64
	bankhash []byte
}

func (t *persistedTracker) Set(slot uint64, hash []byte) {
	t.mu.Lock()
	t.slot = slot
	t.bankhash = make([]byte, len(hash))
	copy(t.bankhash, hash)
	t.mu.Unlock()
}

func (t *persistedTracker) Get() (uint64, []byte) {
	t.mu.Lock()
	slot := t.slot
	out := make([]byte, len(t.bankhash))
	copy(out, t.bankhash)
	t.mu.Unlock()
	return slot, out
}

func ReplayBlocks(
	ctx context.Context,
	acctsDb *accountsdb.AccountsDb,
	acctsDbPath string,
	mithrilState *state.MithrilState, // State file with manifest_* seed fields
	resumeState *ResumeState, // nil if not resuming, contains parent slot info when resuming
	startSlot, endSlot uint64,
	rpcEndpoints []string, // RPC endpoints in priority order (first = primary, rest = fallbacks)
	lightbringerEndpoint string,
	turbineBindAddr string,
	turbineGossipEntrypoint string,
	turbineGossipBindAddr string,
	turbineAdvertisedIP string,
	turbineShredVersion uint16,
	blockDir string,
	txParallelism int,
	isLive bool,
	useLightbringer bool,
	useTurbine bool,
	dbgOpts *DebugOptions,
	metricsWriter io.Writer,
	rpcServer SlotCtxSetter,
	blockFetchOpts *BlockFetchOpts,
	consensusOpts *ConsensusOpts, // nil = use defaults (max_depth=64, policy="halt")
	onCancelWriteState OnCancelWriteState, // callback to write state immediately on cancellation (can be nil)
) *ReplayResult {
	result := &ReplayResult{}

	// Generate unique run ID for log correlation (only if not already set by startup)
	if CurrentRunID == "" {
		CurrentRunID = GenerateRunID()
	}
	// Create bankhash log file
	bankhashLogPath := fmt.Sprintf("%s/bankhash.log", acctsDbPath)
	bankhashLogFile, bankhashLogErr := os.OpenFile(bankhashLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if bankhashLogErr != nil {
		mlog.Log.Errorf("failed to open bankhash log file: %v", bankhashLogErr)
	} else {
		defer bankhashLogFile.Close()
		// Write run header for correlation
		fmt.Fprintf(bankhashLogFile, "# run:%s started:%s slots:%d-%d\n",
			CurrentRunID, time.Now().UTC().Format(time.RFC3339), startSlot, endSlot)
	}

	// Track last successfully persisted slot for checkpoint/resume
	pt := &persistedTracker{}

	// RPC client - for all cluster access (blocks, leader schedule, tip polling)
	// First endpoint is primary, rest are backups for failover
	rpcc := rpcclient.NewRpcClient(rpcEndpoints[0])
	var rpcBackups []string
	if len(rpcEndpoints) > 1 {
		rpcBackups = rpcEndpoints[1:]
	}

	cacheConstantSysvars(acctsDb)
	epochSchedule, usingManifestEpochSchedule, err := bankEpochScheduleForReplay(mithrilState)
	if err != nil {
		result.Error = err
		return result
	}
	if usingManifestEpochSchedule && !epochSchedulesEqual(epochSchedule, sealevel.SysvarCache.EpochSchedule.Sysvar) {
		sysvarSchedule := sealevel.SysvarCache.EpochSchedule.Sysvar
		mlog.Log.Warnf("bank epoch schedule differs from SysvarEpochSchedule account; using manifest bank schedule for replay | bank_slots_per_epoch=%d bank_leader_offset=%d bank_first_normal_epoch=%d bank_first_normal_slot=%d | sysvar_slots_per_epoch=%d sysvar_leader_offset=%d sysvar_first_normal_epoch=%d sysvar_first_normal_slot=%d",
			epochSchedule.SlotsPerEpoch, epochSchedule.LeaderScheduleSlotOffset, epochSchedule.FirstNormalEpoch, epochSchedule.FirstNormalSlot,
			sysvarSchedule.SlotsPerEpoch, sysvarSchedule.LeaderScheduleSlotOffset, sysvarSchedule.FirstNormalEpoch, sysvarSchedule.FirstNormalSlot)
	}

	global.SetCalcUnixTimeForClockSysvar(true)
	global.SetManageLeaderSchedule(true)

	var currentSlot uint64
	currentEpoch := epochSchedule.GetEpoch(startSlot)
	var lastSlotCtx *sealevel.SlotCtx
	var partitionedEpochRewardsEnabled bool
	var partitionedRewardsInfo *rewards.PartitionedRewardDistributionInfo
	var featuresActivatedInFirstSlot []*accounts.Account
	var parentFeaturesActivatedInFirstSlot []*accounts.Account

	// Pass mithrilState + resumeState so ReplayCtx uses state file for seed data
	replayCtx, err := newReplayCtx(mithrilState, resumeState)
	if err != nil {
		result.Error = err
		return result
	}

	// Replays may restart in-process after a fork switch, so assign rather than
	// increment. A rooted checkpoint carries the exact count for its branch.
	if resumeState != nil && resumeState.TransactionCount != nil {
		global.SetTransactionCount(*resumeState.TransactionCount)
	} else {
		global.SetTransactionCount(mithrilState.ManifestTransactionCount)
	}
	isFirstSlotInEpoch := epochSchedule.FirstSlotInEpoch(currentEpoch) == startSlot
	alpenglowReplayMode := isAlpenglowReplayMode(consensusOpts) || useTurbine
	replayCtx.CurrentFeatures, featuresActivatedInFirstSlot, parentFeaturesActivatedInFirstSlot = scanAndEnableFeatures(acctsDb, replayCtx, startSlot, isFirstSlotInEpoch)
	if alpenglowReplayMode {
		applyAlpenglowRuntimeFeatureOverrides(replayCtx.CurrentFeatures, startSlot)
	}
	alpenglowClock := useAlpenglowClockSemantics(alpenglowReplayMode, replayCtx.CurrentFeatures)
	if alpenglowReplayMode && !alpenglowClockFeatureActive(replayCtx.CurrentFeatures) {
		mlog.Log.Infof("Alpenglow mode: Alpenglow feature gate is not active; forcing Alpenglow bank clock semantics")
	}
	partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
	if partitionedEpochRewardsEnabled &&
		sealevel.SysvarCache.EpochRewards.Sysvar != nil &&
		sealevel.SysvarCache.EpochRewards.Sysvar.Active {
		parentSlot := mithrilState.ManifestParentSlot
		if resumeState != nil {
			parentSlot = resumeState.ParentSlot
		}
		result.Error = fmt.Errorf("startup state at slot %d is inside an active partitioned-rewards distribution; resume from the durable boundary before this epoch transition", parentSlot)
		return result
	}

	var chainTipAcctsLtHash *lthash.LtHash
	if resumeState != nil && resumeState.AcctsLtHash != nil {
		chainTipAcctsLtHash = resumeState.AcctsLtHash
	} else if mithrilState != nil && mithrilState.ManifestAcctsLtHash != "" {
		if ltHashBytes, err := base64.StdEncoding.DecodeString(mithrilState.ManifestAcctsLtHash); err == nil && len(ltHashBytes) == lthash.HashByteLen {
			chainTipAcctsLtHash = new(lthash.LtHash).InitWithHash(ltHashBytes)
		}
	}
	var chainTipPrevNumSigs uint64
	if resumeState != nil {
		chainTipPrevNumSigs = resumeState.NumSignatures
	} else if mithrilState != nil {
		chainTipPrevNumSigs = mithrilState.ManifestSignatureCount
	}
	var chainTipLastEntryHash solana.Hash
	if resumeState != nil && resumeState.LastBlockhash != ([32]byte{}) {
		chainTipLastEntryHash = solana.Hash(resumeState.LastBlockhash)
	} else if mithrilState != nil && mithrilState.ManifestLastBlockhash != "" {
		if hash, err := base58.DecodeFromString(mithrilState.ManifestLastBlockhash); err == nil {
			chainTipLastEntryHash = solana.Hash(hash)
		}
	}
	InitChainTip(chainTipAcctsLtHash, replayCtx.CurrentFeatures, chainTipPrevNumSigs, chainTipLastEntryHash)

	// Load epoch stakes - persisted stakes on resume, state file on fresh start
	snapshotEpoch := epochSchedule.GetEpoch(mithrilState.ManifestParentSlot)
	if resumeState != nil {
		// Resume case - check if we've crossed epoch boundaries since snapshot
		epochsCrossed := currentEpoch > snapshotEpoch
		if epochsCrossed && len(resumeState.ComputedEpochStakes) == 0 {
			// Crossed epoch boundary but no persisted stakes - data loss (crash before persist)
			mlog.Log.Errorf("Resume at epoch %d (snapshot epoch %d) but no persisted epoch stakes found - cannot use stale manifest stakes (need fresh snapshot)", currentEpoch, snapshotEpoch)
			result.Error = fmt.Errorf("resume at epoch %d (snapshot epoch %d) but no persisted epoch stakes found - cannot use stale manifest stakes (need fresh snapshot)", currentEpoch, snapshotEpoch)
			return result
		}
		if len(resumeState.ComputedEpochStakes) > 0 {
			// Load ONLY persisted epoch stakes from state file (NO manifest fallback)
			// This ensures we use the exact stakes computed at prior epoch boundaries
			for epoch, data := range resumeState.ComputedEpochStakes {
				if loadedEpoch, err := global.DeserializeAndLoadEpochStakes(data); err != nil {
					mlog.Log.Errorf("Failed to load persisted epoch %d stakes: %v", epoch, err)
					result.Error = fmt.Errorf("failed to load persisted epoch %d stakes: %w", epoch, err)
					return result
				} else {
					mlog.Log.Debugf("Loaded persisted epoch stakes for epoch %d from state file", loadedEpoch)
				}
			}
			// Validate current epoch stakes exist (we build schedules for block.Epoch)
			// Note: Don't validate leaderScheduleEpoch here - it can point to E+1 in second
			// half of epoch E with non-standard slot offsets, causing false failures
			if !global.HasEpochStakes(currentEpoch) {
				mlog.Log.Errorf("Missing required epoch stakes for current epoch %d - cannot resume (need fresh snapshot)", currentEpoch)
				result.Error = fmt.Errorf("missing required epoch stakes for current epoch %d - cannot resume (need fresh snapshot)", currentEpoch)
				return result
			}
			// Load EpochAuthorizedVoters from state file (required for forkchoice vote parsing).
			// buildInitialEpochStakesCache loads these, but this path skips that function.
			if len(mithrilState.ManifestEpochAuthorizedVoters) > 0 {
				for voteAcctStr, authorizedVoterStrs := range mithrilState.ManifestEpochAuthorizedVoters {
					voteAcct, vErr := base58.DecodeFromString(voteAcctStr)
					if vErr != nil {
						result.Error = fmt.Errorf("corrupted state file: failed to decode epoch_authorized_voters key %s: %w", voteAcctStr, vErr)
						return result
					}
					for _, authorizedVoterStr := range authorizedVoterStrs {
						authorizedVoter, vErr := base58.DecodeFromString(authorizedVoterStr)
						if vErr != nil {
							result.Error = fmt.Errorf("corrupted state file: failed to decode epoch_authorized_voters value %s: %w", authorizedVoterStr, vErr)
							return result
						}
						global.PutEpochAuthorizedVoter(voteAcct, authorizedVoter)
					}
				}
			}
		} else {
			// Resume in same epoch as snapshot, no boundaries crossed - state file epoch stakes still valid
			if err := buildInitialEpochStakesCache(mithrilState, currentEpoch, snapshotEpoch); err != nil {
				result.Error = err
				return result
			}
		}
	} else {
		// Fresh start: load all epochs from state file
		if err := buildInitialEpochStakesCache(mithrilState, currentEpoch, snapshotEpoch); err != nil {
			result.Error = err
			return result
		}
	}
	if err := initializeBlockHeight(rpcc, mithrilState, resumeState); err != nil {
		result.Error = err
		return result
	}
	// Resolve consensus config defaults before forkchoice init so we can
	// check whether enforcement requires authorized voters.
	useLiveShredStream := useLightbringer || useTurbine
	consensusCfg := resolveConsensusConfig(consensusOpts, useLightbringer, useTurbine, isLive)
	var consensusEngine consensusengine.Engine
	if consensusOpts != nil {
		consensusEngine = consensusOpts.Engine
	}
	if sink, ok := consensusEngine.(consensusengine.AlpenglowEpochLookupSink); ok {
		schedule := epochSchedule
		sink.SetAlpenglowEpochLookup(func(slot uint64) uint64 {
			return schedule.GetEpoch(slot)
		})
	}
	consensusManagedLiveStream := consensusCfg.enforceActive &&
		isLive &&
		consensusManagesLiveShredStream(consensusCfg.enforceSource, useLightbringer, useTurbine)
	consensusLiveStreamName := "Lightbringer"
	if useTurbine {
		consensusLiveStreamName = "TURBINE"
	}

	epochAuthVoters := global.EpochAuthorizedVoters()
	if epochAuthVoters == nil {
		// Without authorized voters, forkchoice can't parse votes → no supermajority → enforcement is blind.
		// If consensus enforcement is active, this is a fatal misconfiguration.
		if consensusCfg.enforceActive && consensusCfg.policy == "halt" {
			result.Error = fmt.Errorf("forkchoice: EpochAuthorizedVoters is nil — cannot enforce consensus without vote parsing (check snapshot/state file)")
			return result
		}
		mlog.Log.Warnf("forkchoice: EpochAuthorizedVoters is nil — vote parsing will be skipped until populated")
	}
	forkChoice := forkchoice.NewForkChoiceService(currentEpoch, global.EpochStakes(currentEpoch), global.EpochTotalStake(currentEpoch), epochAuthVoters)
	forkChoice.Start()
	defer forkChoice.Stop()
	global.SetForkChoice(forkChoice)

	if consensusOpts != nil && consensusOpts.Engine != nil {
		consensusengine.RefreshAlpenglowValidatorSetsFromCache(consensusOpts.Engine)
	}

	// Instantiate the consensus coordinator for skip-path resolution and policy.
	// In Lightbringer mode this now resolves a pre-execution block/skip path from
	// the current anchor to a vote-confirmed leaf.
	consensusCoordinator := forkchoice.NewConsensusCoordinator(forkChoice, consensusCfg.maxDepth, consensusCfg.policy)
	consensusBufferedExecutionActive := consensusCfg.bufferedExecutionActive

	var statsCounter int
	var execTimes []float64      // seconds per block
	var waitTimes []float64      // seconds per block
	var cuValues []uint64        // CU per block
	var voteTxCounts []uint64    // vote txns per block
	var nonVoteTxCounts []uint64 // non-vote txns per block
	var justCrossedEpochBoundary bool

	// Preallocate slices for 100 blocks
	const summaryInterval = 100
	execTimes = make([]float64, 0, summaryInterval)
	waitTimes = make([]float64, 0, summaryInterval)
	cuValues = make([]uint64, 0, summaryInterval)
	voteTxCounts = make([]uint64, 0, summaryInterval)
	nonVoteTxCounts = make([]uint64, 0, summaryInterval)

	var readyConsensusPath *pendingConsensusPath
	observedConsensusBlocks := make(map[uint64]*b.Block)

	var opts *blockstream.BlockSourceOpts
	if useLiveShredStream {
		if _, err := PrepareLeaderScheduleLocal(currentEpoch, epochSchedule, ""); err != nil {
			result.Error = fmt.Errorf("bootstrap leader schedule for live shred stream at start_slot %d epoch %d: %w", startSlot, currentEpoch, err)
			return result
		}
		if _, ok := global.LeaderForSlot(startSlot); !ok {
			result.Error = fmt.Errorf("bootstrap leader schedule for epoch %d does not cover start_slot %d", currentEpoch, startSlot)
			return result
		}
		mlog.Log.Infof("bootstrap leader schedule ready for epoch %d at start_slot %d", currentEpoch, startSlot)
	}
	if useLightbringer {
		opts = &blockstream.BlockSourceOpts{
			SourceType:                   blockstream.BlockSourceLightbringer,
			RpcClient:                    rpcc,
			LightbringerEndpoint:         lightbringerEndpoint,
			BackupRpcEndpoints:           rpcBackups,
			StartSlot:                    startSlot,
			EndSlot:                      endSlot,
			BlockDir:                     blockDir,
			ConsensusManagedLightbringer: consensusManagedLiveStream,
		}
	} else if useTurbine {
		opts = &blockstream.BlockSourceOpts{
			SourceType:                   blockstream.BlockSourceTurbine,
			RpcClient:                    rpcc,
			TurbineBindAddr:              turbineBindAddr,
			TurbineGossipEntrypoint:      turbineGossipEntrypoint,
			TurbineGossipBindAddr:        turbineGossipBindAddr,
			TurbineAdvertisedIP:          turbineAdvertisedIP,
			TurbineShredVersion:          turbineShredVersion,
			LeaderForSlot:                global.LeaderForSlot,
			BackupRpcEndpoints:           rpcBackups,
			StartSlot:                    startSlot,
			EndSlot:                      endSlot,
			BlockDir:                     blockDir,
			ConsensusManagedLightbringer: consensusManagedLiveStream,
		}
	} else {
		opts = &blockstream.BlockSourceOpts{
			SourceType:         blockstream.BlockSourceRpc,
			RpcClient:          rpcc,
			BackupRpcEndpoints: rpcBackups,
			StartSlot:          startSlot,
			EndSlot:            endSlot,
			BlockDir:           blockDir,
		}
	}

	// Apply block fetching options if provided
	if blockFetchOpts != nil {
		opts.MaxRPS = blockFetchOpts.MaxRPS
		opts.MaxInflight = blockFetchOpts.MaxInflight
		opts.TipPollMs = blockFetchOpts.TipPollMs
		opts.TipSafetyMargin = blockFetchOpts.TipSafetyMargin

		// Mode thresholds
		opts.NearTipThreshold = blockFetchOpts.NearTipThreshold
		opts.CatchupThreshold = blockFetchOpts.CatchupThreshold
		opts.CatchupTipGateThreshold = blockFetchOpts.CatchupTipGateThreshold

		// Near-tip tuning
		opts.NearTipPollMs = blockFetchOpts.NearTipPollMs
		opts.NearTipLookahead = blockFetchOpts.NearTipLookahead
		opts.GossipClient = blockFetchOpts.GossipClient
		if len(blockFetchOpts.GossipIdentity) > 0 {
			opts.GossipIdentity = blockFetchOpts.GossipIdentity
		}
		opts.HasLocalLeaderCommit = blockFetchOpts.HasLocalLeaderCommit
		opts.TurbineAlpenglowBindAddr = blockFetchOpts.TurbineAlpenglowBindAddr
		if blockFetchOpts.GossipClient == nil {
			opts.TPUQUICAdvertise = blockFetchOpts.TPUQUICAdvertise
		}
	}
	turbAlpenglowBind := ""
	if blockFetchOpts != nil {
		turbAlpenglowBind = blockFetchOpts.TurbineAlpenglowBindAddr
	}
	if blockFetchOpts != nil && useTurbine && blockFetchOpts.TurbineRepairOnly {
		opts.TurbineRepairOnly = true
	}
	if useTurbine && turbAlpenglowBind != "" {
		opts.TurbineAlpenglowBlockIDHints = true
		if decisionSource, ok := consensusEngine.(consensusengine.AlpenglowDecisionSource); ok {
			opts.AlpenglowDecisionSource = decisionSource.NextAlpenglowDecision
		}
		if candidateObserver, ok := consensusEngine.(consensusengine.AlpenglowCandidateBlockObserver); ok {
			opts.AlpenglowCandidateBlockSink = candidateObserver.ObserveAlpenglowCandidateBlock
		}
	}
	blockStream := blockstream.NewBlockSource(opts)
	SetLocalLeaderCommitNotifier(blockStream.NotifyLocalLeaderCommit)
	defer ClearLocalLeaderCommitNotifier()

	captureReplayAux := func() ReplayAuxState {
		aux := ReplayAuxState{
			PartitionedEpochRewardsEnabled: partitionedEpochRewardsEnabled,
		}
		if partitionedRewardsInfo != nil {
			info := *partitionedRewardsInfo
			aux.PartitionedRewardsInfo = &info
		}
		return aux
	}
	restoreReplayAux := func(snapshot *ReplayHeadSnapshot) error {
		if snapshot == nil {
			return fmt.Errorf("nil replay head snapshot")
		}
		previousEpoch := currentEpoch
		currentEpoch = snapshot.Epoch
		partitionedEpochRewardsEnabled = snapshot.Aux.PartitionedEpochRewardsEnabled
		partitionedRewardsInfo = nil
		if snapshot.Aux.PartitionedRewardsInfo != nil {
			info := *snapshot.Aux.PartitionedRewardsInfo
			partitionedRewardsInfo = &info
		}
		justCrossedEpochBoundary = false
		alpenglowClock = useAlpenglowClockSemantics(alpenglowReplayMode, replayCtx.CurrentFeatures)

		if previousEpoch != currentEpoch {
			// A losing epoch-boundary branch may have populated the next consensus
			// snapshot and the following epoch's rank/leader tables. Keep the valid
			// one-epoch lookahead and discard everything derived beyond it.
			global.DropEpochVoteStateSnapshotsAfter(currentEpoch)
			maxStakeEpoch := currentEpoch
			if currentEpoch != ^uint64(0) {
				maxStakeEpoch++
			}
			global.ClearEpochStakesAfter(maxStakeEpoch)
		}
		stakes := global.EpochStakes(currentEpoch)
		if len(stakes) == 0 {
			return fmt.Errorf("no epoch stakes available for rollback epoch %d", currentEpoch)
		}
		global.ResetVoteCache()
		if err := RebuildVoteCacheFromAccountsDB(acctsDb, snapshot.Slot, stakes, 0); err != nil {
			return fmt.Errorf("rebuild vote cache for rollback epoch %d: %w", currentEpoch, err)
		}
		if snapshot.EpochAuthorizedVoters == nil {
			rebuildAuthorizedVotersFromVoteCache(currentEpoch)
		}
		if previousEpoch == currentEpoch {
			return nil
		}
		if global.ManageLeaderSchedule() {
			if _, err := PrepareLeaderScheduleLocal(currentEpoch, epochSchedule, ""); err != nil {
				return fmt.Errorf("restore leader schedule for epoch %d: %w", currentEpoch, err)
			}
		}
		forkChoice.UpdateEpoch(
			currentEpoch,
			global.EpochStakes(currentEpoch),
			global.EpochTotalStake(currentEpoch),
			global.EpochAuthorizedVoters(),
		)
		if consensusEngine != nil {
			if err := consensusengine.RefreshAlpenglowValidatorSet(consensusEngine, currentEpoch); err != nil {
				return fmt.Errorf("restore Alpenglow validator set for epoch %d: %w", currentEpoch, err)
			}
		}
		return nil
	}
	rollbackParams := func() SpeculativeRollbackParams {
		return SpeculativeRollbackParams{
			AcctsDb:     acctsDb,
			PT:          pt,
			ReplayCtx:   replayCtx,
			LastSlotCtx: &lastSlotCtx,
			BlockStream: blockStream,
			ForkChoice:  forkChoice,
			RPCServer:   rpcServer,
			RestoreAux:  restoreReplayAux,
		}
	}

	speculative := NewSpeculativeReplay()
	var alpenglowNextDecision func(anchorSlot uint64) (alpenglow.ChainDecision, bool)
	speculativeEnabled := useTurbine && turbAlpenglowBind != ""
	if speculativeEnabled {
		if decisionSource, ok := consensusEngine.(consensusengine.AlpenglowDecisionSource); ok {
			alpenglowNextDecision = decisionSource.NextAlpenglowDecision
			speculative.Enable()
			acctsDb.RootedDurable = true
			speculative.ConfigureDurable(acctsDb, mithrilState, filepath.Join(acctsDb.AcctsDir, ".."), FoldBatchSlots)
			defer speculative.Close()
			if rootSink, ok := consensusEngine.(consensusengine.AlpenglowRootSink); ok {
				speculative.SetRootSink(rootSink.SetAlpenglowRoot)
			}
			if durableRootSink, ok := consensusEngine.(consensusengine.AlpenglowDurableRootSink); ok {
				speculative.SetDurableRootSink(durableRootSink.SetAlpenglowDurableRoot)
			}
			stopActiveSpeculative := publishActiveSpeculativeReplay(speculative)
			defer stopActiveSpeculative()
			blockStream.SetParentMismatchHandler(func(waitingSlot, observedParent, expectedParent uint64) bool {
				return speculative.HandleParentMismatch(waitingSlot, observedParent, expectedParent, rollbackParams())
			})
			blockStream.SetParentIdentityMismatchHandler(func(
				waitingSlot, parentSlot uint64,
				observedParentID, executedParentID solana.Hash,
			) bool {
				return speculative.HandleParentIdentityMismatch(
					waitingSlot, parentSlot, observedParentID, executedParentID,
					rollbackParams(),
				)
			})
			mlog.Log.Infof("speculative replay enabled: deferring AccountsDB persistence until Alpenglow finalizes turbine blocks")
			if resumeState == nil {
				speculative.SeedFromManifest(mithrilState, replayCtx)
			} else {
				speculative.SeedFromResume(resumeState, replayCtx)
			}
			if durableSlot, durableBankhash := speculative.DurableHead(); durableSlot > 0 {
				pt.Set(durableSlot, durableBankhash)
			}
		} else {
			speculativeEnabled = false
		}
	}
	if publisher, ok := consensusEngine.(consensusengine.AlpenglowBlockIDPublisher); ok && useTurbine && turbAlpenglowBind != "" {
		publisher.SetAlpenglowBlockIDSink(blockStream.SetKnownAlpenglowBlockID)
	}
	if repairPublisher, ok := consensusEngine.(consensusengine.AlpenglowRepairPriorityPublisher); ok && useTurbine && turbAlpenglowBind != "" {
		repairPublisher.SetAlpenglowRepairPrioritySink(blockStream.PrioritizeTurbineRepairSlot)
	}

	if !isLive {
		blockStream.DownloadInitialBlocks()
	}
	go blockStream.Start()

	var skippedSlotsCount int // Track skipped slots for 100-slot summary
	replayStartLogged := false

	currentConsensusAnchorSlot := func() uint64 {
		if lastSlotCtx != nil {
			return lastSlotCtx.Slot
		}
		if resumeState != nil {
			return resumeState.ParentSlot
		}
		return mithrilState.ManifestParentSlot
	}

	observeConsensusAnchor := func() {
		if lastSlotCtx != nil {
			forkChoice.ObserveExecutionAnchor(lastSlotCtx.Slot, solana.Hash(lastSlotCtx.Blockhash))
			return
		}
		if resumeState != nil && resumeState.LastBlockhash != ([32]byte{}) {
			forkChoice.ObserveExecutionAnchor(resumeState.ParentSlot, solana.Hash(resumeState.LastBlockhash))
			return
		}
		if mithrilState != nil && mithrilState.ManifestLastBlockhash != "" {
			manifestLastBlockhash, err := base58.DecodeFromString(mithrilState.ManifestLastBlockhash)
			if err != nil {
				mlog.Log.Warnf("forkchoice: failed to decode manifest PoH blockhash for anchor seeding: %v", err)
				return
			}
			forkChoice.ObserveExecutionAnchor(mithrilState.ManifestParentSlot, solana.Hash(manifestLastBlockhash))
		}
	}

	syncConsensusBufferedExecutionMode := func(triggerSlot uint64) {
		if !consensusManagedLiveStream {
			return
		}

		stats := blockStream.GetFetchStats()
		if consensusBufferedExecutionActive && !stats.IsNearTip {
			anchorSlot := currentConsensusAnchorSlot()
			discardedObservedBlocks := len(observedConsensusBlocks)
			readyDecisionCount := 0
			if readyConsensusPath != nil {
				readyDecisionCount = len(readyConsensusPath.decisions)
			}

			consensusBufferedExecutionActive = false
			readyConsensusPath = nil
			clearObservedConsensusBlocks(observedConsensusBlocks)
			observeConsensusAnchor()
			mlog.Log.Warnf("forkchoice: suspending buffered execution at slot %d because block source left near-tip mode; RPC catchup will continue from anchor %d (discarded_observed_blocks=%d discarded_ready_decisions=%d next_emitted_slot=%d)",
				triggerSlot, anchorSlot, discardedObservedBlocks, readyDecisionCount, stats.NextSlot)
		}
	}

	observeBlockForConsensus := func(block *b.Block) error {
		if !consensusCfg.enforceActive {
			return nil
		}

		if !consensusBufferedExecutionActive && consensusManagedLiveStream {
			if block == nil || !block.FromLightbringer {
				return nil
			}
			consensusBufferedExecutionActive = true
			readyConsensusPath = nil
			observeConsensusAnchor()
			pruneObservedConsensusBlocks(observedConsensusBlocks, currentConsensusAnchorSlot())
			mlog.Log.Warnf("forkchoice: enabling buffered execution at slot %d after block source switched to %s", block.Slot, consensusLiveStreamName)
		}

		if !consensusBufferedExecutionActive {
			return nil
		}

		if block.IsSkipped {
			forkChoice.ObserveSkippedSlot(block.Slot)
			return nil
		}

		meta := forkchoice.ObservedBlockMeta{
			Slot:            block.Slot,
			Blockhash:       solana.Hash(block.Blockhash),
			ParentSlot:      block.SourceParentSlot,
			ParentSlotKnown: block.FromLightbringer && block.SourceParentSlot != 0,
			ParentBlockhash: solana.Hash(block.LastBlockhash),
		}

		if err := forkChoice.ObserveBlock(meta, block.Transactions); err != nil {
			return err
		}

		if consensusBufferedExecutionActive {
			observedConsensusBlocks[block.Slot] = block
		}
		return nil
	}

	observeConsensusAnchor()

	for {
		if ctx.Err() != nil {
			mlog.Log.Infof("context cancelled, stopping replay: %v", ctx.Err())
			result.WasCancelled = true
			break
		}

		syncConsensusBufferedExecutionMode(currentConsensusAnchorSlot())

		var (
			block    *b.Block
			waitTime time.Duration
		)

		if consensusBufferedExecutionActive && readyConsensusPath != nil && len(readyConsensusPath.decisions) > 0 {
			nextDecision := readyConsensusPath.decisions[0]
			readyConsensusPath.decisions = readyConsensusPath.decisions[1:]

			if nextDecision.UseBlock {
				var exists bool
				block, exists = observedConsensusBlocks[nextDecision.Slot]
				if !exists {
					result.Error = fmt.Errorf("forkchoice: missing observed block for resolved slot %d", nextDecision.Slot)
					break
				}
				delete(observedConsensusBlocks, nextDecision.Slot)
			} else {
				delete(observedConsensusBlocks, nextDecision.Slot)
				mlog.Log.Infof("forkchoice: resolved slot %d as skipped on path to confirmed leaf %d",
					nextDecision.Slot, readyConsensusPath.leafSlot)
				block = &b.Block{Slot: nextDecision.Slot, IsSkipped: true}
			}
		} else {
			// Start stall monitor goroutine (only after first block to avoid startup false positives)
			// Logs to file every second while waiting for a block
			var stallDone chan struct{}
			if len(execTimes) > 0 {
				stallDone = make(chan struct{})
				go func() {
					ticker := time.NewTicker(1 * time.Second)
					defer ticker.Stop()
					secondsWaiting := 0
					for {
						select {
						case <-stallDone:
							return
						case <-ticker.C:
							secondsWaiting++
							stats := blockStream.GetFetchStats()
							modeStr := "catchup"
							if stats.IsNearTip {
								modeStr = "near-tip"
							}
							stallMsg := fmt.Sprintf("STALL: waiting %ds for slot %d | mode: %s | tip_stale: %ds | state: %s | retries: %d | inflight: %d | retry_q: %d | tip: %d",
								secondsWaiting, stats.NextSlot, modeStr, stats.TipStaleSecs, stats.WaitingSlotState,
								stats.WaitingSlotRetries, stats.InflightCount, stats.RetryQueueLen, stats.ConfirmedTip)
							mlog.Log.FileOnlyf("%s", stallMsg)
						}
					}
				}()
			}

			waitStart := time.Now()
			block = blockStream.NextBlock()
			waitTime = time.Since(waitStart)

			if stallDone != nil {
				close(stallDone)
			}

			if block == nil {
				if result.Error == nil {
					switch {
					case blockStream.Err() != nil:
						result.Error = blockStream.Err()
					case blockStream.Stalled():
						result.Error = fmt.Errorf("block fetch stalled - no progress for %v", blockStream.StallTimeout())
					case isLive && !blockStream.Completed():
						result.Error = fmt.Errorf("block source stopped unexpectedly: %s", blockStream.StopReason())
					}
				}
				break
			}

			if anchorSlot := currentConsensusAnchorSlot(); anchorSlot != 0 && block.Slot <= anchorSlot {
				mlog.Log.Warnf("replay: discarding stale block source emission for slot %d; already executed through slot %d",
					block.Slot, anchorSlot)
				continue
			}

			if speculativeEnabled && alpenglowNextDecision != nil {
				switched, err := speculative.ReconcileCertifiedTail(alpenglowNextDecision, rollbackParams())
				if err != nil {
					result.Error = err
					break
				}
				if switched {
					continue
				}
			}

			if consensusEngine != nil {
				stats := blockStream.GetFetchStats()
				if err := consensusEngine.ObserveBlock(ctx, consensusengine.BlockObservation{
					Block:  block,
					Source: stats.CurrentSource,
					At:     time.Now(),
				}); err != nil {
					result.Error = fmt.Errorf("consensus engine observe block: %w", err)
					break
				}
			}

			if speculativeEnabled && alpenglowNextDecision != nil {
				speculative.TryFlushPending(acctsDb, pt, replayCtx, alpenglowNextDecision)
				if err := speculative.Err(); err != nil {
					result.Error = err
					break
				}
			}

			syncConsensusBufferedExecutionMode(block.Slot)

			if block.FromLightbringer {
				stats := blockStream.GetFetchStats()
				if shouldDiscardLightbringerObservationAfterFallback(isLive, useLiveShredStream, block, stats) {
					modeStr := "catchup"
					if stats.IsNearTip {
						modeStr = "near-tip"
					}
					mlog.Log.Warnf("forkchoice: discarding stale %s observation for slot %d after source fallback (mode=%s current_source=%s anchor=%d next_emitted_slot=%d)",
						consensusLiveStreamName, block.Slot, modeStr, stats.CurrentSource, currentConsensusAnchorSlot(), stats.NextSlot)
					continue
				}
			}

			if err := observeBlockForConsensus(block); err != nil {
				if errors.Is(err, forkchoice.ErrEquivocation) {
					result.Error = fmt.Errorf("forkchoice: equivocation detected at slot %d", block.Slot)
				} else {
					result.Error = err
				}
				break
			}

			if consensusBufferedExecutionActive {
				resolvedPath, err := consensusCoordinator.ResolveFromAnchor(currentConsensusAnchorSlot())
				if err != nil {
					switch {
					case errors.Is(err, forkchoice.ErrNeedWait), errors.Is(err, forkchoice.ErrPathIncomplete):
						continue
					case errors.Is(err, forkchoice.ErrDepthExceeded):
						if consensusManagedLiveStream && isLive && useLiveShredStream {
							anchorSlot := currentConsensusAnchorSlot()
							discardedObservedBlocks := len(observedConsensusBlocks)
							readyDecisionCount := 0
							if readyConsensusPath != nil {
								readyDecisionCount = len(readyConsensusPath.decisions)
							}
							mlog.Log.Warnf("forkchoice: unable to resolve %s consensus path within %d slots from anchor %d after observing slot %d; falling back to RPC catchup (discarded_observed_blocks=%d discarded_ready_decisions=%d)",
								consensusLiveStreamName, consensusCfg.maxDepth, anchorSlot, block.Slot, discardedObservedBlocks, readyDecisionCount)
							consensusBufferedExecutionActive = false
							readyConsensusPath = nil
							clearObservedConsensusBlocks(observedConsensusBlocks)
							observeConsensusAnchor()
							blockStream.ForceRPCFallback("consensus_depth_exceeded")
							continue
						}
						if consensusCoordinator.Policy() == "halt" {
							result.Error = fmt.Errorf("forkchoice: unable to resolve a confirmed path within %d slots from anchor %d",
								consensusCfg.maxDepth, currentConsensusAnchorSlot())
							break
						}
						mlog.Log.Warnf("forkchoice: path resolution exceeded max depth from anchor %d", currentConsensusAnchorSlot())
						continue
					default:
						mlog.Log.Warnf("forkchoice: failed to resolve a confirmed path from anchor %d after observing slot %d: %v",
							currentConsensusAnchorSlot(), block.Slot, err)
						result.Error = err
					}
				}
				if result.Error != nil {
					break
				}
				if resolvedPath == nil || len(resolvedPath.SlotDecisions) == 0 {
					continue
				}

				readyConsensusPath = newPendingConsensusPath(currentConsensusAnchorSlot(), resolvedPath)
				continue
			}
		}

		if block == nil {
			break
		}

		// Handle skipped slots - log and continue without execution
		if block.IsSkipped {
			if commit, ok := TakeLocalLeaderCommit(block.Slot); ok && commit.SlotCtx != nil {
				lastSlotCtx = commit.SlotCtx
				if speculativeEnabled && commit.Block != nil {
					replayCtx.Capitalization -= lastSlotCtx.LamportsBurnt
					if err := speculative.FinalizePendingSnapshot(block.Slot, replayCtx, captureReplayAux()); err != nil {
						result.Error = fmt.Errorf("local leader slot %d: %w", block.Slot, err)
						break
					}
					if consensusEngine != nil {
						if err := consensusEngine.ObserveBlock(ctx, consensusengine.BlockObservation{
							Block:  commit.Block,
							Source: "local-leader",
							At:     time.Now(),
						}); err != nil {
							result.Error = fmt.Errorf("consensus engine observe local leader block: %w", err)
							break
						}
						var bankhash [32]byte
						copy(bankhash[:], lastSlotCtx.FinalBankhash)
						if err := consensusEngine.OnReplayResult(ctx, consensusengine.SlotReplayResult{
							Slot:     block.Slot,
							Bankhash: bankhash,
							Source:   "local-leader",
							At:       time.Now(),
						}); err != nil {
							result.Error = fmt.Errorf("consensus engine local leader result: %w", err)
							break
						}
					}
					if switched, err := speculative.ReconcileCertifiedTail(alpenglowNextDecision, rollbackParams()); err != nil {
						result.Error = err
						break
					} else if switched {
						continue
					}
					speculative.TryCommitPending(acctsDb, pt, commit.Block, commit.Block.BlockHeight, replayCtx, alpenglowNextDecision)
					if err := speculative.Err(); err != nil {
						result.Error = err
						break
					}
				}
				UpdateChainTipFromSlotCtx(lastSlotCtx, replayCtx.CurrentFeatures)
				blockStream.SetLastExecutedSlot(block.Slot)
				leaderStr := "unknown"
				if leader, exists := global.LeaderForSlot(block.Slot); exists {
					leaderStr = leader.String()
				}
				mlog.Log.InfofPrecise("slot %-10d | leader: %-44s | txns: local           | cu: N/A        | exec:     N/A | wait:%7.3fs | total:%7.3fs (local leader commit)",
					block.Slot, leaderStr, waitTime.Seconds(), waitTime.Seconds())
				if rpcServer != nil {
					rpcServer.SetSlotCtx(lastSlotCtx)
				}
				continue
			}
			// Look up leader for informational logging
			leaderStr := "unknown"
			if leader, exists := global.LeaderForSlot(block.Slot); exists {
				leaderStr = leader.String()
			}
			// Log skipped slot in same format as regular blocks (with N/A for missing values)
			// Padding: cu=10 chars, txns fields, exec/wait/total=%7.3fs = 8 chars (7 for number + 's')
			mlog.Log.InfofPrecise("slot %-10d | leader: %-44s | txns: N/A              | cu: N/A        | exec:     N/A | wait:%7.3fs | total:%7.3fs (skip)",
				block.Slot, leaderStr, waitTime.Seconds(), waitTime.Seconds())
			skippedSlotsCount++
			// A resolved skip still advances replay progress for near-tip mode and
			// consensus-managed Lightbringer delivery.
			blockStream.SetLastExecutedSlot(block.Slot)
			continue // Skip all execution - no state changes for skipped slots
		}

		if ctx.Err() != nil {
			mlog.Log.Infof("context cancelled, stopping replay: %v", ctx.Err())
			result.WasCancelled = true
			break
		}
		start := time.Now()

		// Notify block source we're starting execution - in near-tip mode this
		// triggers fetching N+1 so RPC latency overlaps with execution time
		blockStream.NotifyBlockStart(block.Slot)

		currentSlot = block.Slot
		block.Epoch = epochSchedule.GetEpoch(currentSlot)
		var configErr error
		initialBlockConfigured := lastSlotCtx == nil
		// Use lastSlotCtx == nil to detect first block, not currentSlot == startSlot.
		// This handles the case where startSlot (or slots after it) are skipped -
		// the first emitted block might have slot > startSlot.
		if initialBlockConfigured {
			if resumeState != nil {
				// RESUME: Use resume state + state file (for static fields)
				configErr = configureInitialBlockFromResume(acctsDb, block, resumeState, mithrilState, epochSchedule)
			} else {
				// FRESH START: Use state file manifest_* fields
				configErr = configureInitialBlock(acctsDb, block, mithrilState, epochSchedule)
			}
		} else {
			configErr = configureBlock(block, lastSlotCtx, epochSchedule)
		}
		if configErr != nil {
			mlog.Log.Errorf("FATAL: block configuration failed: %v", configErr)
			mlog.Log.Errorf("Triggering graceful shutdown to preserve AccountsDB state.")
			result.Error = configErr
			break
		}
		if initialBlockConfigured && resumeState != nil && speculativeEnabled {
			speculative.SeedFromResume(resumeState, replayCtx)
		}
		if initialBlockConfigured {
			// Initial block configuration rebuilds VoteCache and EpochAuthorizedVoters
			// from AccountsDB. Forkchoice is created before that happens, so refresh
			// its epoch view here to avoid using stale manifest voters after resume or
			// an epoch boundary.
			forkChoice.UpdateEpoch(
				block.Epoch,
				global.EpochStakes(block.Epoch),
				global.EpochTotalStake(block.Epoch),
				global.EpochAuthorizedVoters(),
			)
			if consensusEngine != nil {
				if err := consensusengine.RefreshAlpenglowValidatorSet(consensusEngine, block.Epoch); err != nil {
					mlog.Log.Warnf("ALPENGLOW observer: %v", err)
				}
			}
		}

		// Log replay start message once, after initial configuration completes
		if !replayStartLogged {
			fmt.Println()
			fmt.Println("=== Replay Start ===")
			replayStartLogged = true
		}

		// epoch boundary
		if block.Epoch != currentEpoch {
			mlog.Log.Infof("")
			mlog.Log.Infof("=== Epoch Boundary ===")
			mlog.Log.Infof("%d -> %d", currentEpoch, currentEpoch+1)

			var newlyActivatedFeatures, parentNewlyActivatedFeatures []*accounts.Account
			replayCtx.CurrentFeatures, newlyActivatedFeatures, parentNewlyActivatedFeatures = scanAndEnableFeatures(acctsDb, replayCtx, currentSlot, true)
			if alpenglowReplayMode {
				applyAlpenglowRuntimeFeatureOverrides(replayCtx.CurrentFeatures, currentSlot)
			}
			alpenglowClock = useAlpenglowClockSemantics(alpenglowReplayMode, replayCtx.CurrentFeatures)
			partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
			partitionedRewardsInfo = handleEpochTransition(acctsDb, partitionedEpochRewardsEnabled, lastSlotCtx, replayCtx, epochSchedule, replayCtx.CurrentFeatures, block, currentEpoch, rpcc, dbgOpts, alpenglowReplayMode)
			currentEpoch = block.Epoch
			justCrossedEpochBoundary = true

			// Refresh forkchoice with new epoch's stake weights and authorized voters
			forkChoice.UpdateEpoch(
				currentEpoch,
				global.EpochStakes(currentEpoch),
				global.EpochTotalStake(currentEpoch),
				global.EpochAuthorizedVoters(),
			)

			if consensusOpts != nil && consensusOpts.Engine != nil {
				consensusengine.RefreshAlpenglowValidatorSetsFromCache(consensusOpts.Engine)
			}

			// Persist rebuilt authorized voters to state file so resume loads fresh data
			if !speculativeEnabled {
				if cache := global.EpochAuthorizedVoters(); cache != nil && mithrilState != nil {
					updatedVoters := make(map[string][]string, cache.Len())
					for voteAcct, voters := range cache.Entries() {
						voterStrs := make([]string, len(voters))
						for i, v := range voters {
							voterStrs[i] = base58.Encode(v[:])
						}
						updatedVoters[base58.Encode(voteAcct[:])] = voterStrs
					}
					mithrilState.ManifestEpochAuthorizedVoters = updatedVoters
				}
			}

			if len(newlyActivatedFeatures) != 0 {
				block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, newlyActivatedFeatures...)
				block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parentNewlyActivatedFeatures...)
			}
			if len(featuresActivatedInFirstSlot) != 0 {
				block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, featuresActivatedInFirstSlot...)
				block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parentFeaturesActivatedInFirstSlot...)
				featuresActivatedInFirstSlot = nil
				parentFeaturesActivatedInFirstSlot = nil
			}
		}

		block.Features = replayCtx.CurrentFeatures

		// post-epoch boundary rewards distribution
		if partitionedEpochRewardsEnabled && partitionedRewardsInfo != nil && currentSlot >= partitionedRewardsInfo.FirstStakingRewardSlot && partitionedRewardsInfo.NumRewardPartitionsRemaining > 0 {
			distributedAccts, parentDistributedAccts := distributePartitionedEpochRewardsForSlot(acctsDb, replayCtx, partitionedRewardsInfo, currentSlot, block.BlockHeight)
			block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, distributedAccts...)
			block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parentDistributedAccts...)
		}

		metrics.GlobalBlockReplay.PreprocessBlock.AddTimingSince(start)

		deferPersist := speculativeEnabled
		var deferred *DeferredBlockCommit
		lastSlotCtx, deferred, err = ProcessBlock(acctsDb, block, epochSchedule, txParallelism, dbgOpts, pt, alpenglowClock, deferPersist, speculative, turbineShredVersion)
		if err != nil {
			mlog.Log.Errorf("error encountered during block replay: %s\n", err)
			result.Error = err
			// Preserve entries from earlier finalized-but-unfolded slots. Only the
			// failing slot and its speculative suffix are invalid.
			global.DropPendingStakePubkeysFrom(block.Slot)
			break
		}
		UpdateChainTipFromSlotCtx(lastSlotCtx, replayCtx.CurrentFeatures)
		global.SetBlockHeight(block.BlockHeight)

		if rpcServer != nil {
			rpcServer.SetSlotCtx(lastSlotCtx)
		}

		if consensusEngine != nil {
			var finalBankhash [32]byte
			copy(finalBankhash[:], lastSlotCtx.FinalBankhash)
			stats := blockStream.GetFetchStats()
			if err := consensusEngine.OnReplayResult(ctx, consensusengine.SlotReplayResult{
				Slot:     block.Slot,
				Bankhash: finalBankhash,
				Source:   stats.CurrentSource,
				At:       time.Now(),
			}); err != nil {
				result.Error = fmt.Errorf("consensus engine replay result: %w", err)
				global.DropPendingStakePubkeysFrom(block.Slot)
				break
			}
		}

		if consensusBufferedExecutionActive {
			if readyConsensusPath != nil && block.Slot == readyConsensusPath.leafSlot {
				actualBankhash := solana.HashFromBytes(lastSlotCtx.FinalBankhash)
				if actualBankhash != readyConsensusPath.leafBankhash {
					mlog.Log.Errorf("CONSENSUS MISMATCH: replayed leaf slot %d to bankhash %s, but votes confirmed %s",
						block.Slot,
						base58.Encode(actualBankhash[:]),
						base58.Encode(readyConsensusPath.leafBankhash[:]),
					)
					writeConsensusArtifact(
						fmt.Sprintf("bankhash_mismatch_slot_%d.json", block.Slot),
						buildConsensusMismatchArtifact(
							block,
							lastSlotCtx,
							readyConsensusPath,
							actualBankhash,
							blockStream.GetFetchStats(),
							forkChoice,
							consensusCoordinator.Policy(),
							observedConsensusBlocks,
							currentConsensusAnchorSlot(),
						),
					)
					if consensusCoordinator.Policy() == "halt" {
						result.Error = fmt.Errorf("consensus halt: slot %d bankhash mismatch (our=%s winning=%s)",
							block.Slot, base58.Encode(actualBankhash[:]), base58.Encode(readyConsensusPath.leafBankhash[:]))
						break
					}
				}
				readyConsensusPath = nil
			}
			observeConsensusAnchor()
			pruneObservedConsensusBlocks(observedConsensusBlocks, currentConsensusAnchorSlot())
		}

		replayCtx.Capitalization -= lastSlotCtx.LamportsBurnt
		if deferred != nil {
			if err := speculative.TrackPending(deferred, replayCtx, captureReplayAux()); err != nil {
				result.Error = fmt.Errorf("speculative replay slot %d: %w", block.Slot, err)
				break
			}
			if switched, err := speculative.ReconcileCertifiedTail(alpenglowNextDecision, rollbackParams()); err != nil {
				result.Error = err
				break
			} else if switched {
				deferred = nil
				continue
			}
			if speculative.TryCommitPending(acctsDb, pt, block, block.BlockHeight, replayCtx, alpenglowNextDecision) {
				deferred = nil
			}
			if err := speculative.Err(); err != nil {
				result.Error = err
				break
			}
		} else {
			speculative.UpdateCommittedHead(lastSlotCtx, replayCtx, block.BlockHeight)
		}

		// Clear ManifestEpochStakes after first replayed slot past snapshot
		// This frees memory and ensures we don't use stale manifest data on restart
		if block.Slot > mithrilState.SnapshotSlot && len(mithrilState.ManifestEpochStakes) > 0 {
			mithrilState.ClearManifestEpochStakes()
			mlog.Log.Debugf("cleared manifest_epoch_stakes after replaying past snapshot slot")
		}

		// Check for cancellation immediately after block completes.
		// This minimizes the window between bankhash persistence and state file update,
		// preventing false "corruption" detection on graceful shutdown.
		if ctx.Err() != nil {
			mlog.Log.Infof("Context cancelled after slot %d, exiting replay loop", block.Slot)
			result.WasCancelled = true
			if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
				result.WasCancelled = false
				result.Error = fmt.Errorf("validator safety halt: %w", cause)
			}

			if speculativeEnabled {
				if _, err := speculative.ReconcileCertifiedTail(alpenglowNextDecision, rollbackParams()); err != nil {
					mlog.Log.Errorf("rooted-durable shutdown reconciliation: %v", err)
				}
				if err := speculative.FlushFinalized(pt, alpenglowNextDecision); err != nil {
					mlog.Log.Errorf("rooted-durable shutdown fold: %v", err)
				}
			}
			acctsDb.WaitForStoreWorker()

			// Populate result immediately for state write
			result.LastPersistedSlot, result.LastPersistedBankhash = pt.Get()
			result.LastBlockHeight = global.BlockHeight()

			// Capture resume context from the last slot context
			if lastSlotCtx != nil {
				result.LastAcctsLtHash = lastSlotCtx.AcctsLtHash
				if lastSlotCtx.FeeRateGovernor != nil {
					result.LastLamportsPerSignature = lastSlotCtx.FeeRateGovernor.LamportsPerSignature
					result.LastPrevLamportsPerSig = lastSlotCtx.FeeRateGovernor.PrevLamportsPerSignature
				}
				result.LastNumSignatures = lastSlotCtx.NumSignatures
				result.LastRecentBlockhashes = sealevel.SysvarCache.RecentBlockHashes.Sysvar
				result.LastEvictedBlockhash = lastSlotCtx.LatestEvictedBlockhash
				result.LastBlockhash = lastSlotCtx.Blockhash
				result.LastSlotHashes = sealevel.SysvarCache.SlotHashes.Sysvar
			}

			// Capture ReplayCtx fields for resume independence from stale manifest
			result.LastCapitalization = replayCtx.Capitalization
			result.LastSlotsPerYear = replayCtx.SlotsPerYear
			result.LastInflation = replayCtx.Inflation

			// Serialize all epoch stakes for persistence
			result.ComputedEpochStakes = serializeAllEpochStakes()
			if speculativeEnabled {
				speculative.PopulateDurableResult(result)
			}

			// Write state immediately via callback (eliminates timing window for hard kills)
			if onCancelWriteState != nil {
				if err := onCancelWriteState(result); err != nil {
					mlog.Log.Errorf("failed to write state on cancel: %v", err)
				} else {
					result.StateWrittenOnCancel = true
				}
			}

			break
		}

		slotReplayDuration := time.Since(start)

		// Calculate slot stats: vote/non-vote tx counts and locally replayed CU.
		var voteTxCount, nonVoteTxCount int
		for _, tx := range block.Transactions {
			if tx.IsVote() {
				voteTxCount++
			} else {
				nonVoteTxCount++
			}
		}
		totalCU := lastSlotCtx.TotalComputeUnitsConsumed

		// Get leader from block (set by configureBlock in live mode, or by block source in verify mode)
		leaderStr := "unknown"
		if !block.Leader.IsZero() {
			leaderStr = block.Leader.String()
		}

		// Fixed-width format for consistent alignment (use precise timing for block replay)
		// exec/wait/total use 7 char width to handle times up to 99.999s without breaking alignment
		totalSlotTime := waitTime + slotReplayDuration
		mlog.Log.InfofPrecise("slot %-10d | leader: %-44s | txns: v:%-5d nv:%-5d | cu: %-10d | exec:%7.3fs | wait:%7.3fs | total:%7.3fs",
			block.Slot, leaderStr, voteTxCount, nonVoteTxCount, totalCU, slotReplayDuration.Seconds(), waitTime.Seconds(), totalSlotTime.Seconds())

		// Write bankhash to log file
		if bankhashLogFile != nil {
			fmt.Fprintf(bankhashLogFile, "%d %s\n", block.Slot, base58.Encode(lastSlotCtx.FinalBankhash))
		}

		statsd.Count(statsd.SlotReplays, 1, nil)
		statsd.Timing(statsd.SlotReplayDurationMs, uint64(slotReplayDuration.Nanoseconds())/1e6, nil)
		statsd.Gauge(statsd.Epoch, float64(block.Epoch), nil)
		statsd.Gauge(statsd.Slot, float64(block.Slot), nil)
		statsd.Timing(statsd.TxsPerBlock, uint64(len(block.Transactions)), nil)

		// Track last executed slot for accurate tip distance calculation and mode switching
		blockStream.SetLastExecutedSlot(block.Slot)

		if !justCrossedEpochBoundary {
			statsCounter++
			execTimes = append(execTimes, slotReplayDuration.Seconds())
			waitTimes = append(waitTimes, waitTime.Seconds())
			cuValues = append(cuValues, totalCU)
			voteTxCounts = append(voteTxCounts, uint64(voteTxCount))
			nonVoteTxCounts = append(nonVoteTxCounts, uint64(nonVoteTxCount))

			// Trigger async tip refresh 5 slots before summary so it's fresh when we print
			if statsCounter == summaryInterval-5 {
				blockStream.RefreshTipsForSummary()
			}

			if statsCounter == summaryInterval {
				// Calculate statistics for float64 slices
				medianFloat := func(vals []float64) float64 {
					if len(vals) == 0 {
						return 0
					}
					sorted := make([]float64, len(vals))
					copy(sorted, vals)
					sort.Float64s(sorted)
					n := len(sorted)
					if n%2 == 0 {
						return (sorted[n/2-1] + sorted[n/2]) / 2
					}
					return sorted[n/2]
				}
				minFloat := func(vals []float64) float64 {
					if len(vals) == 0 {
						return 0
					}
					m := vals[0]
					for _, v := range vals[1:] {
						if v < m {
							m = v
						}
					}
					return m
				}
				maxFloat := func(vals []float64) float64 {
					if len(vals) == 0 {
						return 0
					}
					m := vals[0]
					for _, v := range vals[1:] {
						if v > m {
							m = v
						}
					}
					return m
				}

				// Calculate statistics for uint64 slices
				medianUint := func(vals []uint64) uint64 {
					if len(vals) == 0 {
						return 0
					}
					sorted := make([]uint64, len(vals))
					copy(sorted, vals)
					sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
					n := len(sorted)
					if n%2 == 0 {
						return (sorted[n/2-1] + sorted[n/2]) / 2
					}
					return sorted[n/2]
				}
				minUint := func(vals []uint64) uint64 {
					if len(vals) == 0 {
						return 0
					}
					m := vals[0]
					for _, v := range vals[1:] {
						if v < m {
							m = v
						}
					}
					return m
				}
				maxUint := func(vals []uint64) uint64 {
					if len(vals) == 0 {
						return 0
					}
					m := vals[0]
					for _, v := range vals[1:] {
						if v > m {
							m = v
						}
					}
					return m
				}

				// Compute total times (exec + wait for each block)
				totalTimes := make([]float64, len(execTimes))
				for i := range execTimes {
					totalTimes[i] = execTimes[i] + waitTimes[i]
				}

				// Execution stats
				medExec := medianFloat(execTimes)
				minExec := minFloat(execTimes)
				maxExec := maxFloat(execTimes)

				// Wait stats
				medWait := medianFloat(waitTimes)
				minWait := minFloat(waitTimes)
				maxWait := maxFloat(waitTimes)

				// Total stats (only median needed - min/max can be inferred from execution + wait)
				medTotal := medianFloat(totalTimes)

				// CU stats
				medCU := medianUint(cuValues)
				minCU := minUint(cuValues)
				maxCU := maxUint(cuValues)

				// Txn stats
				medVoteTx := medianUint(voteTxCounts)
				medNonVoteTx := medianUint(nonVoteTxCounts)

				// Blocks per second based on median total time
				var blocksPerSec float64
				if medTotal > 0 {
					blocksPerSec = 1.0 / medTotal
				}

				// Get fetch stats (includes tip snapshot - refreshed at slot 95)
				fetchStats := blockStream.GetFetchStats()

				// Calculate distance from tip using current slot (more accurate than TipAtSlot)
				// TipAtSlot is when we started the refresh, but block.Slot is what we just executed
				var tipDistanceStr string
				currentSlotForTip := block.Slot
				if fetchStats.ConfirmedTip > 0 {
					var behindConfirmed uint64
					if currentSlotForTip < fetchStats.ConfirmedTip {
						behindConfirmed = fetchStats.ConfirmedTip - currentSlotForTip
					}
					tipDistanceStr = fmt.Sprintf("%d slots behind confirmed", behindConfirmed)
				} else {
					tipDistanceStr = "tip unknown"
				}

				// Print summary in reorganized format
				mlog.Log.InfofPrecise("")
				mlog.Log.InfofPrecise("=== 100 Slot Summary ===")

				// Line 1: Mode, blocks/sec, skipped slots, tip distance
				modeStr := "catchup"
				if fetchStats.IsNearTip {
					modeStr = "near-tip"
				}
				if skippedSlotsCount > 0 {
					mlog.Log.InfofPrecise("  mode: %s | %.1f blocks/sec | %d skipped | %s",
						modeStr, blocksPerSec, skippedSlotsCount, tipDistanceStr)
				} else {
					mlog.Log.InfofPrecise("  mode: %s | %.1f blocks/sec | %s",
						modeStr, blocksPerSec, tipDistanceStr)
				}

				// Line 2: Current block source
				mlog.Log.InfofPrecise("  block source: %s", formatBlockSourceStatus(fetchStats))
				if consensusEngine != nil {
					if status, ok := formatAlpenglowObserverStatus(consensusEngine.Snapshot()); ok {
						mlog.Log.InfofPrecise("  alpenglow observer: %s", status)
					}
				}
				if consensusBufferedExecutionActive {
					readyDecisionCount := 0
					readyLeafSlot := uint64(0)
					if readyConsensusPath != nil {
						readyDecisionCount = len(readyConsensusPath.decisions)
						readyLeafSlot = readyConsensusPath.leafSlot
					}
					mlog.Log.InfofPrecise("  consensus buffer: observed=%d ready_decisions=%d anchor=%d ready_leaf=%d",
						len(observedConsensusBlocks), readyDecisionCount, currentConsensusAnchorSlot(), readyLeafSlot)
				}

				// Line 3: CU and transaction stats (median/min/max)
				mlog.Log.InfofPrecise("  cu: median %d, min %d, max %d | txns: median vote %d, median non-vote %d",
					medCU, minCU, maxCU, medVoteTx, medNonVoteTx)

				// Line 4: Execution stats (median/min/max for execution, wait; median for replay total)
				mlog.Log.InfofPrecise("  execution: median %.3fs, min %.3fs, max %.3fs | wait: median %.3fs, min %.3fs, max %.3fs | replay total: median %.3fs",
					medExec, minExec, maxExec, medWait, minWait, maxWait, medTotal)

				// Account clone stats for copy-on-write optimization profiling
				cloneStats := GetAndResetCloneStats()
				if cloneStats.TxCount > 0 {
					var cloneRatio float64
					if cloneStats.AcctsLoaded > 0 {
						cloneRatio = float64(cloneStats.AcctsCloned) / float64(cloneStats.AcctsLoaded) * 100
					}
					avgLoadedPerTx := float64(cloneStats.AcctsLoaded) / float64(cloneStats.TxCount)
					avgClonedPerTx := float64(cloneStats.AcctsCloned) / float64(cloneStats.TxCount)
					avgTouchedPerTx := float64(cloneStats.AcctsTouched) / float64(cloneStats.TxCount)
					loadedMB := float64(cloneStats.AcctsLoadedBytes) / 1024 / 1024
					clonedMB := float64(cloneStats.AcctsClonedBytes) / 1024 / 1024
					touchedMB := float64(cloneStats.AcctsTouchedBytes) / 1024 / 1024
					mlog.Log.InfofPrecise("  account COW: %.1f%% cloned on write (%d/%d accts) | %.1fMB loaded, %.1fMB cloned, %.1fMB modified | avg/tx: %.1f loaded, %.1f cloned, %.1f modified",
						cloneRatio, cloneStats.AcctsCloned, cloneStats.AcctsLoaded,
						loadedMB, clonedMB, touchedMB, avgLoadedPerTx, avgClonedPerTx, avgTouchedPerTx)
				}

				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				const gib = 1024 * 1024 * 1024
				mlog.Log.InfofPrecise("  memory: alloc %.1fGiB | inuse %.1fGiB | idle %.1fGiB | released %.1fGiB | next_gc %.1fGiB | objs %d | gc %d | queue=%d",
					float64(mem.HeapAlloc)/gib,
					float64(mem.HeapInuse)/gib,
					float64(mem.HeapIdle)/gib,
					float64(mem.HeapReleased)/gib,
					float64(mem.NextGC)/gib,
					mem.HeapObjects,
					mem.NumGC,
					acctsDb.StoreQueueLen(),
				)

				// Line 5: RPC/fetch debugging info
				if fetchStats.Attempts > 0 {
					retryRate := float64(fetchStats.Retries) / float64(fetchStats.Attempts) * 100
					prefetch := fetchStats.BufferDepth + fetchStats.ReorderBufLen
					mlog.Log.InfofPrecise("  getBlock fetch: %.1f rps (%d calls) | avg %.0fms | %.0f%% success | retries %.1f%% | buf %d (stream:%d ro:%d) | wq %d | errs: na:%d rl:%d bt:%d tr:%d",
						fetchStats.GetBlockRPS, fetchStats.Attempts, fetchStats.AvgLatencyMs, fetchStats.SuccessRate, retryRate, prefetch, fetchStats.BufferDepth, fetchStats.ReorderBufLen,
						fetchStats.WorkQueueLen, fetchStats.ErrNotAvail, fetchStats.ErrRateLimit, fetchStats.ErrBeyondTip, fetchStats.ErrTransient)

					// Surface tip poll issues (only show if there are problems)
					if fetchStats.TipStaleSecs > 30 || fetchStats.TotalTipPollFails > 0 {
						mlog.Log.InfofPrecise("  WARNING: tip stale %ds | tip poll fails: %d (consecutive: %d)",
							fetchStats.TipStaleSecs, fetchStats.TotalTipPollFails, fetchStats.TipPollFailures)
					}

					blockStream.ResetStats()
				}
				mlog.Log.InfofPrecise("")

				// Reset slices (reuse capacity)
				execTimes = execTimes[:0]
				waitTimes = waitTimes[:0]
				cuValues = cuValues[:0]
				voteTxCounts = voteTxCounts[:0]
				nonVoteTxCounts = nonVoteTxCounts[:0]
				statsCounter = 0
				skippedSlotsCount = 0
			}
		} else {
			justCrossedEpochBoundary = false
		}
		{
			metrics.GlobalBlockReplay.Slot = block.Slot
			if metricsWriter != nil {
				encoder := json.NewEncoder(metricsWriter)
				err := encoder.Encode(metrics.GlobalBlockReplay)
				if err != nil {
					mlog.Log.Errorf("Error marshaling latencies: %v", err)
				}
			}
			statsd.SendBlockReplayMetrics(metrics.GlobalBlockReplay)
			metrics.GlobalBlockReplay = metrics.BlockReplay{}
		}

	}

	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) && result.Error == nil {
		result.WasCancelled = false
		result.Error = fmt.Errorf("validator safety halt: %w", cause)
	}

	// Check if block source stalled (this provides explicit error info)
	if blockStream.Stalled() && result.Error == nil {
		result.Error = fmt.Errorf("block fetch stalled - no progress for %v", blockStream.StallTimeout())
	}
	if speculativeEnabled {
		if _, err := speculative.ReconcileCertifiedTail(alpenglowNextDecision, rollbackParams()); err != nil && result.Error == nil {
			result.Error = err
		}
		if err := speculative.FlushFinalized(pt, alpenglowNextDecision); err != nil && result.Error == nil {
			result.Error = err
		}
	}

	acctsDb.WaitForStoreWorker()
	result.LastPersistedSlot, result.LastPersistedBankhash = pt.Get()
	result.LastBlockHeight = global.BlockHeight()

	// Capture resume context from the last slot context (if available)
	// This enables proper resume from Ctrl+C shutdown
	if lastSlotCtx != nil {
		result.LastAcctsLtHash = lastSlotCtx.AcctsLtHash
		if lastSlotCtx.FeeRateGovernor != nil {
			result.LastLamportsPerSignature = lastSlotCtx.FeeRateGovernor.LamportsPerSignature
			result.LastPrevLamportsPerSig = lastSlotCtx.FeeRateGovernor.PrevLamportsPerSignature
		}
		result.LastNumSignatures = lastSlotCtx.NumSignatures

		// Capture blockhash context from SysvarCache (required because appendvec writes are not fsynced)
		result.LastRecentBlockhashes = sealevel.SysvarCache.RecentBlockHashes.Sysvar
		result.LastEvictedBlockhash = lastSlotCtx.LatestEvictedBlockhash
		result.LastBlockhash = lastSlotCtx.Blockhash

		// Capture SlotHashes context (same issue, vote program needs accurate slot→hash mappings)
		result.LastSlotHashes = sealevel.SysvarCache.SlotHashes.Sysvar
	}

	// Capture ReplayCtx fields for resume independence from stale manifest
	result.LastCapitalization = replayCtx.Capitalization
	result.LastSlotsPerYear = replayCtx.SlotsPerYear
	result.LastInflation = replayCtx.Inflation

	// Serialize all epoch stakes for persistence
	result.ComputedEpochStakes = serializeAllEpochStakes()
	if speculativeEnabled {
		speculative.PopulateDurableResult(result)
	}

	return result
}

func runIncinerator(slotCtx *sealevel.SlotCtx) {
	incineratorAcct, err := slotCtx.GetAccount(a.IncineratorAddr)
	if err != nil || incineratorAcct.Lamports == 0 {
		return
	}
	newIncineratorAcct := &accounts.Account{Key: a.IncineratorAddr, Owner: a.SystemProgramAddr, RentEpoch: math.MaxUint64}
	slotCtx.SetAccount(a.IncineratorAddr, newIncineratorAcct)
	slotCtx.LamportsBurnt += incineratorAcct.Lamports
	slotCtx.RecordModifiedAcct(a.IncineratorAddr)
}

func compileWritableAndModifiedAccts(slotCtx *sealevel.SlotCtx, block *b.Block, rentAccts []*accounts.Account, sysvarsAlreadyUpdated bool) ([]*accounts.Account, []*accounts.Account) {
	writableAccts := make([]*accounts.Account, 0, len(slotCtx.WritableAccts)+len(block.UpdatedAccts)+len(rentAccts)+4)
	modifiedAccts := make([]*accounts.Account, 0, len(slotCtx.ModifiedAccts)+len(block.UpdatedAccts)+len(rentAccts)+4)
	alreadyAdded := make(map[solana.PublicKey]bool, len(slotCtx.WritableAccts))

	for pk := range slotCtx.WritableAccts {
		acct, _ := slotCtx.GetAccount(pk)
		writableAccts = append(writableAccts, acct)
		alreadyAdded[pk] = true
	}

	for pk := range slotCtx.ModifiedAccts {
		acct, err := slotCtx.GetAccount(pk)
		if err != nil {
			mlog.Log.Infof("compileWritableAndModifiedAccts: unable to get %s", acct.Key)
		}
		modifiedAccts = append(modifiedAccts, acct)
	}

	for _, eua := range block.EpochUpdatedAccts {
		if eua == nil {
			continue
		}
		if _, exists := alreadyAdded[eua.Key]; exists {
			continue
		}
		acct, err := slotCtx.GetAccount(eua.Key)
		if err != nil {
			acct, err = slotCtx.GetAccountFromAccountsDb(eua.Key)
			if err != nil {
				panic(fmt.Sprintf("unable to fetch %s from neither SlotCtx nor accountsdb for inclusion in bankhash in slot %d", eua.Key, slotCtx.Slot))
			}
		}
		writableAccts = append(writableAccts, acct)
		modifiedAccts = append(modifiedAccts, acct)
	}

	writableAccts = append(writableAccts, rentAccts...)
	modifiedAccts = append(modifiedAccts, rentAccts...)

	var sysvarAccts []*accounts.Account
	if sysvarsAlreadyUpdated {
		sysvarAccts = collectSysvarAcctsFromSlotCtx(slotCtx)
	} else {
		sysvarAccts = collectAndUpdateSysvarAcctsForAdh(slotCtx)
	}
	writableAccts = append(writableAccts, sysvarAccts...)
	modifiedAccts = append(modifiedAccts, sysvarAccts...)

	return writableAccts, modifiedAccts
}

func ensureVoteTimestamps(m map[solana.PublicKey]sealevel.BlockTimestamp) map[solana.PublicKey]sealevel.BlockTimestamp {
	if m != nil {
		return m
	}
	return make(map[solana.PublicKey]sealevel.BlockTimestamp)
}

func ensureVoteAccts(m map[solana.PublicKey]uint64) map[solana.PublicKey]uint64 {
	if m != nil {
		return m
	}
	return make(map[solana.PublicKey]uint64)
}

func newSlotCtx(block *b.Block, accts accounts.Accounts, parentAccts accounts.Accounts, acctsDb *accountsdb.AccountsDb) *sealevel.SlotCtx {
	slotCtx := &sealevel.SlotCtx{
		Accounts:        accts,
		ParentAccts:     parentAccts,
		AccountsDb:      acctsDb,
		Slot:            block.Slot,
		ParentSlot:      block.ParentSlot,
		Epoch:           block.Epoch,
		FeeRateGovernor: block.FeeRateGovernor,
		NumSignatures:   block.NumSignatures,

		AcctMapsMu:    &sync.Mutex{},
		ModifiedAccts: make(map[solana.PublicKey]bool),
		WritableAccts: make(map[solana.PublicKey]bool),

		Blockhash:              block.Blockhash,
		LastBlockhash:          block.LastBlockhash,
		LatestEvictedBlockhash: block.LatestEvictedBlockhash,
		Replay:                 true,
		Features:               block.Features,
		AcctsLtHash:            block.AcctsLtHash,

		VoteAccts:       ensureVoteAccts(block.EpochStakesPerVoteAcct),
		VoteTimestampMu: &sync.Mutex{},
		VoteTimestamps:  ensureVoteTimestamps(block.VoteTimestamps),
		TotalEpochStake: block.TotalEpochStake,

		EahWorkaroundBankhash: block.EahWorkaroundBankhash,

		HasEahWorkaround: block.HasEahWorkaround,

		SerializedParameterArena: SerializedParameterArena,
	}

	return slotCtx
}

func sequentialTxLoop(slotCtx *sealevel.SlotCtx, sigverifyWg *sync.WaitGroup, block *b.Block, dbgOpts *DebugOptions) (fees.TxFeeInfoAccumulator, uint64) {
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	var totalComputeUnitsConsumed uint64
	// process & execute each transaction in turn
	for idx, tx := range block.Transactions {
		var txMeta *rpc.TransactionMeta
		if block.TxMetas != nil {
			txMeta = block.TxMetas[idx]
		}
		txFeeInfo, txComputeUnitsConsumed, txErr := ProcessTransaction(slotCtx, sigverifyWg, tx, txMeta, dbgOpts, nil)
		totalComputeUnitsConsumed += txComputeUnitsConsumed

		if txMeta == nil {
			if txFeeInfo == nil {
				panic(fmt.Sprintf("txFeeInfo is nil for tx %s in slot %d", tx.Signatures[0], block.Slot))
			}
			txFeeAccumulator.Add(txFeeInfo)
			continue
		}

		if txErr != nil {
			if txMeta.Err == nil && tx.IsVote() {
				mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: vote tx %s failed locally but succeeded onchain => bankhash mismatch at parent slot %d",
					CurrentRunID, block.Slot, tx.Signatures[0], block.ParentSlot)
				panic(fmt.Sprintf("vote tx %s failed in slot %d => bankhash mismatch at slot %d", tx.Signatures[0], block.Slot, block.ParentSlot))
			}
		}

		// check for success-failure return value divergences
		if txErr == nil && txMeta.Err != nil {
			mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s succeeded locally but failed onchain: %+v",
				CurrentRunID, block.Slot, tx.Signatures[0], block.TxMetas[idx].Err)
			panic(fmt.Sprintf("tx %s return value divergence: txErr was nil, but onchain err was %+v", tx.Signatures[0], block.TxMetas[idx].Err))
		} else if txErr != nil && txMeta.Err == nil {
			mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s failed locally (%v) but succeeded onchain",
				CurrentRunID, block.Slot, tx.Signatures[0], txErr)
			panic(fmt.Sprintf("tx %s return value divergence: txErr was %+v (%s), but onchain err was nil", tx.Signatures[0], txErr, txErr))
		}

		if txFeeInfo == nil {
			panic(fmt.Sprintf("txFeeInfo is nil for tx %s in slot %d", tx.Signatures[0], block.Slot))
		}
		txFeeAccumulator.Add(txFeeInfo)
	}
	return txFeeAccumulator, totalComputeUnitsConsumed
}

func lightbringerEntryExecutionBatches(transactions []*solana.Transaction, entry *b.TxEntry, relaxIntraBatchAccountLocks bool) [][]uint64 {
	if len(entry.Indices) == 0 {
		return nil
	}
	if !relaxIntraBatchAccountLocks {
		return [][]uint64{entry.Indices}
	}

	flushDerivableSegment := func(batches *[][]uint64, segment []uint64) {
		if len(segment) == 0 {
			return
		}

		// Build batches directly instead of allocating a full dependency graph per
		// entry segment. The per-entry fallback is intentionally cheap because the
		// common Lightbringer path should stay on the whole-block planner.
		segmentBatches := make([][]uint64, 0, 1)
		lastReadBatch := make(map[solana.PublicKey]int, len(segment)*4)
		lastWriteBatch := make(map[solana.PublicKey]int, len(segment)*2)
		for _, txIdx := range segment {
			tx := transactions[txIdx]
			readonlyAccounts := messageReadonlyAccounts(&tx.Message)
			writableAccounts := messageWritableAccounts(&tx.Message)

			batchIdx := 0
			for _, roAcct := range readonlyAccounts {
				if writeBatch, exists := lastWriteBatch[roAcct]; exists && writeBatch >= batchIdx {
					batchIdx = writeBatch + 1
				}
			}
			for _, writeAcct := range writableAccounts {
				if readBatch, exists := lastReadBatch[writeAcct]; exists && readBatch >= batchIdx {
					batchIdx = readBatch + 1
				}
				if writeBatch, exists := lastWriteBatch[writeAcct]; exists && writeBatch >= batchIdx {
					batchIdx = writeBatch + 1
				}
			}

			for len(segmentBatches) <= batchIdx {
				segmentBatches = append(segmentBatches, nil)
			}
			segmentBatches[batchIdx] = append(segmentBatches[batchIdx], txIdx)
			for _, roAcct := range readonlyAccounts {
				lastReadBatch[roAcct] = batchIdx
			}
			for _, writeAcct := range writableAccounts {
				lastWriteBatch[writeAcct] = batchIdx
			}
		}
		*batches = append(*batches, segmentBatches...)
	}

	// Under SIMD-0083, Lightbringer entries may legally contain intra-entry
	// conflicts. Recover safe parallelism for contiguous derivable segments and
	// treat unresolved transactions as ordering barriers.
	batches := make([][]uint64, 0, len(entry.Indices))
	derivableSegment := make([]uint64, 0, len(entry.Indices))
	for _, txIdx := range entry.Indices {
		if canDeriveAccountsFromMessage(transactions[txIdx]) {
			derivableSegment = append(derivableSegment, txIdx)
			continue
		}

		flushDerivableSegment(&batches, derivableSegment)
		derivableSegment = derivableSegment[:0]
		batches = append(batches, []uint64{txIdx})
	}
	flushDerivableSegment(&batches, derivableSegment)

	return batches
}

func parallelTxLoop(slotCtx *sealevel.SlotCtx, sigverifyWg *sync.WaitGroup, block *b.Block, rblock *b.Block, txParallelism int, dbgOpts *DebugOptions) (fees.TxFeeInfoAccumulator, uint64) {
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	txFeeInfos := make([]*fees.TxFeeInfo, len(block.Transactions))
	txComputeUnitsConsumed := make([]uint64, len(block.Transactions))
	errs := make([]error, len(block.Transactions))
	txDurations := make([]time.Duration, txParallelism)

	plannerBlock := block
	if rblock.FromLightbringer {
		plannerBlock = rblock
	}

	if canUseDependencyPlanner(plannerBlock) {
		do := make(chan int, len(block.Transactions))
		done := make(chan int, len(block.Transactions))
		go TopsortPlannerStream(plannerBlock, do, done)

		wg := &sync.WaitGroup{}
		wg.Add(txParallelism)
		for i := range txParallelism {
			go func() {
				defer wg.Done()
				for idx := range do {
					txStart := time.Now()
					tx := block.Transactions[idx]
					var txMeta *rpc.TransactionMeta
					if idx < len(rblock.TxMetas) {
						txMeta = rblock.TxMetas[idx]
					}
					txFeeInfos[idx], txComputeUnitsConsumed[idx], errs[idx] = ProcessTransaction(slotCtx, sigverifyWg, rblock.Transactions[idx], txMeta, dbgOpts, sealevel.BorrowedAccountArenas[i])
					txErr := errs[idx]
					// check for success-failure return value divergences
					if txMeta != nil && txErr == nil && txMeta.Err != nil {
						mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s succeeded locally but failed onchain: %+v",
							CurrentRunID, block.Slot, tx.Signatures[0], txMeta.Err)
						panic(fmt.Sprintf("tx %s return value divergence: txErr was nil, but onchain err was %+v", tx.Signatures[0], txMeta.Err))
					} else if txMeta != nil && txErr != nil && txMeta.Err == nil {
						mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s failed locally (%v) but succeeded onchain",
							CurrentRunID, block.Slot, tx.Signatures[0], txErr)
						panic(fmt.Sprintf("tx %s return value divergence: txErr was %+v (%s), but onchain err was nil", tx.Signatures[0], txErr, txErr))
					}
					txDurations[i] += time.Since(txStart)
					done <- idx
				}

			}()
		}

		wg.Wait()
		close(done)
	} else if rblock.FromLightbringer {
		batchWg := &sync.WaitGroup{}
		workersWg := &sync.WaitGroup{}
		do := make(chan uint64, txParallelism)
		workersWg.Add(txParallelism)
		for i := range txParallelism {
			go func(workerIdx int) {
				defer workersWg.Done()
				for idx := range do {
					txStart := time.Now()
					tx := block.Transactions[idx]
					var txMeta *rpc.TransactionMeta
					if int(idx) < len(rblock.TxMetas) {
						txMeta = rblock.TxMetas[idx]
					}
					txFeeInfos[idx], txComputeUnitsConsumed[idx], errs[idx] = ProcessTransaction(slotCtx, sigverifyWg, rblock.Transactions[idx], txMeta, dbgOpts, sealevel.BorrowedAccountArenas[workerIdx])
					txErr := errs[idx]
					if txMeta != nil && txErr == nil && txMeta.Err != nil {
						mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s succeeded locally but failed onchain: %+v",
							CurrentRunID, block.Slot, tx.Signatures[0], txMeta.Err)
						panic(fmt.Sprintf("tx %s return value divergence: txErr was nil, but onchain err was %+v", tx.Signatures[0], txMeta.Err))
					} else if txMeta != nil && txErr != nil && txMeta.Err == nil {
						mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s failed locally (%v) but succeeded onchain",
							CurrentRunID, block.Slot, tx.Signatures[0], txErr)
						panic(fmt.Sprintf("tx %s return value divergence: txErr was %+v (%s), but onchain err was nil", tx.Signatures[0], txErr, txErr))
					}
					txDurations[workerIdx] += time.Since(txStart)
					batchWg.Done()
				}
			}(i)
		}

		relaxIntraBatchAccountLocks := rblock.Features != nil &&
			rblock.Features.IsActive(features.RelaxIntraBatchAccountLocks)
		for _, entry := range rblock.Entries {
			batches := lightbringerEntryExecutionBatches(rblock.Transactions, entry, relaxIntraBatchAccountLocks)
			for _, batch := range batches {
				batchWg.Add(len(batch))
				for _, txIdx := range batch {
					do <- txIdx
				}
				batchWg.Wait()
			}
		}
		close(do)
		workersWg.Wait()
	} else {
		panic("dependency planner unavailable for non-Lightbringer block")
	}

	var totalComputeUnitsConsumed uint64
	for idx, txFeeInfo := range txFeeInfos {
		totalComputeUnitsConsumed += txComputeUnitsConsumed[idx]
		if txFeeInfo == nil {
			// This happens when IsTransactionAgeValid returns false (blockhash not found)
			tx := block.Transactions[idx]
			recentBlockhashes := sealevel.SysvarCache.RecentBlockHashes.Sysvar
			mlog.Log.Errorf("txFeeInfo is nil for tx %s in slot %d", tx.Signatures[0], block.Slot)
			mlog.Log.Errorf("  tx blockhash: %s", tx.Message.RecentBlockhash)
			mlog.Log.Errorf("  LatestEvictedBlockhash: %x", slotCtx.LatestEvictedBlockhash[:8])
			if recentBlockhashes != nil && len(*recentBlockhashes) > 0 {
				mlog.Log.Errorf("  RecentBlockhashes: %d entries, newest=%x, oldest=%x",
					len(*recentBlockhashes), (*recentBlockhashes)[0].Blockhash[:8], (*recentBlockhashes)[len(*recentBlockhashes)-1].Blockhash[:8])
			} else {
				mlog.Log.Errorf("  RecentBlockhashes: nil or empty!")
			}
			panic(fmt.Sprintf("txFeeInfo is nil - blockhash validation failed for tx %s", tx.Signatures[0]))
		}
		txFeeAccumulator.Add(txFeeInfo)
	}

	return txFeeAccumulator, totalComputeUnitsConsumed
}

func ProcessBlock(
	acctsDb *accountsdb.AccountsDb,
	block *b.Block,
	epochSchedule *sealevel.SysvarEpochSchedule,
	txParallelism int,
	dbgOpts *DebugOptions,
	// pt is updated after StoreAccounts completes through a callback.
	// Must be non-nil.
	pt *persistedTracker,
	alpenglowClock bool,
	deferPersist bool,
	spec *SpeculativeReplay,
	alpenglowShredVersion uint16,
) (*sealevel.SlotCtx, *DeferredBlockCommit, error) {
	ctx, task := trace.NewTask(context.Background(), "ProcessBlock")
	defer task.End()
	trace.Log(ctx, "slot", fmt.Sprintf("%d", block.Slot))
	trace.Log(ctx, "txCount", fmt.Sprintf("%d", len(block.Transactions)))

	var replayStage atomic.Value
	var replayStageSince atomic.Int64
	setReplayStage := func(stage string) {
		replayStage.Store(stage)
		replayStageSince.Store(time.Now().UnixNano())
	}
	setReplayStage("clone_transactions")

	replayWatchdogDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		var lastLoggedStage string
		var lastLoggedSince int64
		for {
			select {
			case <-replayWatchdogDone:
				return
			case <-ticker.C:
				stageVal := replayStage.Load()
				stage, ok := stageVal.(string)
				if !ok || stage == "" {
					continue
				}
				sinceUnix := replayStageSince.Load()
				if sinceUnix == 0 {
					continue
				}
				if stage == lastLoggedStage && sinceUnix == lastLoggedSince {
					continue
				}
				stageDuration := time.Since(time.Unix(0, sinceUnix))
				if stageDuration < 10*time.Second {
					continue
				}
				mlog.Log.Warnf("REPLAY WATCHDOG: slot %d stuck in stage %s for %s | txs=%d | lightbringer=%t",
					block.Slot, stage, stageDuration.Round(time.Second), len(block.Transactions), block.FromLightbringer)
				lastLoggedStage = stage
				lastLoggedSince = sinceUnix
			}
		}
	}()
	defer close(replayWatchdogDone)

	if SerializedParameterArena != nil {
		SerializedParameterArena.Reset()
	}

	var sigverifyWg sync.WaitGroup
	defer sigverifyWg.Wait()
	start := time.Now()
	unresolvedBlock := &b.Block{
		Transactions: make([]*solana.Transaction, len(block.Transactions)),
		TxMetas:      make([]*rpc.TransactionMeta, len(block.TxMetas)),
		Slot:         block.Slot,
		ParentSlot:   block.ParentSlot,
	}
	for i := range block.Transactions {
		clonedTx, cloneErr := cloneTransaction(block.Transactions[i])
		if cloneErr != nil {
			panic(fmt.Sprintf("unable to clone tx %s for unresolved block copy in slot %d: %v", block.Transactions[i].Signatures[0], block.Slot, cloneErr))
		}
		unresolvedBlock.Transactions[i] = clonedTx
		if unresolvedBlock.TxMetas != nil && !block.FromLightbringer {
			unresolvedBlock.TxMetas[i] = &rpc.TransactionMeta{}
			*(unresolvedBlock.TxMetas[i]) = *block.TxMetas[i]
		}
	}

	start = time.Now()
	setReplayStage("load_accounts")
	loadAcctsRegion := trace.StartRegion(ctx, "LoadBlockAccounts")
	accts, parentAccts, err := loadBlockAccountsAndUpdateSysvars(acctsDb, block, epochSchedule, alpenglowClock, spec)
	loadAcctsRegion.End()
	if err != nil {
		panic(fmt.Sprintf("unable to load slot accounts and update sysvars: %s", err))
	}
	metrics.GlobalBlockReplay.LoadBlockAccounts.AddTimingSince(start)

	slotCtx := newSlotCtx(block, accts, parentAccts, acctsDb)
	slotCtx.AccountLoader = func(parentSlot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
		return loadAccountForBlockReplay(acctsDb, spec, parentSlot, block.Slot, pubkey)
	}
	slotCtx.TraceCtx = ctx
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	var totalComputeUnitsConsumed uint64
	start = time.Now()

	setReplayStage("tx_loop")
	txLoopRegion := trace.StartRegion(ctx, "TxLoop")
	if txParallelism > 0 {
		txFeeAccumulator, totalComputeUnitsConsumed = parallelTxLoop(slotCtx, &sigverifyWg, unresolvedBlock, block, txParallelism, dbgOpts)
	} else {
		txFeeAccumulator, totalComputeUnitsConsumed = sequentialTxLoop(slotCtx, &sigverifyWg, block, dbgOpts)
	}
	slotCtx.TotalComputeUnitsConsumed = totalComputeUnitsConsumed
	txLoopRegion.End()
	metrics.GlobalBlockReplay.TxLoop.AddTimingSince(start)

	start = time.Now()
	setReplayStage("distribute_fees")

	// distribute tx fees to the slot leader
	// skip leader handling if there are zero transactions in this block
	loadFeeParent := feeParentAccountLoader(acctsDb, spec, slotCtx)
	if !global.ManageLeaderSchedule() && block.BlockReward != nil && len(block.Transactions) > 0 {
		feeDist := fees.DistributeTxFeesToSlotLeader(acctsDb, slotCtx, block.BlockReward.Leader, &txFeeAccumulator, loadFeeParent)
		slotCtx.LamportsBurnt = feeDist.LamportsBurnt
		if !feeDist.FeeCollector.IsZero() {
			slotCtx.RecordModifiedAcct(feeDist.FeeCollector)
		}
	} else if global.ManageLeaderSchedule() && len(block.Transactions) > 0 {
		feeDist := fees.DistributeTxFeesToSlotLeader(acctsDb, slotCtx, block.Leader, &txFeeAccumulator, loadFeeParent)
		slotCtx.LamportsBurnt = feeDist.LamportsBurnt
		if !feeDist.FeeCollector.IsZero() {
			slotCtx.RecordModifiedAcct(feeDist.FeeCollector)
		}
	}
	metrics.GlobalBlockReplay.Reward.AddTimingSince(start)

	start = time.Now()
	setReplayStage("collect_rent")
	rentSysvar := sealevel.SysvarCache.Rent.Sysvar
	rentAccts := rent.CollectRentEagerly(slotCtx, rentSysvar, epochSchedule)
	metrics.GlobalBlockReplay.Rent.AddTimingSince(start)

	start = time.Now()
	setReplayStage("run_incinerator")
	runIncinerator(slotCtx)
	metrics.GlobalBlockReplay.RunIncinerator.AddTimingSince(start)

	if alpenglowClock {
		if err := applyAlpenglowFooterClock(slotCtx, block, epochSchedule); err != nil {
			return nil, nil, err
		}
		if err := ApplyAlpenglowVoteRewards(acctsDb, spec, slotCtx, block, epochSchedule, block.SkipRewardCert, block.NotarRewardCert, block.BlockFinalCert, alpenglowShredVersion); err != nil {
			return nil, nil, err
		}
	}

	start = time.Now()
	setReplayStage("compile_accounts")
	writableAccts, modifiedAccts := compileWritableAndModifiedAccts(slotCtx, block, rentAccts, false)

	if err := ensureParentAcctsForModified(acctsDb, slotCtx, spec); err != nil {
		return nil, nil, err
	}

	start = time.Now()
	setReplayStage("bankhash")
	slotCtx.FinalBankhash = bankhash.CalculateBankHash(slotCtx, writableAccts, modifiedAccts, block.ParentBankhash, block.NumSignatures, block.Blockhash)
	metrics.GlobalBlockReplay.BankHash.AddTimingSince(start)

	if alpenglowClock {
		if err := verifyAlpenglowBlockFooter(slotCtx, block, alpenglowClock); err != nil {
			logFooterBankHashMismatchDiagnostics(slotCtx, block, modifiedAccts, spec, acctsDb.AcctsDir)
			return nil, nil, err
		}
	}

	// Bankhash consensus enforcement is handled in the replay loop (not here)
	// because forkchoice is fed after ProcessBlock returns — checking here would
	// never see votes from recently submitted blocks and could deadlock.

	if deferPersist {
		global.IncrTransactionCount(uint64(len(block.Transactions)))
		setReplayStage("done")
		return slotCtx, &DeferredBlockCommit{
			SlotCtx:                 slotCtx,
			ModifiedAccts:           modifiedAccts,
			BlockSlot:               block.Slot,
			BlockHeight:             block.BlockHeight,
			Bankhash:                append([]byte(nil), slotCtx.FinalBankhash...),
			HasAlpenglowBlockID:     block.HasAlpenglowBlockID,
			AlpenglowBlockID:        solana.Hash(block.AlpenglowBlockID),
			HasAlpenglowChainedRoot: block.HasAlpenglowLastChainedRoot,
			AlpenglowChainedRoot:    solana.Hash(block.AlpenglowLastChainedRoot),
		}, nil
	}

	// Enter critical commit window - panics here may leave AccountsDB inconsistent
	commitSlot.Store(slotCtx.Slot)
	commitInProgress.Store(true)
	start = time.Now()
	setReplayStage("store_accounts")
	persistedSlot := slotCtx.Slot
	persistedBankhash := append([]byte(nil), slotCtx.FinalBankhash...)
	persistedBlockSlot := block.Slot
	stakeIndexDir := filepath.Join(acctsDb.AcctsDir, "..")
	afterStoreAccounts := func() {
		metrics.GlobalBlockReplay.BlockUpdateAccounts.AddTimingSince(start)
		err := acctsDb.StoreBankHashForSlot(persistedSlot, persistedBankhash)
		if err != nil {
			mlog.Log.Infof("unable to store bankhash for slot %d", persistedSlot)
		}
		flushed, err := global.FlushPendingStakePubkeys(stakeIndexDir)
		if err != nil {
			mlog.Log.Errorf("failed to flush stake pubkey index: %v", err)
		} else if flushed > 0 {
			mlog.Log.Debugf("flushed %d new stake pubkeys to index", flushed)
		}

		pt.Set(persistedBlockSlot, persistedBankhash)

		// Exit critical commit window - AccountsDB is now consistent
		commitInProgress.Store(false)
		commitSlot.Store(0)
	}

	if len(modifiedAccts) > 0 {
		err = acctsDb.StoreAccounts(modifiedAccts, slotCtx.Slot, afterStoreAccounts)
		if err != nil {
			commitInProgress.Store(false)
			commitSlot.Store(0)
		}
	} else {
		afterStoreAccounts()
	}

	global.IncrTransactionCount(uint64(len(block.Transactions)))
	setReplayStage("done")
	return slotCtx, nil, err
}
