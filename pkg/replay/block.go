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
	"os"
	"path/filepath"
	"runtime"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
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
	// ParentBlockHeight is the block height of the last successfully replayed block.
	ParentBlockHeight uint64
	// ParentBankhash is the bankhash of the parent slot
	ParentBankhash []byte
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

	// Clock is the Clock sysvar data as-of the parent slot. Read from durable (not
	// SysvarCache) during load, so resume must restore it explicitly. nil = fresh start.
	Clock []byte

	// ReplayCtx fields - so resume uses fresh values instead of stale manifest
	Capitalization          uint64
	SlotsPerYear            float64
	InflationInitial        float64
	InflationTerminal       float64
	InflationTaper          float64
	InflationFoundation     float64
	InflationFoundationTerm float64
	// TransactionCount as of the resume slot. nil = the source context predates
	// the field (seed from the snapshot manifest, approximate); non-nil is exact.
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

func resolveAddrTableLookups(accountsDb blockAccountSource, block *b.Block) error {
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
	accts, err := accountsDb.GetAccountsBatch(context.Background(), block.Slot, tablesSlice)
	if err != nil {
		return err
	}

	for i := range tablesSlice {
		if len(accts[i].Data) == 0 {
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
	accts, err := accountsDb.GetAccountsBatch(ctx, slot, tableIDs)
	if err != nil {
		return err
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

	{
		acct, err := acctsDb.GetAccount(0, sealevel.SysvarFeesAddr)
		if errors.Is(err, accountsdb.ErrNoAccount) {
			// Alpenglow removes the Fees sysvar; absence is expected there.
		} else if err != nil {
			panic("unable to get fees sysvar when caching sysvars")
		} else {
			var fees sealevel.SysvarFees
			decoder := bin.NewBinDecoder(acct.Data)
			fees.MustUnmarshalWithDecoder(decoder)
			sealevel.SysvarCache.Fees.Sysvar = &fees
			sealevel.SysvarCache.Fees.Acct = acct
		}
	}

	{
		acct, err := acctsDb.GetAccount(0, sealevel.SysvarEpochRewardsAddr)
		if err != nil {
			panic("unable to get fees sysvar when caching sysvars")
		}
		var rewards sealevel.SysvarEpochRewards
		decoder := bin.NewBinDecoder(acct.Data)
		rewards.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.EpochRewards.Sysvar = &rewards
		sealevel.SysvarCache.EpochRewards.Acct = acct
	}
}

func loadBlockAccountsAndUpdateSysvars(accountsDb blockAccountSource, block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule, alpenglowClock bool) (accounts.Accounts, accounts.Accounts, error) {
	err := resolveAddrTableLookups(accountsDb, block)
	if err != nil {
		return nil, nil, err
	}

	dedupedAccts := extractAndDedupeBlockAccts(block)
	ctx := context.Background()
	slotAccts, err := accountsDb.GetAccountsBatch(ctx, block.Slot, dedupedAccts)
	if err != nil {
		return nil, nil, err
	}

	numAccts := uint64(len(slotAccts))
	parentAccts := accounts.NewMemAccountsWithLen(numAccts)
	for _, acct := range slotAccts {
		if err = parentAccts.SetAccountWithoutLock(acct.Key, acct); err != nil {
			return nil, nil, err
		}
	}

	// accts is a branch-local overlay over the pristine parent snapshot; execution
	// copy-on-writes, so parentAccts stays pristine for LtHash "before" values.
	accts := accounts.NewOverlayAccounts(parentAccts)

	block.FeeRateGovernor = sealevel.NewFeeRateGovernorDerived(block.PrevFeeRateGovernor, block.PrevNumSignatures)
	if block.FeeRateGovernor.PrevLamportsPerSignature == 0 {
		block.FeeRateGovernor.PrevLamportsPerSignature = block.InitialPreviousLamportsPerSignature
	}

	// load sysvar accounts and assign them to the sysvar cache
	{
		// update and cache clock sysvar
		{
			var clockAcct *accounts.Account
			var err error
			if sealevel.SysvarCache.Clock.Acct != nil {
				// Prefer the in-RAM Clock (mirrors SlotHashes/RecentBlockhashes): on
				// resume it is the restored Clock as of the last rooted slot, which durable may not match.
				clockAcct = sealevel.SysvarCache.Clock.Acct.Clone()
			} else {
				clockAcct, err = accountsDb.GetAccount(block.Slot, sealevel.SysvarClockAddr)
				if err != nil {
					panic("unable to retrieve clock sysvar when updating clock")
				}
			}

			err = parentAccts.SetAccountWithoutLock(sealevel.SysvarClockAddr, clockAcct.Clone())
			if err != nil {
				panic("unable to set clock sysvar to accts")
			}

			decoder := bin.NewBinDecoder(clockAcct.Data)
			var clock sealevel.SysvarClock

			err = clock.UnmarshalWithDecoder(decoder)
			if err != nil {
				panic("unable to unmarshal clock sysvar")
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
			slotHashesAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarSlotHashesAddr)
			if err != nil {
				panic("unable to retrieve slothashes sysvar from acctsdb")
			}

			var slotHashes sealevel.SysvarSlotHashes

			if sealevel.SysvarCache.SlotHashes.Sysvar == nil {
				// Fresh start (first slot): unmarshal from AccountsDB
				decoder := bin.NewBinDecoder(slotHashesAcct.Data)
				err = slotHashes.UnmarshalWithDecoder(decoder)
				if err != nil {
					panic("unable to unmarshal slothashes sysvar")
				}

			} else {
				// SysvarCache already populated (either from resume state file or from previous slot).
				// The account data from AccountsDB may be stale (appendvec writes are not fsynced),
				// so overwrite it with the authoritative data from SysvarCache.
				// This ensures BPF programs reading the account data directly see correct values.
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
			recentBlockhashesAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarRecentBlockHashesAddr)
			if err != nil {
				panic("unable to get recentblockhashes")
			}

			if sealevel.SysvarCache.RecentBlockHashes.Sysvar == nil {
				// Fresh start (first slot): unmarshal from AccountsDB
				decoder := bin.NewBinDecoder(recentBlockhashesAcct.Data)
				var recentBlockhashes sealevel.SysvarRecentBlockhashes
				recentBlockhashes.MustUnmarshalWithDecoder(decoder)
				sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recentBlockhashes
				sealevel.SysvarCache.RecentBlockHashes.Acct = recentBlockhashesAcct

				// Debug: log the blockhash range on first load
				if len(recentBlockhashes) > 0 {
					mlog.Log.Infof("loaded RecentBlockhashes sysvar: %d entries, newest=%x, oldest=%x",
						len(recentBlockhashes), recentBlockhashes[0].Blockhash[:8], recentBlockhashes[len(recentBlockhashes)-1].Blockhash[:8])
				}
			} else {
				// SysvarCache already populated (either from resume state file or from previous slot).
				// The account data from AccountsDB may be stale (appendvec writes are not fsynced),
				// so overwrite it with the authoritative data from SysvarCache.
				// This ensures BPF programs reading the account data directly see correct values.
				recentBlockhashes := sealevel.SysvarCache.RecentBlockHashes.Sysvar
				newData := recentBlockhashes.MustMarshal()
				if len(newData) != len(recentBlockhashesAcct.Data) {
					panic(fmt.Sprintf("RecentBlockhashes data length mismatch: marshaled=%d, account=%d",
						len(newData), len(recentBlockhashesAcct.Data)))
				}
				copy(recentBlockhashesAcct.Data, newData)
				sealevel.SysvarCache.RecentBlockHashes.Acct = recentBlockhashesAcct
			}

			// Set parentAccts AFTER potential data correction to ensure LtHash delta is computed correctly
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
			slotHistoryAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarSlotHistoryAddr)
			if err != nil {
				panic("unable to get slothistory")
			}

			err = parentAccts.SetAccountWithoutLock(sealevel.SysvarSlotHistoryAddr, slotHistoryAcct.Clone())
			if err != nil {
				panic("unable to set slothistory sysvar to accts")
			}

			decoder := bin.NewBinDecoder(slotHistoryAcct.Data)
			var slotHistory sealevel.SysvarSlotHistory
			slotHistory.MustUnmarshalWithDecoder(decoder)
			sealevel.SysvarCache.SlotHistory.Sysvar = &slotHistory
			sealevel.SysvarCache.SlotHistory.Acct = slotHistoryAcct

			err = accts.SetAccountWithoutLock(sealevel.SysvarSlotHistoryAddr, slotHistoryAcct)
			if err != nil {
				panic("unable to set clock sysvar to accts")
			}
		}

		// cache StakeHistory sysvar
		{
			stakeHistoryAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarStakeHistoryAddr)
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
			lastRestartSlotAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarLastRestartSlotAddr)
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

	// LtHash: decode base64, restore with InitWithHash
	if mithrilState.ManifestAcctsLtHash != "" {
		ltHashBytes, err := base64.StdEncoding.DecodeString(mithrilState.ManifestAcctsLtHash)
		if err != nil {
			return fmt.Errorf("corrupted state file: failed to decode manifest_accts_lt_hash: %w", err)
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
	block.VoteTimestamps = lastSlotCtx.VoteTimestamps
	block.EpochStakesPerVoteAcct = lastSlotCtx.VoteAccts
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
		if epochSchedule.GetEpoch(block.Slot) == lastSlotCtx.Epoch {
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

		// Restore the Clock sysvar as of the last rooted slot so the first resumed slot uses it rather
		// than durable's (which can diverge). Lamports/owner are constant, so load
		// from durable and overwrite only the data.
		if len(resumeState.Clock) > 0 {
			clockAcct, err := acctsDb.GetAccount(block.Slot, sealevel.SysvarClockAddr)
			if err != nil {
				return fmt.Errorf("cannot resume: failed to load clock sysvar account: %w", err)
			}
			clockAcct.Data = make([]byte, len(resumeState.Clock))
			copy(clockAcct.Data, resumeState.Clock)
			var clock sealevel.SysvarClock
			if err := clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockAcct.Data)); err != nil {
				return fmt.Errorf("cannot resume: failed to decode restored clock sysvar: %w", err)
			}
			sealevel.SysvarCache.Clock.Sysvar = &clock
			sealevel.SysvarCache.Clock.Acct = clockAcct
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
	turbineAlpenglowAddr string,
	turbineIdentity ed25519.PrivateKey,
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
	// Fresh vote/stake dirty watermark for this run (gates the in-loop unwind).
	resetVoteStakeDirty()
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
	persistedHashes := &persistedTracker{}

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

	// Seed the running transaction count. On resume, the checkpoint carries the
	// exact count as of the last rooted slot; on a fresh start use the snapshot
	// manifest. Set (not increment) so a re-replay in the same process never
	// double-counts. A checkpoint from before the field existed can only be
	// seeded approximately (the folded snapshot→root span is unrecorded) —
	// warn rather than fail: the count is RPC/metadata, not consensus.
	{
		txCount, exact := resolveInitialTransactionCount(resumeState, mithrilState.ManifestTransactionCount)
		global.SetTransactionCount(txCount)
		if !exact && mithrilState.LastRootedSlot > mithrilState.SnapshotSlot {
			mlog.Log.Warnf("resume checkpoint at slot %d predates transaction-count tracking: transactionCount seeded from the snapshot (slot %d) and will read LOW by the folded span until the next re-bootstrap",
				mithrilState.LastRootedSlot, mithrilState.SnapshotSlot)
		}
	}
	isFirstSlotInEpoch := epochSchedule.FirstSlotInEpoch(currentEpoch) == startSlot
	// Alpenglow-only node: Alpenglow clock/feature/finality semantics apply
	// unconditionally (this binary targets Alpenglow clusters exclusively).
	replayCtx.CurrentFeatures, featuresActivatedInFirstSlot, parentFeaturesActivatedInFirstSlot = scanAndEnableFeatures(acctsDb, replayCtx, startSlot, isFirstSlotInEpoch)
	applyAlpenglowRuntimeFeatureOverrides(replayCtx.CurrentFeatures, startSlot)
	partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)

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
	// Alpenglow consensus engine (certificate-driven finality; nil-safe).
	var consensusEngine consensusengine.Engine
	if consensusOpts != nil {
		consensusEngine = consensusOpts.Engine
	}

	if consensusEngine != nil {
		// Certs must verify against their own epoch's validator set; without the
		// lookup the engine falls back to the latest set and cross-epoch certs
		// silently fail BLS (and the deferred-cert replay never triggers).
		if lookupSink, ok := consensusEngine.(consensusengine.AlpenglowEpochLookupSink); ok {
			lookupSink.SetAlpenglowEpochLookup(epochSchedule.GetEpoch)
		}
		installCachedAlpenglowValidatorSets(consensusEngine, currentEpoch)
	}

	// 100-slot summary window collectors ("full" = reconstructable-from-shreds,
	// Agave SlotMeta/is_full sense; detailed debugging stays in file logs).
	var statsCounter int
	var execTimes []float64 // seconds per executed block
	var cuValues []uint64   // CU per executed block
	var txnCounts []uint64  // transactions per executed block
	var shredSamples []shredSample
	var windowRepairedShreds int
	var windowRepairedSlots int
	var windowEmptyBlocks int
	var windowSkippedWithShreds int // skipped slots where the leader sent partial shreds
	var windowSwitches int          // certificate switches detected this window
	var windowSwitchInRAM int       // switches resolved by the in-RAM unwind
	var windowSwitchFallback int    // switches that fell back to rooted-checkpoint re-replay
	switchFallbackReasons := make(map[string]int)
	var promotionHolds int // iterations promotion was fully stalled while finality ran a chunk ahead
	windowStart := time.Now()
	var lastGCCount uint32
	var justCrossedEpochBoundary bool

	// Preallocate slices for 100 blocks
	const summaryInterval = 100
	execTimes = make([]float64, 0, summaryInterval)
	cuValues = make([]uint64, 0, summaryInterval)
	txnCounts = make([]uint64, 0, summaryInterval)
	shredSamples = make([]shredSample, 0, summaryInterval)

	var lastRootedWatermark uint64 // highest certificate/delegated finality slot seen
	var highestExecutedSlot uint64 // highest slot ProcessBlock has executed; bounds the promotion-gate walk
	// While partitioned rewards distribute, promotion holds below the boundary
	// block so a crash-resume always re-runs it (the distribution bookkeeping is
	// RAM-only and not reconstructible mid-window). Self-clears when the window
	// completes (NumRewardPartitionsRemaining reaches 0).
	var rewardsHoldBelowSlot uint64
	// Alpenglow finality identities captured at observe/ingest time for the promotion
	// gate (the tracker's own state may be pruned by promotion time). Pruned as slots
	// promote; bounded by the unrooted tail cap.
	alpenglowFooterFinalized := make(map[uint64]solana.Hash)
	alpenglowExecutedBlockIDs := make(map[uint64]solana.Hash)
	var gateStats alpenglowGateStats
	// Persisted gate evidence: a previously-disputed slot promotes only on an exact
	// executed match after restart — never under delegated trust.
	alpenglowForced := make(map[uint64]solana.Hash)
	for _, ev := range mithrilState.AlpenglowEvidence {
		if ev.Slot <= mithrilState.LastRootedSlot {
			continue
		}
		var want solana.Hash
		if !ev.Conflict {
			raw, err := hex.DecodeString(ev.Finalized)
			if err != nil || len(raw) != 32 {
				mlog.Log.Warnf("alpenglow evidence for slot %d has a malformed finalized id; treating as conflict", ev.Slot)
			} else {
				copy(want[:], raw)
			}
		}
		alpenglowForced[ev.Slot] = want
		mlog.Log.Warnf("alpenglow evidence loaded: slot %d requires exact finality match before promotion", ev.Slot)
	}
	// Alpenglow delegated finality: an unstaked observer can't receive Votor certs,
	// so finality is attested by the RPC's finalized commitment (polled + throttled).
	var delegatedFinalizedSlot uint64
	var delegatedFinalizedAt time.Time

	// unrootedTailState holds the in-RAM speculative state — the working set plus
	// its per-slot undo journal — in rooted-durable mode (the only mode of the
	// Alpenglow build). It buffers replayed slots and folds them to disk via
	// CommitBatch once rooted; block reads resolve through it.
	var unrootedTailState *unrootedTail
	var promoter *asyncPromoter
	var applyFoldOutcome func(res *foldResult) // assigned below, used by the exit drain
	if acctsDb.RootedDurable {
		unrootedTailState = newUnrootedTail(acctsDb, acctsDb, unrootedTailHaltCap, FoldBatchSlots, filepath.Join(acctsDb.AcctsDir, ".."))
		// Folds run on a worker goroutine so replay never stalls on the
		// segment write + fsync; the loop builds jobs and applies completions.
		promoter = newAsyncPromoter(acctsDb)
		defer func() {
			// Settle the worker on ANY exit: apply a completed fold so the
			// in-process recovery retry resumes from the true durable frontier
			// instead of re-folding it (a discarded-but-committed fold is
			// still safe — RecoverFoldState reconciles — just wasteful).
			if applyFoldOutcome != nil {
				applyFoldOutcome(promoter.drain())
			}
			promoter.stop()
		}()
		mlog.Log.Infof("rooted-durable mode: canonical store stays rooted-only; replayed slots buffer in RAM until rooted (halt cap %d slots); folds run async off the replay loop", unrootedTailHaltCap)
	}
	// Any stake-index entries still pending from a previous in-process replay
	// attempt (rooted-checkpoint re-replay after a fork switch or finality
	// mismatch) belong to slots this run re-executes — or to a discarded wrong
	// fork. Either way they re-enqueue if real; stale ones must not leak.
	global.ClearPendingStakePubkeys()

	// Trailing execution verifier: the dual-watermark's second leg. Runs on
	// its own RPC client + budget so it never competes with block fetch.
	var trailingVerifier *TrailingVerifier
	replayDivergenceFloor := uint64(0)
	for _, ev := range mithrilState.ReplayDivergenceEvidence {
		if replayDivergenceFloor == 0 || ev.Slot < replayDivergenceFloor {
			replayDivergenceFloor = ev.Slot
		}
	}
	if replayDivergenceFloor > 0 {
		mlog.Log.Warnf("replay divergence evidence present (earliest slot %d): folds are blocked at that slot until the evidence is cleared after triage", replayDivergenceFloor)
	}
	// Switch sweep: detects executed slots contradicted by later decisive
	// certificates (wrong sibling / certified skip) under execute-on-receipt.
	switchSweeper := newAlpenglowSwitchSweeper(consensusEngine)

	if TrailingVerifierCfg.Enabled && unrootedTailState != nil {
		trailingVerifier = newTrailingVerifier(&rpcVerificationSource{rpcc: rpcclient.NewRpcClient(rpcEndpoints[0])}, TrailingVerifierCfg)
		go trailingVerifier.Run(ctx)
		// Publish a run-local tx-capture registry for the verifier's lifetime and
		// unpublish it on return, so no other run (a later re-replay, a test, a
		// sim) shares or inherits this run's capture state.
		stopCapture := beginTxCapture()
		defer stopCapture()
		if !TrailingVerifierCfg.Required {
			mlog.Log.Warnf("trailing verifier running in ADVISORY mode (verifier.required=false): folds are NOT gated on execution verification")
		}
		mlog.Log.Infof("trailing verifier active: lag=%d slots, budget=%d rps — folds gate on min(finality, verified)", TrailingVerifierCfg.LagSlots, TrailingVerifierCfg.MaxRPS)
	} else if unrootedTailState != nil {
		mlog.Log.Warnf("trailing verifier DISABLED: folds gate on certificate finality only — certificates attest block data, not execution; a mithril-side execution divergence would fold to disk undetected")
	}

	// applyPromotionBookkeeping advances the durable watermark and prunes every
	// per-slot structure bounded by it. Shared by async fold application, the
	// shutdown flush, and nothing else — it is the ONLY place LastRootedSlot
	// advances during replay.
	applyPromotionBookkeeping := func(promotedThrough uint64, rootedCtx *state.ResumeContext) {
		mithrilState.LastRootedSlot = promotedThrough
		mithrilState.LastRootedBankhash = rootedCtx.Bankhash
		mithrilState.LastRootedContext = rootedCtx
		for slot := range alpenglowFooterFinalized {
			if slot <= promotedThrough {
				delete(alpenglowFooterFinalized, slot)
			}
		}
		for slot := range alpenglowExecutedBlockIDs {
			if slot <= promotedThrough {
				delete(alpenglowExecutedBlockIDs, slot)
			}
		}
		if trailingVerifier != nil {
			trailingVerifier.PruneThrough(promotedThrough)
		}
		if pruner, ok := consensusEngine.(consensusengine.AlpenglowPruneSink); ok {
			pruner.PruneAlpenglowBefore(promotedThrough)
		}
		// Disputed slots that promoted passed the exact-match requirement —
		// the evidence is satisfied.
		for slot := range alpenglowForced {
			if slot <= promotedThrough {
				delete(alpenglowForced, slot)
				clearAlpenglowEvidence(mithrilState, slot)
			}
		}
	}

	// applyFoldOutcome applies a completed async fold on the loop thread. A
	// failed fold only logs: LastRootedSlot did not advance, so the next
	// iteration rebuilds the same chunk (natural retry); a permanently broken
	// store surfaces as the OverCap halt (fail-closed).
	applyFoldOutcome = func(res *foldResult) {
		if res == nil {
			return
		}
		if res.err != nil {
			mlog.Log.Errorf("rooted-durable: async fold failed: %v", res.err)
			return
		}
		rootedCtx := unrootedTailState.applyFoldJob(res.job)
		if rootedCtx == nil {
			mlog.Log.Errorf("rooted-durable: fold through slot %d returned no resume context; watermark held back", res.job.through)
			return
		}
		applyPromotionBookkeeping(res.job.through, rootedCtx)
	}

	// foldRootedPrefix folds the rooted RAM prefix onto disk up to the SAFE
	// target = min(certificate finality, trailing-verification watermark), after
	// the persisted-divergence floor and the Alpenglow exact-block-id gate. It is
	// the SINGLE fold path shared by in-loop promotion and the graceful-shutdown
	// flush, so shutdown can never fold a slot the loop would refuse. It runs on
	// every loop iteration (not only when finality advances) so verified progress
	// alone can advance the watermark, and it checks the verifier for a divergence
	// unconditionally so a failure halts even while finality is flat. Returns true
	// when the caller must halt (result.Error is already set). force=true
	// force-folds the trailing partial chunk (shutdown); force=false folds full
	// chunks only.
	foldRootedPrefix := func(force bool) (halt bool) {
		if unrootedTailState == nil {
			return false
		}
		// Apply any completed async fold first so the gates below see the
		// current durable frontier.
		applyFoldOutcome(promoter.poll())
		if lastRootedWatermark == 0 {
			return false
		}
		// The trailing verifier is the only execution-correctness oracle; a
		// divergence halts regardless of finality progress.
		if trailingVerifier != nil {
			if div := trailingVerifier.Failure(); div != nil {
				recordReplayDivergenceEvidence(mithrilState, div)
				if result.Error == nil {
					result.Error = fmt.Errorf("REPLAY DIVERGENCE (verified vs RPC): %w; halting — durable state remains at slot %d", div, mithrilState.LastRootedSlot)
				}
				mlog.Log.Errorf("REPLAY DIVERGENCE (verified vs RPC): %v; durable state remains at slot %d", div, mithrilState.LastRootedSlot)
				return true
			}
		}
		// Dual watermark: nothing folds unless BOTH certificate finality AND the
		// trailing verifier cover it (certificates attest block data, not
		// execution), and never at or past a persisted-divergence floor.
		verifierRequired := trailingVerifier != nil && TrailingVerifierCfg.Required
		verifiedWM := uint64(0)
		if verifierRequired {
			verifiedWM = trailingVerifier.VerifiedWatermark()
		}
		promoteThrough := safePromoteTarget(lastRootedWatermark, verifierRequired, verifiedWM, replayDivergenceFloor)
		// Partitioned-rewards window: hold promotion below the boundary block
		// until every partition distributes, so a crash-resume re-runs the
		// boundary and rebuilds the RAM-only distribution bookkeeping.
		if rewardsHoldBelowSlot > 0 && partitionedRewardsInfo != nil && partitionedRewardsInfo.NumRewardPartitionsRemaining > 0 {
			if promoteThrough >= rewardsHoldBelowSlot {
				promoteThrough = rewardsHoldBelowSlot - 1
			}
		}
		if promoteThrough <= mithrilState.LastRootedSlot {
			// Operator signal: promotion is fully stalled (verifier lag,
			// divergence floor, or rewards hold) while finality has run at
			// least a whole fold chunk ahead. Healthy steady state stays 0.
			if lastRootedWatermark >= mithrilState.LastRootedSlot+uint64(FoldBatchSlots) {
				promotionHolds++
			}
			return false // nothing new is both final and verified
		}
		// Alpenglow: never fold a slot whose executed block contradicts certificate
		// finality (prefix-stop; equivocation fails closed).
		if consensusEngine != nil {
			gated, gerr := alpenglowPromotionGate(consensusEngine,
				alpenglowFooterFinalized, alpenglowExecutedBlockIDs, alpenglowForced,
				mithrilState.LastRootedSlot, promoteThrough, highestExecutedSlot, &gateStats)
			promoteThrough = gated
			if gerr != nil {
				var mismatch *AlpenglowFinalityMismatch
				if errors.As(gerr, &mismatch) {
					recordAlpenglowEvidence(mithrilState, mismatch)
				}
				if result.Error == nil {
					result.Error = fmt.Errorf("ALPENGLOW SAFETY: %w; halting before folding slot %d", gerr, gated+1)
				}
				mlog.Log.Errorf("ALPENGLOW SAFETY: %v; halting before folding slot %d", gerr, gated+1)
				return true
			}
			mlog.Log.FileOnlyf("alpenglow gate: checked=%d matched=%d no_finality=%d no_local_id=%d",
				gateStats.checked, gateStats.matched, gateStats.noFinality, gateStats.noLocalID)
		}
		if promoteThrough <= mithrilState.LastRootedSlot {
			return false
		}

		if force {
			// Shutdown flush: settle the worker first, then fold everything
			// (including the trailing partial chunk) synchronously through the
			// SAME gate-derived target — shutdown can never fold a slot the
			// loop would refuse.
			if res := promoter.drain(); res != nil {
				applyFoldOutcome(res)
			}
			promotedThrough, rootedCtx, perr := unrootedTailState.flush(promoteThrough)
			if perr != nil {
				mlog.Log.Errorf("rooted-durable: shutdown flush stopped at slot %d: %v", promotedThrough, perr)
			}
			if promotedThrough > mithrilState.LastRootedSlot && rootedCtx != nil {
				applyPromotionBookkeeping(promotedThrough, rootedCtx)
			}
			return false
		}
		// Async: one chunk in flight at a time. Enqueue the next chunk only
		// when idle; completions are applied at the top of this function on a
		// later iteration.
		if !promoter.inFlight {
			job, jerr := unrootedTailState.buildFoldJob(promoteThrough, false)
			if jerr != nil {
				mlog.Log.Errorf("rooted-durable: %v; watermark held back", jerr)
				return false
			}
			if job != nil {
				promoter.enqueue(job)
			}
		}
		return false
	}

	var opts *blockstream.BlockSourceOpts
	if useLightbringer {
		opts = &blockstream.BlockSourceOpts{
			SourceType:           blockstream.BlockSourceLightbringer,
			RpcClient:            rpcc,
			LightbringerEndpoint: lightbringerEndpoint,
			BackupRpcEndpoints:   rpcBackups,
			StartSlot:            startSlot,
			EndSlot:              endSlot,
			BlockDir:             blockDir,
		}
	} else if useTurbine {
		opts = &blockstream.BlockSourceOpts{
			SourceType:              blockstream.BlockSourceTurbine,
			RpcClient:               rpcc,
			TurbineBindAddr:         turbineBindAddr,
			TurbineGossipEntrypoint: turbineGossipEntrypoint,
			TurbineGossipBindAddr:   turbineGossipBindAddr,
			TurbineAdvertisedIP:     turbineAdvertisedIP,
			TurbineShredVersion:     turbineShredVersion,
			LeaderForSlot:           global.LeaderForSlot,
			BackupRpcEndpoints:      rpcBackups,
			StartSlot:               startSlot,
			EndSlot:                 endSlot,
			BlockDir:                blockDir,
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

	// Alpenglow: drive cert-based block/skip selection at the block source from the
	// engine's ChainTracker when running native turbine in observer mode. Without this
	// the decision source is nil and applyAlpenglowDecisionLocked is a no-op.
	if useTurbine && consensusEngine != nil {
		if ds, ok := consensusEngine.(consensusengine.AlpenglowDecisionSource); ok {
			opts.TurbineAlpenglowBlockIDHints = true
			opts.AlpenglowDecisionSource = ds.NextAlpenglowDecision
		}
		// Candidate observations (block-id + parent link) feed the tracker's
		// ancestry and duplicate accounting before emission.
		if co, ok := consensusEngine.(consensusengine.AlpenglowCandidateBlockObserver); ok {
			opts.AlpenglowCandidateBlockSink = co.ObserveAlpenglowCandidateBlock
		}
		// Cert-driven repair: the source's repair loop steers turbine toward
		// certified-but-unobserved blocks and cancels shred state for
		// certificate-skipped slots.
		if wb, ok := consensusEngine.(consensusengine.AlpenglowWantedBlocksSource); ok {
			opts.AlpenglowWantedBlocks = wb.AlpenglowWantedBlocks
			opts.AlpenglowSkipCertified = wb.SkipCertifiedAt
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
	}
	blockStream := blockstream.NewBlockSource(opts)

	if !isLive {
		blockStream.DownloadInitialBlocks()
	}
	go blockStream.Start()

	var skippedSlotsCount int // Track skipped slots for 100-slot summary
	replayStartLogged := false

	currentExecutedAnchorSlot := func() uint64 {
		if lastSlotCtx != nil {
			return lastSlotCtx.Slot
		}
		if resumeState != nil {
			return resumeState.ParentSlot
		}
		return mithrilState.ManifestParentSlot
	}

	for {
		if ctx.Err() != nil {
			mlog.Log.Infof("context cancelled, stopping replay: %v", ctx.Err())
			result.WasCancelled = true
			break
		}

		var (
			block    *b.Block
			waitTime time.Duration
			neededAt time.Time // when replay asked the source for this slot
		)

		{
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

			neededAt = time.Now()
			block = blockStream.NextBlock()
			waitTime = time.Since(neededAt)

			if stallDone != nil {
				close(stallDone)
			}

			if block == nil {
				if result.Error == nil {
					switch {
					case blockStream.Stalled():
						result.Error = fmt.Errorf("block fetch stalled - no progress for %v", blockStream.StallTimeout())
					case isLive && !blockStream.Completed():
						result.Error = fmt.Errorf("block source stopped unexpectedly: %s", blockStream.StopReason())
					}
				}
				break
			}

			if anchorSlot := currentExecutedAnchorSlot(); anchorSlot != 0 && block.Slot <= anchorSlot {
				mlog.Log.Warnf("replay: discarding stale block source emission for slot %d; already executed through slot %d",
					block.Slot, anchorSlot)
				continue
			}

			// Alpenglow: feed the observed block to the consensus engine. Observer
			// telemetry must never break replay, so log-and-continue on error.
			if consensusEngine != nil {
				if err := consensusEngine.ObserveBlock(ctx, consensusengine.BlockObservation{
					Block:  block,
					Source: blockStream.GetFetchStats().CurrentSource,
					At:     time.Now(),
				}); err != nil {
					mlog.Log.Warnf("consensus engine observe block %d: %v", block.Slot, err)
				}
				if len(block.AlpenglowFinalCert) > 0 {
					finalized := ingestAlpenglowFooterCertificate(consensusEngine, block.AlpenglowFinalCert)
					if unrootedTailState != nil { // gate capture: only meaningful (and pruned) on the promotion path
						for _, fin := range finalized {
							alpenglowFooterFinalized[fin.Slot] = fin.Hash
						}
					}
				}
			}

			// Execute-on-receipt correction: certificates arriving after a slot
			// executed can name a different outcome. The sweep reports the first
			// contradiction. The COMMON path resolves it in RAM: evict the wrong
			// suffix from the WorkingSet, rebuild execution state from the
			// retained parent context, and continue the loop. Guarded cases
			// (reasons below) surface a typed error instead and the node-level
			// recovery loop re-replays from the rooted checkpoint (repair
			// re-fetches the certified version either way).
			if unrootedTailState != nil {
				if sw := switchSweeper.sweep(alpenglowExecutedBlockIDs, mithrilState.LastRootedSlot, currentExecutedAnchorSlot()); sw != nil {
					windowSwitches++
					blockStream.RewindForAlpenglowSwitch(sw.Slot, sw.Certified)
					// Settle the in-flight fold before touching the overlay: the
					// worker reads chunk layers the unwind would evict. If the
					// applied fold moved the durable frontier past the switch
					// slot, the contradiction is now at/below durable state —
					// that is the node-level rewind/recovery path, not an
					// in-RAM unwind.
					applyFoldOutcome(promoter.drain())
					if sw.Slot <= mithrilState.LastRootedSlot {
						windowSwitchFallback++
						switchFallbackReasons["durable-overlap"]++
						result.Error = sw
						mlog.Log.Warnf("%v — switch slot is at/below the durable watermark %d after settling the in-flight fold; deferring to the recovery loop", sw, mithrilState.LastRootedSlot)
						break
					}
					rs, fallbackReason := tryInLoopUnwind(sw, unrootedTailState, mithrilState, epochSchedule, currentEpoch, partitionedRewardsInfo)
					if rs != nil {
						// In-RAM unwind: drop the wrong suffix, rebuild execution state
						// from the retained parent context, and let the certified
						// version re-execute — no process restart, cost = the unwound
						// blocks' re-execution.
						for slot := range alpenglowExecutedBlockIDs {
							if slot >= sw.Slot {
								delete(alpenglowExecutedBlockIDs, slot)
							}
						}
						resumeState = rs
						lastSlotCtx = nil // next block configures from the rebuilt resume context
						replayCtx.Capitalization = rs.Capitalization
						global.SetBlockHeight(rs.ParentBlockHeight)
						if rs.TransactionCount != nil {
							global.SetTransactionCount(*rs.TransactionCount) // drop the discarded fork's txs
						}
						blockStream.SetLastExecutedSlot(sw.Slot - 1)
						windowSwitchInRAM++
						mlog.Log.Warnf("%v — unwound in RAM to slot %d; re-executing the certified chain (in-RAM switches this window: %d)", sw, sw.Slot-1, windowSwitchInRAM)
						continue
					}
					// Guarded out: fall back to the rooted-checkpoint re-replay,
					// recording WHY (the signal for whether the in-RAM engine
					// suffices or FD-style branching is actually needed).
					windowSwitchFallback++
					switchFallbackReasons[fallbackReason]++
					result.Error = sw
					mlog.Log.Warnf("%v — in-RAM unwind unavailable (%s); re-replaying the certified chain from the rooted checkpoint", sw, fallbackReason)
					break
				}
			}

			// Advance the certificate-finality watermark. Prefer engine
			// cert-finality; fall back to the RPC-attested finalized slot
			// (delegated) since an unstaked observer gets no certs. Poll
			// throttled (the RPC round-trip is slow).
			{
				rooted, ok := uint64(0), false
				if certRooted, certOk := alpenglowRootedSlot(consensusEngine); certOk {
					rooted, ok = certRooted, true
				} else {
					if time.Since(delegatedFinalizedAt) > 2*time.Second {
						// Record the attempt time regardless of outcome, so an RPC
						// outage doesn't re-issue a blocking poll on every block.
						delegatedFinalizedAt = time.Now()
						if fin, err := rpcc.GetSlotWithTimeoutAndCommitment(15*time.Second, rpc.CommitmentFinalized); err == nil {
							delegatedFinalizedSlot = fin
						}
					}
					if delegatedFinalizedSlot > 0 {
						rooted, ok = delegatedFinalizedSlot, true
					}
				}
				if ok && rooted > lastRootedWatermark {
					lastRootedWatermark = rooted
					mlog.Log.Infof("forkchoice: rooted watermark advanced to slot %d", rooted)
				}
			}
			// Fold the rooted RAM prefix onto disk (irreversible) through the
			// shared dual-watermark + Alpenglow gate. Runs every iteration so
			// verified progress alone advances the watermark and a verifier
			// divergence halts promptly even while finality is flat.
			if foldRootedPrefix(false) {
				break
			}

		}

		if block == nil {
			break
		}

		// Handle skipped slots - log and continue without execution
		if block.IsSkipped {
			// Look up leader for informational logging
			leaderStr := "unknown"
			if leader, exists := global.LeaderForSlot(block.Slot); exists {
				leaderStr = leader.String()
			}
			// Terminal: aligned skipped-slot line, reporting any PARTIAL shred
			// arrivals (leader sent something but the slot never became full).
			// Full detail (wait) stays in logs.
			partialShreds, repairedShreds, _, _ := blockStream.TurbineShredObservation(block.Slot)
			mlog.Log.InfofPrecise("%s", buildSkippedStatsLine(block.Slot, leaderStr, partialShreds, repairedShreds))
			if partialShreds > 0 {
				windowSkippedWithShreds++
			}
			mlog.Log.FileOnlyf("slot %d skipped | leader %s | wait %.3fs | partial shreds %d (repair %d)", block.Slot, leaderStr, waitTime.Seconds(), partialShreds, repairedShreds)
			skippedSlotsCount++
			if trailingVerifier != nil {
				trailingVerifier.RecordSkip(block.Slot)
			}
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
			applyAlpenglowRuntimeFeatureOverrides(replayCtx.CurrentFeatures, currentSlot)
			partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
			partitionedRewardsInfo = handleEpochTransition(acctsDb, partitionedEpochRewardsEnabled, lastSlotCtx, replayCtx, epochSchedule, replayCtx.CurrentFeatures, block, currentEpoch, rpcc, dbgOpts)
			currentEpoch = block.Epoch
			justCrossedEpochBoundary = true
			// While partitioned rewards are distributing, hold durable promotion
			// BELOW the boundary block: the distribution bookkeeping exists only
			// in RAM, so if the rooted watermark landed inside the window a crash
			// would resume past the boundary with no way to rebuild it — the
			// remaining partitions would be silently skipped and the first
			// re-executed distribution slot would diverge. Holding keeps the
			// boundary in the re-execution window so a resume rebuilds the
			// bookkeeping by re-running it. Costs tail RAM for the window length
			// (bounded by the OverCap halt if a huge stake count exceeds it —
			// fail-closed; reconstructing from the EpochRewards sysvar is the
			// eventual lift for mainnet-scale partition counts).
			if partitionedRewardsInfo != nil && partitionedRewardsInfo.NumRewardPartitionsRemaining > 0 {
				rewardsHoldBelowSlot = block.Slot
				mlog.Log.Infof("epoch boundary: holding durable promotion below slot %d until %d reward partitions distribute",
					block.Slot, partitionedRewardsInfo.NumRewardPartitionsRemaining)
			}

			// Persist the freshly computed epoch stakes NOW: the state file is
			// otherwise written only on graceful shutdown, so a hard crash any
			// time after the boundary would resume at R+1 in the new epoch with
			// no stakes for it — forcing a snapshot re-bootstrap and defeating
			// the manifest-recovery design. Once per epoch; atomic tmp+rename.
			if mithrilState != nil {
				if all := serializeAllEpochStakes(); len(all) > 0 {
					if mithrilState.ComputedEpochStakes == nil {
						mithrilState.ComputedEpochStakes = make(map[uint64]string, len(all))
					}
					for e, b := range all {
						mithrilState.ComputedEpochStakes[e] = string(b)
					}
					if serr := mithrilState.Save(acctsDbPath); serr != nil {
						mlog.Log.Errorf("failed to persist epoch %d stakes at the boundary (crash before next save would force re-bootstrap): %v", currentEpoch, serr)
					}
				}
			}

			// Alpenglow: reinstall the BLS validator set for the new epoch.
			if consensusEngine != nil {
				installAlpenglowValidatorSet(consensusEngine, currentEpoch)
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
		} else if lastSlotCtx == nil && partitionedEpochRewardsEnabled {
			// First block being processed - check if we're in rewards period
			// (uses lastSlotCtx == nil to detect first block, handles skipped startSlot)
			if rewards.IsWithinRewardsPeriod(block.Epoch, currentSlot, epochSchedule) {
				mlog.Log.Errorf("=======================================================")
				mlog.Log.Errorf("RESUME DURING REWARDS PERIOD NOT YET SUPPORTED")
				mlog.Log.Errorf("=======================================================")
				mlog.Log.Errorf("You stopped during the epoch reward distribution period")
				mlog.Log.Errorf("(first ~243 slots after epoch boundary).")
				mlog.Log.Errorf("")
				mlog.Log.Errorf("This will be supported in a future release.")
				mlog.Log.Errorf("")
				mlog.Log.Errorf("Workaround: Delete AccountsDB and restart from snapshot:")
				mlog.Log.Errorf("  rm -rf <accountsdb_dir>")
				mlog.Log.Errorf("  Set bootstrap.mode = 'new-snapshot' in config.toml")
				mlog.Log.Errorf("=======================================================")
				os.Exit(1)
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

		alpenglowClock := true // Alpenglow-only node: footer-clock semantics always on
		lastSlotCtx, err = ProcessBlock(acctsDb, block, epochSchedule, txParallelism, dbgOpts, persistedHashes, unrootedTailState, alpenglowClock)
		if err != nil {
			mlog.Log.Errorf("error encountered during block replay: %s\n", err)
			result.Error = err
			// Clear any pending stake pubkeys from this failed block
			global.ClearPendingStakePubkeys()
			break
		}

		// Rooted-durable backpressure: if the unrooted tail grew past its cap (rooting
		// stalled), halt rather than grow RAM unbounded; resume re-replays from the last rooted slot.
		if unrootedTailState != nil && unrootedTailState.OverCap() {
			result.Error = fmt.Errorf("rooted-durable: speculative state exceeded %d held slots/branches at slot %d (rooting stalled); halting", unrootedTailHaltCap, block.Slot)
			mlog.Log.Errorf("%v", result.Error)
			break
		}
		global.SetBlockHeight(block.BlockHeight)
		if trailingVerifier != nil {
			trailingVerifier.Record(buildSlotDigest(block))
		}
		if block.Slot > highestExecutedSlot {
			highestExecutedSlot = block.Slot // bounds the promotion-gate walk (shared fold path)
		}

		// Alpenglow: report the replayed slot's bankhash to the engine (drives cert
		// replay reconciliation). Log-and-continue — never break replay on telemetry.
		if consensusEngine != nil && lastSlotCtx != nil {
			// Record the executed identity here — execution is proven (a block
			// captured at observe time can still be discarded before it runs).
			if block.HasAlpenglowBlockID && unrootedTailState != nil {
				alpenglowExecutedBlockIDs[block.Slot] = solana.Hash(block.AlpenglowBlockID)
			}
			if err := consensusEngine.OnReplayResult(ctx, consensusengine.SlotReplayResult{
				Slot:     block.Slot,
				Bankhash: solana.HashFromBytes(lastSlotCtx.FinalBankhash),
				Source:   blockStream.GetFetchStats().CurrentSource,
				At:       time.Now(),
			}); err != nil {
				mlog.Log.Warnf("consensus engine replay result %d: %v", block.Slot, err)
			}
		}

		if rpcServer != nil {
			rpcServer.SetSlotCtx(lastSlotCtx)
		}

		replayCtx.Capitalization -= lastSlotCtx.LamportsBurnt

		// Rooted-durable: capture this slot's end-of-slot resume context (deep-copied,
		// no pointers into the global SysvarCache) and retain it in the tail until
		// promotion, so resume restarts from the last rooted slot not the lost in-RAM replayed tip.
		if unrootedTailState != nil && lastSlotCtx != nil {
			txCountAtSlot := global.TransactionCount() // ProcessBlock already added this block's txs
			resumeCtx := &state.ResumeContext{
				Slot:                    block.Slot,
				Bankhash:                base58.Encode(lastSlotCtx.FinalBankhash),
				BlockHeight:             global.BlockHeight(),
				Epoch:                   block.Epoch,
				NumSignatures:           lastSlotCtx.NumSignatures,
				EvictedBlockhash:        base58.Encode(lastSlotCtx.LatestEvictedBlockhash[:]),
				Blockhash:               base58.Encode(lastSlotCtx.Blockhash[:]),
				RecentBlockhashes:       EncodeRecentBlockhashes(sealevel.SysvarCache.RecentBlockHashes.Sysvar),
				SlotHashes:              EncodeSlotHashes(sealevel.SysvarCache.SlotHashes.Sysvar),
				Capitalization:          replayCtx.Capitalization,
				SlotsPerYear:            replayCtx.SlotsPerYear,
				InflationInitial:        replayCtx.Inflation.Initial,
				InflationTerminal:       replayCtx.Inflation.Terminal,
				InflationTaper:          replayCtx.Inflation.Taper,
				InflationFoundation:     replayCtx.Inflation.FoundationVal,
				InflationFoundationTerm: replayCtx.Inflation.FoundationTerm,
				TransactionCount:        &txCountAtSlot,
			}
			if lastSlotCtx.AcctsLtHash != nil {
				resumeCtx.AcctsLtHash = base64.StdEncoding.EncodeToString(lastSlotCtx.AcctsLtHash.Hash())
			}
			// Capture the Clock sysvar as of the last rooted slot: read from durable (not SysvarCache)
			// during load, so resume must restore it or the first slot's LtHash diverges.
			if sealevel.SysvarCache.Clock.Acct != nil {
				resumeCtx.Clock = base64.StdEncoding.EncodeToString(sealevel.SysvarCache.Clock.Acct.Data)
			}
			if lastSlotCtx.FeeRateGovernor != nil {
				resumeCtx.LamportsPerSignature = lastSlotCtx.FeeRateGovernor.LamportsPerSignature
				resumeCtx.PrevLamportsPerSig = lastSlotCtx.FeeRateGovernor.PrevLamportsPerSignature
			}
			unrootedTailState.SetContext(block.Slot, resumeCtx)
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

			acctsDb.WaitForStoreWorker()

			// Populate result immediately for state write
			result.LastPersistedSlot, result.LastPersistedBankhash = persistedHashes.Get()
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

		txnCount := len(block.Transactions)
		totalCU := lastSlotCtx.TotalComputeUnitsConsumed

		// Get leader from block (set by configureBlock in live mode, or by block source in verify mode)
		leaderStr := "unknown"
		if !block.Leader.IsZero() {
			leaderStr = block.Leader.String()
		}

		// Terminal: concise per-slot line. Shred timings only for shred-sourced
		// blocks (never fabricated for RPC/file). ready = assembly completion
		// minus when replay asked for the slot (negative: ready that long
		// early; positive: replay waited); asm = first shred -> full.
		execMsLine := slotReplayDuration.Seconds() * 1000
		hasShreds := block.ShredFirstNanos > 0 && block.ShredFullNanos > 0
		var readySecsLine, asmSecsLine float64
		if hasShreds {
			readySecsLine = float64(block.ShredFullNanos-neededAt.UnixNano()) / 1e9
			asmSecsLine = float64(block.ShredFullNanos-block.ShredFirstNanos) / 1e9
		}
		mlog.Log.InfofPrecise("%s", buildSlotStatsLine(block.Slot, leaderStr, txnCount, totalCU, execMsLine, hasShreds, readySecsLine, asmSecsLine, block.RepairedShreds))
		// Full detail (wait, vote split) stays in file logs for debugging.
		var voteTxCount int
		for _, tx := range block.Transactions {
			if tx.IsVote() {
				voteTxCount++
			}
		}
		mlog.Log.FileOnlyf("slot %d detail | leader %s | txns v:%d nv:%d | exec %.3fs | wait %.3fs | total %.3fs",
			block.Slot, leaderStr, voteTxCount, txnCount-voteTxCount, slotReplayDuration.Seconds(), waitTime.Seconds(), (waitTime + slotReplayDuration).Seconds())

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
			cuValues = append(cuValues, totalCU)
			txnCounts = append(txnCounts, uint64(txnCount))
			if txnCount == 0 {
				windowEmptyBlocks++
			}
			if hasShreds {
				shredSamples = append(shredSamples, shredSample{readySecs: readySecsLine, asmSecs: asmSecsLine})
				if block.RepairedShreds > 0 {
					windowRepairedSlots++
					windowRepairedShreds += block.RepairedShreds
				}
			}

			// Trigger async tip refresh 5 slots before summary so it's fresh when we print
			if statsCounter == summaryInterval-5 {
				blockStream.RefreshTipsForSummary()
			}

			if statsCounter == summaryInterval {
				fetchStats := blockStream.GetFetchStats()
				elapsed := time.Since(windowStart).Seconds()
				slotsPerSec := 0.0
				if elapsed > 0 {
					slotsPerSec = float64(statsCounter+skippedSlotsCount) / elapsed
				}

				execMs := make([]float64, len(execTimes))
				slowBlocks := 0
				for i, secs := range execTimes {
					execMs[i] = secs * 1000
					if execMs[i] > 200 {
						slowBlocks++
					}
				}
				effVals := make([]float64, 0, len(execMs))
				cuPerTx := make([]float64, 0, len(execMs))
				txF := make([]float64, 0, len(txnCounts))
				cuF := make([]float64, 0, len(cuValues))
				for i := range cuValues {
					cuF = append(cuF, float64(cuValues[i]))
					txF = append(txF, float64(txnCounts[i]))
					if cuValues[i] > 0 {
						effVals = append(effVals, execMs[i]/(float64(cuValues[i])/1e6))
					}
					if txnCounts[i] > 0 {
						cuPerTx = append(cuPerTx, float64(cuValues[i])/float64(txnCounts[i]))
					}
				}

				mlog.Log.InfofPrecise("")
				mlog.Log.InfofPrecise("=== 100 Slot Summary ===")
				mlog.Log.InfofPrecise("  source: %s", fetchStats.CurrentSource)

				// Shred gaps only when a turbine receiver is live — never fabricated
				// for RPC-only operation.
				progress := fmt.Sprintf("  progress: %.1f slots/sec", slotsPerSec)
				if latestShred, highestFull, edgesOK := blockStream.TurbineShredEdges(); edgesOK && latestShred > 0 {
					replayGap := int64(latestShred) - int64(block.Slot)
					fullGap := int64(latestShred) - int64(highestFull)
					if replayGap < 0 {
						replayGap = 0
					}
					if fullGap < 0 {
						fullGap = 0
					}
					progress += fmt.Sprintf(" | behind latest shred: replay %d, full %d", replayGap, fullGap)
				}
				if windowSkippedWithShreds > 0 {
					progress += fmt.Sprintf(" | skipped %d (%d with shreds) | empty blocks %d", skippedSlotsCount, windowSkippedWithShreds, windowEmptyBlocks)
				} else {
					progress += fmt.Sprintf(" | skipped %d | empty blocks %d", skippedSlotsCount, windowEmptyBlocks)
				}
				mlog.Log.InfofPrecise("%s", progress)

				finalizedStr := "--"
				if lastRootedWatermark > 0 {
					finalizedStr = fmt.Sprintf("%d", lastRootedWatermark)
				}
				if windowSwitches > 0 {
					mlog.Log.InfofPrecise("  consensus: finalized slot %s | switches %d (in-RAM %d, fallback %d)", finalizedStr, windowSwitches, windowSwitchInRAM, windowSwitchFallback)
					if len(switchFallbackReasons) > 0 {
						mlog.Log.FileOnlyf("switch fallback reasons this window: %v", switchFallbackReasons)
					}
				} else {
					mlog.Log.InfofPrecise("  consensus: finalized slot %s | switches 0", finalizedStr)
				}

				checkedStr := "--"
				if trailingVerifier != nil {
					if vw := trailingVerifier.VerifiedWatermark(); vw > 0 {
						checkedStr = fmt.Sprintf("%d", vw)
					}
				}
				mlog.Log.InfofPrecise("  safety: exec checked slot %s | holds %d", checkedStr, promotionHolds)

				if len(shredSamples) > 0 {
					ready := make([]float64, 0, len(shredSamples))
					asm := make([]float64, 0, len(shredSamples))
					for _, s := range shredSamples {
						ready = append(ready, s.readySecs)
						asm = append(asm, s.asmSecs)
					}
					mlog.Log.InfofPrecise("  shreds: ready median %+.1fs, worst %+.1fs (neg = assembled before replay needed it) | asm median %.1fs, max %.1fs",
						medianF(ready), maxF(ready), medianF(asm), maxF(asm))
					mlog.Log.InfofPrecise("  repair: %d slots, %d shreds", windowRepairedSlots, windowRepairedShreds)
				}

				mlog.Log.InfofPrecise("  txns: median %.0f | p90 %.0f | max %.0f | cu/tx median %s | p90 %s",
					medianF(txF), percentileF(txF, 90), maxF(txF), fmtK(medianF(cuPerTx)), fmtK(percentileF(cuPerTx, 90)))
				mlog.Log.InfofPrecise("  cu: median %s | p90 %s | max %s",
					fmtMcu(uint64(medianF(cuF))), fmtMcu(uint64(percentileF(cuF, 90))), fmtMcu(uint64(maxF(cuF))))
				mlog.Log.InfofPrecise("  execution: median %.0fms | p95 %.0fms | max %.0fms | >200ms %d",
					medianF(execMs), percentileF(execMs, 95), maxF(execMs), slowBlocks)
				mlog.Log.InfofPrecise("  efficiency: median %.1fms/Mcu | p95 %.1fms/Mcu | max %.1fms/Mcu",
					medianF(effVals), percentileF(effVals, 95), maxF(effVals))

				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				const gib = 1024 * 1024 * 1024
				gcDelta := mem.NumGC - lastGCCount
				lastGCCount = mem.NumGC
				resLine := "  resources:"
				if rss := processRSSBytes(); rss > 0 {
					resLine += fmt.Sprintf(" rss %.1fGiB |", float64(rss)/gib)
				}
				resLine += fmt.Sprintf(" heap %.1fGiB | heap inuse %.1fGiB | gc %d",
					float64(mem.HeapAlloc)/gib, float64(mem.HeapInuse)/gib, gcDelta)
				mlog.Log.InfofPrecise("%s", resLine)
				mlog.Log.InfofPrecise("")

				// Detailed debugging stays in file logs.
				cloneStats := GetAndResetCloneStats()
				if cloneStats.TxCount > 0 {
					var cloneRatio float64
					if cloneStats.AcctsLoaded > 0 {
						cloneRatio = float64(cloneStats.AcctsCloned) / float64(cloneStats.AcctsLoaded) * 100
					}
					mlog.Log.FileOnlyf("account COW: %.1f%% cloned (%d/%d accts) | %.1fMB loaded, %.1fMB cloned, %.1fMB modified",
						cloneRatio, cloneStats.AcctsCloned, cloneStats.AcctsLoaded,
						float64(cloneStats.AcctsLoadedBytes)/1024/1024, float64(cloneStats.AcctsClonedBytes)/1024/1024, float64(cloneStats.AcctsTouchedBytes)/1024/1024)
				}
				mlog.Log.FileOnlyf("memory detail: alloc %.1fGiB | inuse %.1fGiB | idle %.1fGiB | released %.1fGiB | next_gc %.1fGiB | objs %d | gc_total %d | store_queue %d",
					float64(mem.HeapAlloc)/gib, float64(mem.HeapInuse)/gib, float64(mem.HeapIdle)/gib,
					float64(mem.HeapReleased)/gib, float64(mem.NextGC)/gib, mem.HeapObjects, mem.NumGC, acctsDb.StoreQueueLen())
				if fetchStats.Attempts > 0 {
					retryRate := float64(fetchStats.Retries) / float64(fetchStats.Attempts) * 100
					mlog.Log.FileOnlyf("getBlock fetch: %.1f rps (%d calls) | avg %.0fms | %.0f%% success | retries %.1f%% | errs: na:%d rl:%d bt:%d tr:%d",
						fetchStats.GetBlockRPS, fetchStats.Attempts, fetchStats.AvgLatencyMs, fetchStats.SuccessRate, retryRate,
						fetchStats.ErrNotAvail, fetchStats.ErrRateLimit, fetchStats.ErrBeyondTip, fetchStats.ErrTransient)
					if fetchStats.TipStaleSecs > 30 || fetchStats.TotalTipPollFails > 0 {
						mlog.Log.InfofPrecise("  WARNING: tip stale %ds | tip poll fails: %d (consecutive: %d)",
							fetchStats.TipStaleSecs, fetchStats.TotalTipPollFails, fetchStats.TipPollFailures)
					}
					blockStream.ResetStats()
				}

				// Reset window collectors (reuse capacity)
				execTimes = execTimes[:0]
				cuValues = cuValues[:0]
				txnCounts = txnCounts[:0]
				shredSamples = shredSamples[:0]
				windowRepairedShreds = 0
				windowRepairedSlots = 0
				windowEmptyBlocks = 0
				windowSkippedWithShreds = 0
				windowSwitches = 0
				windowSwitchInRAM = 0
				windowSwitchFallback = 0
				clear(switchFallbackReasons)
				promotionHolds = 0
				windowStart = time.Now()
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

	// Check if block source stalled (this provides explicit error info)
	if blockStream.Stalled() && result.Error == nil {
		result.Error = fmt.Errorf("block fetch stalled - no progress for %v", blockStream.StallTimeout())
	}

	// Graceful shutdown: force-fold the trailing partial chunk of the rooted
	// prefix through the SAME dual-watermark + Alpenglow gate as the in-loop
	// path, so a Ctrl+C can never fold a slot normal promotion would refuse.
	// Bounds restart re-execution to the chunk size instead of chunk size plus
	// however long the partial ran.
	if unrootedTailState != nil {
		preFlushRooted := mithrilState.LastRootedSlot
		foldRootedPrefix(true)
		// The cancel-path state save ran BEFORE this flush; if the flush
		// advanced the watermark, re-save so the state file matches the store
		// exactly. (Without this, startup's store-ahead reconcile still adopts
		// the manifest context — this just keeps the file authoritative.)
		if result.StateWrittenOnCancel && onCancelWriteState != nil && mithrilState.LastRootedSlot > preFlushRooted {
			if err := onCancelWriteState(result); err != nil {
				mlog.Log.Errorf("failed to re-write state after shutdown flush (recovery reconcile will cover it): %v", err)
			}
		}
	}

	acctsDb.WaitForStoreWorker()
	result.LastPersistedSlot, result.LastPersistedBankhash = persistedHashes.Get()
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

	return result
}

func runIncinerator(slotCtx *sealevel.SlotCtx) {
	incineratorAcct, err := slotCtx.GetAccount(a.IncineratorAddr)
	if err != nil {
		return
	}
	newIncineratorAcct := &accounts.Account{Key: a.IncineratorAddr, Owner: a.SystemProgramAddr, RentEpoch: math.MaxUint64}
	slotCtx.SetAccount(a.IncineratorAddr, newIncineratorAcct)
	slotCtx.LamportsBurnt += incineratorAcct.Lamports
}

func compileWritableAndModifiedAccts(slotCtx *sealevel.SlotCtx, block *b.Block, rentAccts []*accounts.Account) ([]*accounts.Account, []*accounts.Account) {
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

	sysvarAccts := collectAndUpdateSysvarAcctsForAdh(slotCtx)
	writableAccts = append(writableAccts, sysvarAccts...)
	modifiedAccts = append(modifiedAccts, sysvarAccts...)

	return writableAccts, modifiedAccts
}

// EncodeRecentBlockhashes deep-copies the RecentBlockhashes sysvar into the
// serializable []state.BlockhashEntry form (newest-first). nil sysvar returns nil.
func EncodeRecentBlockhashes(sysvar *sealevel.SysvarRecentBlockhashes) []state.BlockhashEntry {
	if sysvar == nil {
		return nil
	}
	result := make([]state.BlockhashEntry, 0, len(*sysvar))
	for _, entry := range *sysvar {
		result = append(result, state.BlockhashEntry{
			Blockhash:            base58.Encode(entry.Blockhash[:]),
			LamportsPerSignature: entry.FeeCalculator.LamportsPerSignature,
		})
	}
	return result
}

// EncodeSlotHashes deep-copies the SlotHashes sysvar into the serializable
// []state.SlotHashEntry form. Returns nil for a nil sysvar.
func EncodeSlotHashes(sysvar *sealevel.SysvarSlotHashes) []state.SlotHashEntry {
	if sysvar == nil {
		return nil
	}
	result := make([]state.SlotHashEntry, 0, len(*sysvar))
	for _, entry := range *sysvar {
		result = append(result, state.SlotHashEntry{
			Slot: entry.Slot,
			Hash: base58.Encode(entry.Hash[:]),
		})
	}
	return result
}

func newSlotCtx(block *b.Block, accts accounts.Accounts, parentAccts accounts.Accounts, acctsDb *accountsdb.AccountsDb, tail unrootedState) *sealevel.SlotCtx {
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

		VoteAccts:       block.EpochStakesPerVoteAcct,
		VoteTimestampMu: &sync.Mutex{},
		VoteTimestamps:  block.VoteTimestamps,
		TotalEpochStake: block.TotalEpochStake,

		EahWorkaroundBankhash: block.EahWorkaroundBankhash,

		HasEahWorkaround: block.HasEahWorkaround,

		SerializedParameterArena: SerializedParameterArena,
	}
	// Guard: a nil *unrootedTail stored in the AccountReader interface would be a
	// non-nil typed-nil and break the nil check in GetAccountFromAccountsDb.
	if tail != nil {
		slotCtx.UnrootedRead = tail
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
	// persistedHashes is updated after StoreAccounts completes through a callback.
	// Must be non-nil.
	persistedHashes *persistedTracker,
	// tail is the in-RAM working set in rooted-durable mode; nil when rooted-
	// durable is off. When set, block reads resolve through it and commits
	// buffer into it.
	tail unrootedState,
	alpenglowClock bool,
) (*sealevel.SlotCtx, error) {
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
	// In rooted-durable mode, block accounts/sysvars load through the unrooted
	// tail (overlay→durable) so execution sees confirmed-but-unrooted state.
	var blockSrc blockAccountSource = acctsDb
	if tail != nil {
		blockSrc = tail
	}
	accts, parentAccts, err := loadBlockAccountsAndUpdateSysvars(blockSrc, block, epochSchedule, alpenglowClock)
	loadAcctsRegion.End()
	if err != nil {
		panic(fmt.Sprintf("unable to load slot accounts and update sysvars: %s", err))
	}
	metrics.GlobalBlockReplay.LoadBlockAccounts.AddTimingSince(start)

	slotCtx := newSlotCtx(block, accts, parentAccts, acctsDb, tail)
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
	if !global.ManageLeaderSchedule() && block.BlockReward != nil && len(block.Transactions) > 0 {
		slotCtx.LamportsBurnt = fees.DistributeTxFeesToSlotLeader(acctsDb, slotCtx, block.BlockReward.Leader, &txFeeAccumulator)
		slotCtx.RecordModifiedAcct(block.BlockReward.Leader)
	} else if global.ManageLeaderSchedule() && len(block.Transactions) > 0 {
		slotCtx.LamportsBurnt = fees.DistributeTxFeesToSlotLeader(acctsDb, slotCtx, block.Leader, &txFeeAccumulator)
		slotCtx.RecordModifiedAcct(block.Leader)
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

	// Alpenglow banks set the Clock timestamp from the block footer after execution.
	if alpenglowClock {
		if err := applyAlpenglowFooterClock(slotCtx, block, epochSchedule); err != nil {
			return nil, fmt.Errorf("apply alpenglow footer clock at slot %d: %w", block.Slot, err)
		}
	}

	start = time.Now()
	setReplayStage("compile_accounts")
	writableAccts, modifiedAccts := compileWritableAndModifiedAccts(slotCtx, block, rentAccts)

	start = time.Now()
	setReplayStage("bankhash")
	slotCtx.FinalBankhash = bankhash.CalculateBankHash(slotCtx, writableAccts, modifiedAccts, block.ParentBankhash, block.NumSignatures, block.Blockhash)
	metrics.GlobalBlockReplay.BankHash.AddTimingSince(start)

	// Bankhash consensus enforcement is handled in the replay loop (not here)
	// because forkchoice is fed after ProcessBlock returns — checking here would
	// never see votes from recently submitted blocks and could deadlock.

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
		if tail != nil {
			// Rooted-durable: accounts + bankhash are buffered in the overlay and
			// become durable only on promotion; nothing written here (rooted-only).
		} else {
			if berr := acctsDb.StoreBankHashForSlot(persistedSlot, persistedBankhash); berr != nil {
				mlog.Log.Infof("unable to store bankhash for slot %d", persistedSlot)
			}
		}
		if tail == nil {
			// Legacy/verify modes (no fork ambiguity): flush per block as before.
			// Rooted-durable replay flushes at FOLD time instead — entries stay
			// slot-scoped in RAM so a fork unwind can drop them, and scans merge
			// the pending set (StreamStakeAccounts) for completeness meanwhile.
			flushed, err := global.FlushPendingStakePubkeys(stakeIndexDir)
			if err != nil {
				mlog.Log.Errorf("failed to flush stake pubkey index: %v", err)
			} else if flushed > 0 {
				mlog.Log.Debugf("flushed %d new stake pubkeys to index", flushed)
			}
		}

		persistedHashes.Set(persistedBlockSlot, persistedBankhash)

		// Exit critical commit window - AccountsDB is now consistent
		commitInProgress.Store(false)
		commitSlot.Store(0)
	}

	if tail != nil {
		// Rooted-durable: buffer this slot's writes + bankhash in the RAM overlay
		// (always, even when empty, so the bankhash is recorded); no durable write.
		tail.Add(slotCtx.Slot, modifiedAccts, persistedBankhash)
		afterStoreAccounts()
	} else if len(modifiedAccts) > 0 {
		err = acctsDb.StoreAccounts(modifiedAccts, slotCtx.Slot, afterStoreAccounts)
	}

	global.IncrTransactionCount(uint64(len(block.Transactions)))
	setReplayStage("done")
	return slotCtx, err
}
