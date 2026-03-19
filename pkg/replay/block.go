package replay

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
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
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/forkchoice"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/panjf2000/ants/v2"
)

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

// ConsensusOpts contains vote-anchored consensus configuration.
// Nil means use defaults (max_depth=64, policy="halt").
type ConsensusOpts struct {
	SkipPathMaxDepth int    // Max slots for skip-path solver (default: 64)
	UnresolvedPolicy string // "halt" or "warn" (default: "halt")
	EnforceOnSource  string // "lightbringer" or "all" (default: "lightbringer")
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

// ReplayResult contains the result of a replay operation, including shutdown state
type ReplayResult struct {
	// LastPersistedSlot is the last slot whose state was successfully persisted to AccountsDB
	LastPersistedSlot uint64
	// LastPersistedBankhash is the bankhash of the last persisted slot
	LastPersistedBankhash []byte
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

	// ReplayCtx fields - so resume uses fresh values instead of stale manifest
	Capitalization          uint64
	SlotsPerYear            float64
	InflationInitial        float64
	InflationTerminal       float64
	InflationTaper          float64
	InflationFoundation     float64
	InflationFoundationTerm float64

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

func resolveAddrTableLookups(accountsDb *accountsdb.AccountsDb, block *b.Block) error {
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

func isSysvar(pubkey solana.PublicKey) bool {
	if pubkey == sealevel.SysvarClockAddr || pubkey == sealevel.SysvarEpochScheduleAddr ||
		pubkey == sealevel.SysvarFeesAddr || pubkey == sealevel.SysvarInstructionsAddr ||
		pubkey == sealevel.SysvarRecentBlockHashesAddr || pubkey == sealevel.SysvarRentAddr ||
		pubkey == a.SysvarRewardsAddr || pubkey == sealevel.SysvarSlotHashesAddr ||
		pubkey == sealevel.SysvarSlotHistoryAddr || pubkey == sealevel.SysvarStakeHistoryAddr {
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
		if err != nil {
			panic("unable to get fees sysvar when caching sysvars")
		}
		var fees sealevel.SysvarFees
		decoder := bin.NewBinDecoder(acct.Data)
		fees.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.Fees.Sysvar = &fees
		sealevel.SysvarCache.Fees.Acct = acct
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

func loadBlockAccountsAndUpdateSysvars(accountsDb *accountsdb.AccountsDb, block *b.Block) (accounts.Accounts, accounts.Accounts, error) {
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
			clockAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarClockAddr)
			if err != nil {
				panic("unable to retrieve clock sysvar when updating clock")
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

			err = updateClockSysvar(&clock, block)
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

func scanAndEnableFeatures(acctsDb *accountsdb.AccountsDb, slot uint64, startOfEpoch bool) (*features.Features, []*accounts.Account, []*accounts.Account) {
	parentAccts := make([]*accounts.Account, 0)
	modifiedAccts := make([]*accounts.Account, 0)

	f := features.NewFeaturesDefault()

	for _, featureGate := range features.AllFeatureGates {
		acct, err := acctsDb.GetAccount(slot, featureGate.Address)
		if err == nil {
			if acct.Owner != a.FeatureAddr {
				continue
			}
			parentAccts = append(parentAccts, acct.Clone())

			featureAcct := features.UnmarshalFeatureAcct(acct.Data)

			// already activated
			if featureAcct.ActivatedAt != nil && slot >= *featureAcct.ActivatedAt {
				f.EnableFeature(featureGate, *featureAcct.ActivatedAt)
			}

			if featureAcct.ActivatedAt == nil && startOfEpoch {
				newFeatureAcct := &features.FeatureAcct{ActivatedAt: &slot}
				newFeatureAcctBytes, err := features.MarshalFeatureAcct(newFeatureAcct)
				if err != nil {
					panic(err)
				}

				acct.Data = newFeatureAcctBytes
				modifiedAccts = append(modifiedAccts, acct)

				f.EnableFeature(featureGate, slot)
			}
		}
	}

	if len(modifiedAccts) != 0 {
		err := acctsDb.StoreAccounts(modifiedAccts, slot, nil)
		if err != nil {
			panic(err)
		}
	}

	for _, featureAcct := range modifiedAccts {
		// Handle *SIMD-0194: Deprecate rent exemption threshold* feature activation by updating the Rent sysvar.
		if f.IsActive(features.DeprecateRentExemptionThreshold) && featureAcct.Key == features.DeprecateRentExemptionThreshold.Address {
			var rentSysvar sealevel.SysvarRent
			rentSysvar.LamportsPerUint8Year = uint64(float64(sealevel.SysvarCache.Rent.Sysvar.LamportsPerUint8Year) * sealevel.SysvarCache.Rent.Sysvar.ExemptionThreshold)
			rentSysvar.ExemptionThreshold = 1.0
			rentSysvar.BurnPercent = sealevel.SysvarCache.Rent.Sysvar.BurnPercent

			rentAcct, err := acctsDb.GetAccount(slot, sealevel.SysvarRentAddr)
			if err != nil {
				panic(err)
			}
			parentAccts = append(parentAccts, rentAcct.Clone())

			newRentSysvarBytes := rentSysvar.MustMarshal()
			copy(rentAcct.Data, newRentSysvarBytes)
			err = acctsDb.StoreAccounts([]*accounts.Account{rentAcct}, slot, nil)
			if err != nil {
				panic(err)
			}
			modifiedAccts = append(modifiedAccts, rentAcct)

			sealevel.SysvarCache.Rent.Sysvar = &rentSysvar
			sealevel.SysvarCache.Rent.Acct = rentAcct
		}
	}

	return f, modifiedAccts, parentAccts
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
	epochCtx *ReplayCtx,
	epochSchedule *sealevel.SysvarEpochSchedule,
	rpcClient *rpcclient.RpcClient,
	auxBackupEndpoints []string) error {

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

	block.EpochAcctsHash = epochCtx.EpochAcctsHash

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

	if global.ManageBlockHeight() {
		block.BlockHeight = global.BlockHeight()
	}

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
	epochCtx *ReplayCtx,
	lastSlotCtx *sealevel.SlotCtx,
	epochSchedule *sealevel.SysvarEpochSchedule,
	rpcClient *rpcclient.RpcClient,
	auxBackupEndpoints []string) error {

	copy(block.ParentBankhash[:], lastSlotCtx.FinalBankhash)
	block.AcctsLtHash = lastSlotCtx.AcctsLtHash
	block.VoteTimestamps = lastSlotCtx.VoteTimestamps
	block.EpochStakesPerVoteAcct = lastSlotCtx.VoteAccts
	block.ParentSlot = lastSlotCtx.Slot
	block.LatestEvictedBlockhash = lastSlotCtx.LatestEvictedBlockhash
	block.EpochAcctsHash = epochCtx.EpochAcctsHash
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

	if global.ManageBlockHeight() {
		block.BlockHeight = global.BlockHeight()
	}
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
	epochCtx *ReplayCtx,
	epochSchedule *sealevel.SysvarEpochSchedule,
	rpcClient *rpcclient.RpcClient,
	auxBackupEndpoints []string) error {

	// Use resume state for parent info (the actual last replayed slot)
	copy(block.ParentBankhash[:], resumeState.ParentBankhash)
	block.ParentSlot = resumeState.ParentSlot
	block.AcctsLtHash = resumeState.AcctsLtHash
	block.EpochAcctsHash = epochCtx.EpochAcctsHash

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

	if global.ManageBlockHeight() {
		block.BlockHeight = global.BlockHeight()
	}

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
	global.SetBlockHeight(block.BlockHeight)
}

// buildInitialEpochStakesCache seeds the epoch stakes cache from state file or manifest.
// Priority: 1) State file ManifestEpochStakes, 2) Direct manifest (backwards compat)
func buildInitialEpochStakesCache(mithrilState *state.MithrilState) error {
	// Require state file ManifestEpochStakes (PersistedEpochStakes JSON format)
	if mithrilState == nil || len(mithrilState.ManifestEpochStakes) == 0 {
		return fmt.Errorf("state file missing manifest_epoch_stakes - delete AccountsDB and rebuild from snapshot")
	}

	for epoch, data := range mithrilState.ManifestEpochStakes {
		if loadedEpoch, err := global.DeserializeAndLoadEpochStakes([]byte(data)); err != nil {
			return fmt.Errorf("failed to load manifest epoch %d stakes from state file: %w", epoch, err)
		} else {
			mlog.Log.Debugf("loaded epoch %d stakes from state file manifest_epoch_stakes", loadedEpoch)
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
	resumeState *ResumeState,         // nil if not resuming, contains parent slot info when resuming
	startSlot, endSlot uint64,
	rpcEndpoints []string, // RPC endpoints in priority order (first = primary, rest = fallbacks)
	blockDir string,
	txParallelism int,
	isLive bool,
	useLightbringer bool,
	dbgOpts *DebugOptions,
	metricsWriter io.Writer,
	rpcServer *rpcserver.RpcServer,
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
	epochSchedule := sealevel.SysvarCache.EpochSchedule.Sysvar

	global.SetCalcUnixTimeForClockSysvar(true)
	global.SetManageBlockHeight(true)
	global.SetManageLeaderSchedule(true)

	var err error
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

	// Use state file for transaction count (required)
	global.IncrTransactionCount(mithrilState.ManifestTransactionCount)
	isFirstSlotInEpoch := epochSchedule.FirstSlotInEpoch(currentEpoch) == startSlot
	replayCtx.CurrentFeatures, featuresActivatedInFirstSlot, parentFeaturesActivatedInFirstSlot = scanAndEnableFeatures(acctsDb, startSlot, isFirstSlotInEpoch)
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
			if err := buildInitialEpochStakesCache(mithrilState); err != nil {
				result.Error = err
				return result
			}
		}
	} else {
		// Fresh start: load all epochs from state file
		if err := buildInitialEpochStakesCache(mithrilState); err != nil {
			result.Error = err
			return result
		}
	}
	// Resolve consensus config defaults before forkchoice init so we can
	// check whether enforcement requires authorized voters.
	consensusMaxDepth := 64
	consensusPolicy := "halt"
	consensusEnforceSource := "lightbringer"
	if consensusOpts != nil {
		if consensusOpts.SkipPathMaxDepth > 0 {
			consensusMaxDepth = consensusOpts.SkipPathMaxDepth
		}
		if consensusOpts.UnresolvedPolicy != "" {
			consensusPolicy = consensusOpts.UnresolvedPolicy
		}
		if consensusOpts.EnforceOnSource != "" {
			consensusEnforceSource = consensusOpts.EnforceOnSource
		}
	}

	// Determine whether consensus enforcement is active for this block source.
	// "lightbringer" = only enforce on Lightbringer blocks; "all" = enforce on all sources.
	consensusEnforceActive := (consensusEnforceSource == "all") || (consensusEnforceSource == "lightbringer" && useLightbringer)

	epochAuthVoters := global.EpochAuthorizedVoters()
	if epochAuthVoters == nil {
		// Without authorized voters, forkchoice can't parse votes → no supermajority → enforcement is blind.
		// If consensus enforcement is active, this is a fatal misconfiguration.
		if consensusEnforceActive && consensusPolicy == "halt" {
			result.Error = fmt.Errorf("forkchoice: EpochAuthorizedVoters is nil — cannot enforce consensus without vote parsing (check snapshot/state file)")
			return result
		}
		mlog.Log.Warnf("forkchoice: EpochAuthorizedVoters is nil — vote parsing will be skipped until populated")
	}
	forkChoice := forkchoice.NewForkChoiceService(currentEpoch, global.EpochStakes(currentEpoch), global.EpochTotalStake(currentEpoch), epochAuthVoters)
	forkChoice.Start()
	global.SetForkChoice(forkChoice)
	defer forkChoice.Stop()

	// Instantiate the consensus coordinator for skip-path resolution and policy.
	// The coordinator drives halt/warn policy and will be used for Lightbringer batch
	// range resolution when that mode is fully activated.
	consensusCoordinator := forkchoice.NewConsensusCoordinator(forkChoice, consensusMaxDepth, consensusPolicy)

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

	// Skip-path chain verification state: tracks processed blocks as candidates so that
	// ResolveRange can retroactively verify the bankhash chain when supermajority confirms a slot.
	// The "Blockhash" in candidates is the computed bankhash (not PoH blockhash), and
	// "LastBlockhash" is the parent bankhash — bankhashes chain correctly through blocks.
	var candidateBlocks map[uint64]*forkchoice.SlotCandidate
	var chainAnchorSlot uint64
	var chainAnchorHash solana.Hash
	var chainAnchorSet bool
	if consensusEnforceActive {
		candidateBlocks = make(map[uint64]*forkchoice.SlotCandidate)
	}

	var opts *blockstream.BlockSourceOpts
	if useLightbringer {
		opts = &blockstream.BlockSourceOpts{
			SourceType:         blockstream.BlockSourceLightbringer,
			RpcClient:          rpcc,
			BackupRpcEndpoints: rpcBackups,
			StartSlot:          startSlot,
			EndSlot:            endSlot,
			BlockDir:           blockDir,
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
	}
	blockStream := blockstream.NewBlockSource(opts)

	if !isLive {
		blockStream.DownloadInitialBlocks()
	}
	go blockStream.Start()

	var skippedSlotsCount int // Track skipped slots for 100-slot summary
	replayStartLogged := false

	// writeConsensusArtifact writes a best-effort JSON diagnostic artifact to the
	// per-run consensus subdirectory. If the log dir is empty or any step fails,
	// it logs a warning and continues — artifact failure must not crash replay.
	writeConsensusArtifact := func(filename string, data map[string]interface{}) {
		logDir := mlog.GetLogDir()
		if logDir == "" {
			return
		}
		dir := filepath.Join(logDir, "consensus")
		if err := os.MkdirAll(dir, 0755); err != nil {
			mlog.Log.Warnf("consensus artifact: failed to create directory %s: %v", dir, err)
			return
		}
		artifactPath := filepath.Join(dir, filename)
		artifactJSON, jsonErr := json.MarshalIndent(data, "", "  ")
		if jsonErr != nil {
			mlog.Log.Warnf("consensus artifact: failed to marshal JSON for %s: %v", filename, jsonErr)
			return
		}
		if writeErr := os.WriteFile(artifactPath, artifactJSON, 0644); writeErr != nil {
			mlog.Log.Warnf("consensus artifact: failed to write %s: %v", artifactPath, writeErr)
			return
		}
		mlog.Log.FileOnlyf("consensus artifact written: %s", artifactPath)
	}

	// Forkchoice Lightbringer enforcement: track slots whose bankhash verification
	// is deferred (votes haven't landed yet). Swept each iteration.
	type pendingBankhashCheck struct {
		slot     uint64
		bankhash solana.Hash
	}
	var pendingBankhashChecks []pendingBankhashCheck

	for {
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
		block := blockStream.NextBlock()
		waitTime := time.Since(waitStart)

		// Stop stall monitor
		if stallDone != nil {
			close(stallDone)
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
			// Log skipped slot in same format as regular blocks (with N/A for missing values)
			// Padding: cu=10 chars, txns fields, exec/wait/total=%7.3fs = 8 chars (7 for number + 's')
			mlog.Log.InfofPrecise("slot %-10d | leader: %-44s | txns: N/A              | cu: N/A        | exec:     N/A | wait:%7.3fs | total:%7.3fs (skip)",
				block.Slot, leaderStr, waitTime.Seconds(), waitTime.Seconds())
			skippedSlotsCount++
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
		// Use lastSlotCtx == nil to detect first block, not currentSlot == startSlot.
		// This handles the case where startSlot (or slots after it) are skipped -
		// the first emitted block might have slot > startSlot.
		if lastSlotCtx == nil {
			if resumeState != nil {
				// RESUME: Use resume state + state file (for static fields)
				configErr = configureInitialBlockFromResume(acctsDb, block, resumeState, mithrilState, replayCtx, epochSchedule, rpcc, rpcBackups)
			} else {
				// FRESH START: Use state file manifest_* fields
				configErr = configureInitialBlock(acctsDb, block, mithrilState, replayCtx, epochSchedule, rpcc, rpcBackups)
			}
		} else {
			configErr = configureBlock(block, replayCtx, lastSlotCtx, epochSchedule, rpcc, rpcBackups)
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
			replayCtx.CurrentFeatures, newlyActivatedFeatures, parentNewlyActivatedFeatures = scanAndEnableFeatures(acctsDb, currentSlot, true)
			partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
			partitionedRewardsInfo = handleEpochTransition(acctsDb, partitionedEpochRewardsEnabled, lastSlotCtx, replayCtx, epochSchedule, replayCtx.CurrentFeatures, block, currentEpoch)
			currentEpoch = block.Epoch
			justCrossedEpochBoundary = true

			// Refresh forkchoice with new epoch's stake weights and authorized voters
			if global.HasForkChoice() {
				forkChoice.UpdateEpoch(
					currentEpoch,
					global.EpochStakes(currentEpoch),
					global.EpochTotalStake(currentEpoch),
					global.EpochAuthorizedVoters(),
				)
			}

			// Persist rebuilt authorized voters to state file so resume loads fresh data
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

		// EAH (Epoch Accounts Hash) Workaround - DISABLED
		// Background: Before the AccountsLtHash feature was activated, Solana required an Epoch
		// Accounts Hash at specific slots during partitioned epoch rewards. This hash covers all
		// accounts and is included in the bankhash calculation at the EahStopOffsetSlot.
		// Problem: Mithril does not implement EAH computation (it's expensive and was being phased out).
		// Workaround: For historical slots that require EAH, we fetch the expected bankhash from RPC
		// instead of computing it locally. This allows replaying old slots without EAH implementation.
		// Note: This workaround is only needed for slots before AccountsLtHash activation (~Nov 2024).
		// If replaying historical slots and hitting EAH requirements, the bankhash is fetched from
		// a trusted RPC endpoint rather than computed. The bankhash is NOT stored to bankhash_db
		// in this case (see ProcessBlock's early return when HasEahWorkaround is true).
		// Uncomment if you need to replay pre-AccountsLtHash historical slots.
		/*
			if !block.Features.IsActive(features.AccountsLtHash) {
				if partitionedEpochRewardsEnabled && block.Slot == partitionedRewardsInfo.EahStopOffsetSlot {
					if replayCtx.HasEpochAcctsHash {
						block.EpochAcctsHash = replayCtx.EpochAcctsHash
					} else {
						block.EahWorkaroundBankhash, err = fetchBankhashForSlot(rpcc, block.Slot)
						if err != nil {
							panic(fmt.Sprintf("unable to fetch bankhash for EAH workaround for slot %d", block.Slot))
						}
						block.HasEahWorkaround = true
					}
				}
			}
		*/
		metrics.GlobalBlockReplay.PreprocessBlock.AddTimingSince(start)

		lastSlotCtx, err = ProcessBlock(acctsDb, block, txParallelism, dbgOpts, pt)
		if err != nil {
			mlog.Log.Errorf("error encountered during block replay: %s\n", err)
			result.Error = err
			// Clear any pending stake pubkeys from this failed block
			global.ClearPendingStakePubkeys()
			break
		}

		// Submit block to fork choice service for vote accumulation
		if global.HasForkChoice() {
			global.SubmitBlockToForkChoiceService(block.Slot, block.Transactions)
		}

		// Track processed blocks as candidates for skip-path chain verification.
		// Uses bankhashes (not PoH hashes): Blockhash = our computed bankhash,
		// LastBlockhash = parent bankhash. Skipped slots are omitted (solver handles gaps).
		if candidateBlocks != nil {
			candidateBlocks[block.Slot] = &forkchoice.SlotCandidate{
				Slot:          block.Slot,
				HasBlock:      true,
				Blockhash:     solana.HashFromBytes(lastSlotCtx.FinalBankhash),
				LastBlockhash: solana.Hash(block.ParentBankhash),
			}
			if !chainAnchorSet {
				chainAnchorSlot = block.ParentSlot
				chainAnchorHash = solana.Hash(block.ParentBankhash)
				chainAnchorSet = true
			}
		}

		// Consensus enforcement: verify bankhashes against vote-confirmed hashes.
		// Slots with observed supermajority are confirmed immediately; the 32-slot
		// timeout only applies to unresolved slots (no winner yet).
		// Enforcement scope is controlled by consensus.enforce_on_source config.
		if consensusEnforceActive && global.HasForkChoice() {
			pendingBankhashChecks = append(pendingBankhashChecks, pendingBankhashCheck{
				slot:     block.Slot,
				bankhash: solana.HashFromBytes(lastSlotCtx.FinalBankhash),
			})

			// TTL: slots pending longer than 2x the confirmation timeout are definitively unresolvable.
			const pendingTTLSlots = 2 * forkchoice.VoteConfirmationTimeoutSlots

			// Track latest confirmed slot in this sweep for chain verification.
			var latestConfirmedSlot uint64
			var latestConfirmedHash solana.Hash

			var remaining []pendingBankhashCheck
			for _, pc := range pendingBankhashChecks {
				confirmed := global.BankhashConfirmedForSlot(pc.slot, pc.bankhash)
				switch confirmed.Status {
				case forkchoice.BankhashHasSupermajority:
					mlog.Log.Debugf("forkchoice: slot %d bankhash confirmed by supermajority", pc.slot)
					if pc.slot > latestConfirmedSlot {
						latestConfirmedSlot = pc.slot
						latestConfirmedHash = confirmed.WinningHash
					}

				case forkchoice.BankhashNeedWait:
					remaining = append(remaining, pc)

				case forkchoice.BankhashNoSupermajority:
					if confirmed.WinningHash != (solana.Hash{}) && confirmed.WinningHash != pc.bankhash {
						// Our bankhash lost to a different supermajority hash — fork mismatch.
						mlog.Log.Errorf("CONSENSUS MISMATCH: checked_slot=%d replay_slot=%d our=%s winning=%s (our_stake=%d winning_stake=%d/%d threshold=%d)",
							pc.slot,
							block.Slot,
							base58.Encode(pc.bankhash[:]),
							base58.Encode(confirmed.WinningHash[:]),
							confirmed.StakeForHash,
							confirmed.WinningStake,
							confirmed.TotalEpochStake,
							confirmed.ThresholdStake,
						)

						writeConsensusArtifact(
							fmt.Sprintf("bankhash_mismatch_slot_%d.json", pc.slot),
							map[string]interface{}{
								"type":                          "bankhash_mismatch",
								"checked_slot":                  pc.slot,
								"observed_while_replaying_slot": block.Slot,
								"our_bankhash":                  base58.Encode(pc.bankhash[:]),
								"winning_bankhash":              base58.Encode(confirmed.WinningHash[:]),
								"our_stake":                     confirmed.StakeForHash,
								"winning_stake":                 confirmed.WinningStake,
								"total_epoch_stake":             confirmed.TotalEpochStake,
								"threshold_stake":               confirmed.ThresholdStake,
								"confirmation_status":           confirmed.Status.String(),
								"policy":                        consensusCoordinator.Policy(),
								"pending_age_slots":             block.Slot - pc.slot,
								"confirmation_timeout_slots":    forkchoice.VoteConfirmationTimeoutSlots,
								"chain_anchor_slot":             chainAnchorSlot,
								"chain_anchor_hash":             base58.Encode(chainAnchorHash[:]),
								"run_id":                        CurrentRunID,
							},
						)

						if consensusCoordinator.Policy() == "halt" {
							result.Error = fmt.Errorf("consensus halt: slot %d bankhash mismatch (our=%s winning=%s)",
								pc.slot, base58.Encode(pc.bankhash[:]), base58.Encode(confirmed.WinningHash[:]))
							break
						}
						// policy == "warn": log already emitted, continue
					} else if block.Slot > pc.slot+pendingTTLSlots {
						// TTL expired — no supermajority emerged. Not a fork issue, just low participation.
						mlog.Log.Debugf("forkchoice: slot %d no supermajority after TTL (our_stake=%d/%d)", pc.slot, confirmed.StakeForHash, confirmed.TotalEpochStake)
					} else {
						// No definitive result yet — keep checking.
						remaining = append(remaining, pc)
					}
				}
				if result.Error != nil {
					break
				}
			}
			pendingBankhashChecks = remaining

			if result.Error != nil {
				mlog.Log.Errorf("Triggering graceful shutdown due to consensus mismatch")
				break
			}

			// Chain verification: use the skip-path solver to verify the bankhash chain
			// from the last verified anchor to the latest confirmed slot. This validates
			// that processed blocks chain correctly through parent bankhashes.
			if candidateBlocks != nil && chainAnchorSet && latestConfirmedSlot > chainAnchorSlot {
				_, resolveErr := consensusCoordinator.ResolveRange(
					chainAnchorSlot+1, latestConfirmedSlot, chainAnchorHash, candidateBlocks)
				if resolveErr != nil {
					if errors.Is(resolveErr, forkchoice.ErrNoPath) {
						mlog.Log.Errorf("CHAIN VERIFICATION FAILED: no valid bankhash chain from slot %d to %d (while replaying slot %d)",
							chainAnchorSlot+1, latestConfirmedSlot, block.Slot)
						writeConsensusArtifact(
							fmt.Sprintf("chain_verification_failure_%d_%d.json", chainAnchorSlot+1, latestConfirmedSlot),
							map[string]interface{}{
								"type":                          "chain_verification_failure",
								"start_slot":                    chainAnchorSlot + 1,
								"end_slot":                      latestConfirmedSlot,
								"anchor_slot":                   chainAnchorSlot,
								"anchor_hash":                   base58.Encode(chainAnchorHash[:]),
								"target_slot":                   latestConfirmedSlot,
								"target_hash":                   base58.Encode(latestConfirmedHash[:]),
								"observed_while_replaying_slot": block.Slot,
								"reason":                        "no_path",
								"policy":                        consensusCoordinator.Policy(),
								"candidate_count":               len(candidateBlocks),
								"run_id":                        CurrentRunID,
							},
						)
						if consensusCoordinator.Policy() == "halt" {
							result.Error = fmt.Errorf("consensus halt: chain verification failed (slots %d-%d)",
								chainAnchorSlot+1, latestConfirmedSlot)
						}
					} else if errors.Is(resolveErr, forkchoice.ErrDepthExceeded) {
						// Range too large for solver — not a chain error, just advance the anchor.
						mlog.Log.Debugf("forkchoice: chain verification skipped (depth exceeded for slots %d-%d), advancing anchor",
							chainAnchorSlot+1, latestConfirmedSlot)
					} else {
						mlog.Log.Debugf("forkchoice: chain verification deferred for slots %d-%d: %v",
							chainAnchorSlot+1, latestConfirmedSlot, resolveErr)
					}
				} else {
					mlog.Log.Debugf("forkchoice: chain verified slots %d-%d via skip-path solver",
						chainAnchorSlot+1, latestConfirmedSlot)
				}
				// Advance anchor regardless — verified or not, we won't re-check this range.
				chainAnchorSlot = latestConfirmedSlot
				chainAnchorHash = latestConfirmedHash
				// Clean up old candidates to bound memory usage.
				for slot := range candidateBlocks {
					if slot <= latestConfirmedSlot {
						delete(candidateBlocks, slot)
					}
				}

				if result.Error != nil {
					mlog.Log.Errorf("Triggering graceful shutdown due to chain verification failure")
					break
				}
			}
		}

		replayCtx.Capitalization -= lastSlotCtx.LamportsBurnt

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
			result.LastPersistedSlot, result.LastPersistedBankhash = pt.Get()

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

		// Calculate slot stats: vote/non-vote tx counts and total CU
		var voteTxCount, nonVoteTxCount int
		var totalCU uint64
		for i, tx := range block.Transactions {
			if tx.IsVote() {
				voteTxCount++
			} else {
				nonVoteTxCount++
			}
			if i < len(block.TxMetas) && block.TxMetas[i] != nil && block.TxMetas[i].ComputeUnitsConsumed != nil {
				totalCU += *block.TxMetas[i].ComputeUnitsConsumed
			}
		}

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

				// Line 2: CU and transaction stats (median/min/max)
				mlog.Log.InfofPrecise("  cu: median %d, min %d, max %d | txns: median vote %d, median non-vote %d",
					medCU, minCU, maxCU, medVoteTx, medNonVoteTx)

				// Line 3: Execution stats (median/min/max for execution, wait; median for replay total)
				mlog.Log.InfofPrecise("  execution: median %.3fs, min %.3fs, max %.3fs | wait: median %.3fs, min %.3fs, max %.3fs | replay total: median %.3fs",
					medExec, minExec, maxExec, medWait, minWait, maxWait, medTotal)

				// Account clone stats for copy-on-write optimization profiling
				cloneStats := GetAndResetCloneStats()
				if cloneStats.TxCount > 0 {
					modifyRatio := float64(cloneStats.AcctsTouched) / float64(cloneStats.AcctsCloned) * 100
					avgAcctsPerTx := float64(cloneStats.AcctsCloned) / float64(cloneStats.TxCount)
					avgTouchedPerTx := float64(cloneStats.AcctsTouched) / float64(cloneStats.TxCount)
					clonedMB := float64(cloneStats.AcctsClonedBytes) / 1024 / 1024
					touchedMB := float64(cloneStats.AcctsTouchedBytes) / 1024 / 1024
					mlog.Log.InfofPrecise("  clone stats: %.1f%% modified (%d/%d accts) | %.1fMB cloned, %.1fMB modified | avg/tx: %.1f cloned, %.1f modified",
						modifyRatio, cloneStats.AcctsTouched, cloneStats.AcctsCloned,
						clonedMB, touchedMB, avgAcctsPerTx, avgTouchedPerTx)
				}

				// Line 4: RPC/fetch debugging info
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

	// Check if block source stalled (this provides explicit error info)
	if blockStream.Stalled() && result.Error == nil {
		result.Error = fmt.Errorf("block fetch stalled - no progress for %v", blockStream.StallTimeout())
	}

	acctsDb.WaitForStoreWorker()
	result.LastPersistedSlot, result.LastPersistedBankhash = pt.Get()

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

		VoteAccts:       block.EpochStakesPerVoteAcct,
		VoteTimestampMu: &sync.Mutex{},
		VoteTimestamps:  block.VoteTimestamps,
		TotalEpochStake: block.TotalEpochStake,

		EpochsAcctHash:        block.EpochAcctsHash,
		EahWorkaroundBankhash: block.EahWorkaroundBankhash,

		HasEahWorkaround: block.HasEahWorkaround,

		SerializedParameterArena: SerializedParameterArena,
	}

	return slotCtx
}

func sequentialTxLoop(slotCtx *sealevel.SlotCtx, sigverifyWg *sync.WaitGroup, block *b.Block, dbgOpts *DebugOptions) fees.TxFeeInfoAccumulator {
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	// process & execute each transaction in turn
	for idx, tx := range block.Transactions {
		var txMeta *rpc.TransactionMeta
		if block.TxMetas != nil {
			txMeta = block.TxMetas[idx]
		}
		txFeeInfo, txErr := ProcessTransaction(slotCtx, sigverifyWg, tx, txMeta, dbgOpts, nil)

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

		txFeeAccumulator.Add(txFeeInfo)
	}
	return txFeeAccumulator
}

func parallelTxLoop(slotCtx *sealevel.SlotCtx, sigverifyWg *sync.WaitGroup, block *b.Block, rblock *b.Block, txParallelism int, dbgOpts *DebugOptions) fees.TxFeeInfoAccumulator {
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	txFeeInfos := make([]*fees.TxFeeInfo, len(block.Transactions))
	errs := make([]error, len(block.Transactions))
	txDurations := make([]time.Duration, txParallelism)

	if rblock.FromLightbringer {
		wg := &sync.WaitGroup{}
		workerPool, _ := ants.NewPoolWithFunc(txParallelism, func(i interface{}) {
			defer wg.Done()
			idx := i.(uint64)
			txFeeInfos[idx], errs[idx] = ProcessTransaction(slotCtx, sigverifyWg, rblock.Transactions[idx], nil, dbgOpts, nil)
		})

		for _, entry := range rblock.Entries {
			for _, txIdx := range entry.Indices {
				wg.Add(1)
				workerPool.Invoke(txIdx)
			}
			wg.Wait()
		}
	} else {
		do := make(chan int, len(block.Transactions))
		done := make(chan int, len(block.Transactions))
		go TopsortPlannerStream(block, do, done)

		wg := &sync.WaitGroup{}
		wg.Add(txParallelism)
		for i := range txParallelism {
			go func() {
				defer wg.Done()
				for idx := range do {
					txStart := time.Now()
					tx := block.Transactions[idx]
					txFeeInfos[idx], errs[idx] = ProcessTransaction(slotCtx, sigverifyWg, rblock.Transactions[idx], rblock.TxMetas[idx], dbgOpts, sealevel.BorrowedAccountArenas[i])
					txErr := errs[idx]
					// check for success-failure return value divergences
					if rblock.TxMetas != nil && txErr == nil && rblock.TxMetas[idx].Err != nil {
						mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s succeeded locally but failed onchain: %+v",
							CurrentRunID, block.Slot, tx.Signatures[0], rblock.TxMetas[idx].Err)
						panic(fmt.Sprintf("tx %s return value divergence: txErr was nil, but onchain err was %+v", tx.Signatures[0], rblock.TxMetas[idx].Err))
					} else if rblock.TxMetas != nil && txErr != nil && rblock.TxMetas[idx].Err == nil {
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
	}

	for idx, txFeeInfo := range txFeeInfos {
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

	return txFeeAccumulator
}

func ProcessBlock(
	acctsDb *accountsdb.AccountsDb,
	block *b.Block,
	txParallelism int,
	dbgOpts *DebugOptions,
	// pt is updated after StoreAccounts completes through a callback.
	// Must be non-nil.
	pt *persistedTracker,
) (*sealevel.SlotCtx, error) {
	ctx, task := trace.NewTask(context.Background(), "ProcessBlock")
	defer task.End()
	trace.Log(ctx, "slot", fmt.Sprintf("%d", block.Slot))
	trace.Log(ctx, "txCount", fmt.Sprintf("%d", len(block.Transactions)))

	if SerializedParameterArena != nil {
		SerializedParameterArena.Reset()
	}

	var sigverifyWg sync.WaitGroup
	defer sigverifyWg.Wait()
	start := time.Now()
	unresolvedBlock := &b.Block{
		Transactions: make([]*solana.Transaction, len(block.Transactions)),
		TxMetas:      make([]*rpc.TransactionMeta, len(block.TxMetas)),
	}
	for i := range block.Transactions {
		unresolvedBlock.Transactions[i] = &solana.Transaction{}
		*(unresolvedBlock.Transactions[i]) = *block.Transactions[i]
		if unresolvedBlock.TxMetas != nil && !block.FromLightbringer {
			unresolvedBlock.TxMetas[i] = &rpc.TransactionMeta{}
			*(unresolvedBlock.TxMetas[i]) = *block.TxMetas[i]
		}
	}

	start = time.Now()
	loadAcctsRegion := trace.StartRegion(ctx, "LoadBlockAccounts")
	accts, parentAccts, err := loadBlockAccountsAndUpdateSysvars(acctsDb, block)
	loadAcctsRegion.End()
	if err != nil {
		panic(fmt.Sprintf("unable to load slot accounts and update sysvars: %s", err))
	}
	metrics.GlobalBlockReplay.LoadBlockAccounts.AddTimingSince(start)

	slotCtx := newSlotCtx(block, accts, parentAccts, acctsDb)
	slotCtx.TraceCtx = ctx
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	start = time.Now()

	txLoopRegion := trace.StartRegion(ctx, "TxLoop")
	if txParallelism > 0 {
		txFeeAccumulator = parallelTxLoop(slotCtx, &sigverifyWg, unresolvedBlock, block, txParallelism, dbgOpts)
	} else {
		txFeeAccumulator = sequentialTxLoop(slotCtx, &sigverifyWg, block, dbgOpts)
	}
	txLoopRegion.End()
	metrics.GlobalBlockReplay.TxLoop.AddTimingSince(start)

	start = time.Now()

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
	epochSchedule := sealevel.SysvarCache.EpochSchedule.Sysvar
	rentSysvar := sealevel.SysvarCache.Rent.Sysvar
	rentAccts := rent.CollectRentEagerly(slotCtx, rentSysvar, epochSchedule)
	metrics.GlobalBlockReplay.Rent.AddTimingSince(start)

	start = time.Now()
	runIncinerator(slotCtx)
	metrics.GlobalBlockReplay.RunIncinerator.AddTimingSince(start)

	start = time.Now()
	writableAccts, modifiedAccts := compileWritableAndModifiedAccts(slotCtx, block, rentAccts)

	start = time.Now()
	slotCtx.FinalBankhash = bankhash.CalculateBankHash(slotCtx, writableAccts, modifiedAccts, block.ParentBankhash, block.NumSignatures, block.Blockhash)
	metrics.GlobalBlockReplay.BankHash.AddTimingSince(start)

	// Bankhash consensus enforcement is handled in the replay loop (not here)
	// because forkchoice is fed after ProcessBlock returns — checking here would
	// never see votes from recently submitted blocks and could deadlock.

	// Enter critical commit window - panics here may leave AccountsDB inconsistent
	commitSlot.Store(slotCtx.Slot)
	commitInProgress.Store(true)
	start = time.Now()
	afterStoreAccounts := func() {
		metrics.GlobalBlockReplay.BlockUpdateAccounts.AddTimingSince(start)
		err := acctsDb.StoreBankHashForSlot(slotCtx.Slot, slotCtx.FinalBankhash)
		if err != nil {
			mlog.Log.Infof("unable to store bankhash for slot %d", slotCtx.Slot)
		}
		flushed, err := global.FlushPendingStakePubkeys(filepath.Join(acctsDb.AcctsDir, ".."))
		if err != nil {
			mlog.Log.Errorf("failed to flush stake pubkey index: %v", err)
		} else if flushed > 0 {
			mlog.Log.Debugf("flushed %d new stake pubkeys to index", flushed)
		}

		pt.Set(block.Slot, slotCtx.FinalBankhash)

		// Exit critical commit window - AccountsDB is now consistent
		commitInProgress.Store(false)
		commitSlot.Store(0)
	}

	if len(modifiedAccts) > 0 {
		err = acctsDb.StoreAccounts(modifiedAccts, slotCtx.Slot, afterStoreAccounts)
	}

	/*
		// EAH workaround - see comment at top of replay loop for details
		// Commented since it seems to be disabled but preserved for the curious?
		if slotCtx.HasEahWorkaround {
			slotCtx.FinalBankhash = slotCtx.EahWorkaroundBankhash
			commitInProgress.Store(false)
			commitSlot.Store(0)
			return slotCtx, err
		}
	*/

	global.IncrTransactionCount(uint64(len(block.Transactions)))
	return slotCtx, err
}
