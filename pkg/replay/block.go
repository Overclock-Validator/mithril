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
	"slices"
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

	// Repair-first catchup: resume gaps up to this many slots fill via turbine
	// repair instead of RPC getBlock (0 disables). Turbine source only.
	RepairCatchupMaxGapSlots uint64
	// Repair request-rate ceiling override, requests/second (0 = default).
	RepairMaxRequestsPerSecond int
	// TurbineServeRepairAddr is the fixed UDP endpoint advertised for inbound
	// Solana repair requests (empty disables serving).
	TurbineServeRepairAddr string
	// Shreds-only: RPC never fetches blocks (block.rpc_fallback=false).
	DisableRPCBlockFetch bool
	// TurbinePrewarm: boot-time shred collector to hand over (stop + drain)
	// right before the block source starts. May be nil.
	TurbinePrewarm *blockstream.TurbinePrewarm
	// ShredSpoolDir: on-disk verified-shred spool shared by prewarm and the
	// block source (empty = disabled).
	ShredSpoolDir string
	// LocalBlocks carries fully frozen blocks produced by this validator. Replay
	// adopts the already-mutated leader SlotCtx and does not re-execute.
	LocalBlocks          <-chan *b.Block
	LocalLeaderForSlot   func(slot uint64) bool
	GossipClient         *gossip.Client
	PrewarmBlocks        []*b.Block
	TurbineStakesForSlot func(slot uint64) map[solana.PublicKey]uint64
	TurbineEpochForSlot  func(slot uint64) uint64
	TurbineRootSlot      func() uint64
	TurbineUseChaCha8    bool
	TurbineDedupAddrs    bool
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

func identityFromTurbineKey(priv ed25519.PrivateKey) solana.PublicKey {
	if len(priv) != ed25519.PrivateKeySize {
		return solana.PublicKey{}
	}
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return solana.PublicKey{}
	}
	return solana.PublicKeyFromBytes(pub)
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
	LastNumSignatures        uint64         // signatures processed in the last persisted bank

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
	// ParentAlpenglowBlockID is the exact double-merkle identity of ParentSlot.
	// It seeds the live block source after checkpoint restart so a skip run can
	// be validated before the first post-resume block is emitted.
	ParentAlpenglowBlockID    solana.Hash
	HasParentAlpenglowBlockID bool
	// ParentAlpenglowChainedMerkleRoot is the last data-shred Merkle root of
	// ParentSlot. It must be restored with the block ID so the first producer
	// bank after restart chains its shreds to the same parent generation.
	ParentAlpenglowChainedMerkleRoot    solana.Hash
	HasParentAlpenglowChainedMerkleRoot bool
	// AcctsLtHash is the cumulative LtHash at the end of the parent slot
	AcctsLtHash *lthash.LtHash
	// LamportsPerSignature for reconstructing FeeRateGovernor
	LamportsPerSignature uint64
	// PrevLamportsPerSignature for reconstructing FeeRateGovernor
	PrevLamportsPerSignature uint64
	// NumSignatures is the number of signatures processed in the parent bank.
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
		if tx.Message.GetVersion() != solana.MessageVersionV0 {
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
		if tx.Message.GetVersion() != solana.MessageVersionV0 || tx.Message.AddressTableLookups.NumLookups() == 0 {
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
	if tx.Message.GetVersion() != solana.MessageVersionV0 || tx.Message.AddressTableLookups.NumLookups() == 0 {
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

const transactionPublicationNonTransactionSlack = 8
const expectedTouchedAccountsPerTransaction = 2

func extractAndDedupeBlockAccts(block *b.Block) ([]solana.PublicKey, int) {
	var numPubkeys int
	for _, tx := range block.Transactions {
		numPubkeys += len(tx.Message.AccountKeys)
	}

	numPubkeys += len(block.UpdatedAccts)
	pubkeyMap := make(map[solana.PublicKey]bool, numPubkeys)

	for _, tx := range block.Transactions {
		numStaticAccounts, numWritableLookupAccounts := messageAccountLayout(&tx.Message)
		for idx, pk := range tx.Message.AccountKeys {
			if messageAccountIsWritable(&tx.Message, idx, numStaticAccounts, numWritableLookupAccounts) {
				pubkeyMap[pk] = true
			} else if _, exists := pubkeyMap[pk]; !exists {
				pubkeyMap[pk] = false
			}
		}
	}

	for _, pk := range block.UpdatedAccts {
		pubkeyMap[pk] = true
	}

	pubkeys := make([]solana.PublicKey, len(pubkeyMap))
	i := 0
	writableAccountCount := 0
	for pk, writable := range pubkeyMap {
		pubkeys[i] = pk
		i++
		if writable {
			writableAccountCount++
		}
	}

	return pubkeys, writableAccountCount
}

func includeAlpenglowParentStateAccounts(pubkeys []solana.PublicKey, alpenglowClock bool) []solana.PublicKey {
	if !alpenglowClock {
		return pubkeys
	}
	nanoClockAddr := NanosecondClockAccountAddr()
	if slices.Contains(pubkeys, nanoClockAddr) {
		return pubkeys
	}
	return append(pubkeys, nanoClockAddr)
}

func publicationMapCapacity(block *b.Block, uniqueWritableAccounts int, alpenglow bool) int {
	// This is an allocation hint, not a shard-count bound. Cap speculative
	// transaction capacity at the observed transfer workload's touch rate so
	// failure-heavy blocks do not eagerly allocate for every declared writable
	// key; maps grow naturally for higher-fanout successful transactions.
	expectedTransactionTouches := min(uniqueWritableAccounts, len(block.Transactions)*expectedTouchedAccountsPerTransaction)
	capacity := expectedTransactionTouches + len(block.EpochUpdatedAccts) + transactionPublicationNonTransactionSlack
	if alpenglow {
		// Certificate-driven vote writes are absent from transaction message keys.
		capacity += len(block.EpochStakesPerVoteAcct)
	}
	return capacity
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

// cacheFeesSysvar loads the legacy Fees sysvar when the cluster still has it.
// Its absence is expected after DisableFeesSysvar activation.
func cacheFeesSysvar(acctsDb *accountsdb.AccountsDb) {
	if acctsDb == nil {
		return
	}
	acct, err := acctsDb.GetAccount(0, sealevel.SysvarFeesAddr)
	if errors.Is(err, accountsdb.ErrNoAccount) {
		// Absence is authoritative. Replay can be restarted in-process after a
		// fork recovery, so retaining a value from an earlier bootstrap would
		// incorrectly resurrect the disabled legacy sysvar in BankSysvars.
		sealevel.SysvarCache.Fees.Sysvar = nil
		sealevel.SysvarCache.Fees.Acct = nil
		return
	}
	if err != nil {
		panic("unable to get fees sysvar when caching sysvars")
	}
	var fees sealevel.SysvarFees
	fees.MustUnmarshalWithDecoder(bin.NewBinDecoder(acct.Data))
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
			panic("unable to get fees sysvar when caching sysvars")
		}
		var rewards sealevel.SysvarEpochRewards
		decoder := bin.NewBinDecoder(acct.Data)
		rewards.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.EpochRewards.Sysvar = &rewards
		sealevel.SysvarCache.EpochRewards.Acct = acct
	}
}

// validatePartitionedRewardsResume decides whether replay can safely continue
// without the in-memory partition spool bookkeeping created at an epoch
// boundary. EpochRewards.Active is persisted as bank state; the fixed rewards
// calendar window is only an upper bound and cannot answer this question.
func validatePartitionedRewardsResume(slot uint64, epochRewards *sealevel.SysvarEpochRewards) error {
	if epochRewards == nil {
		return fmt.Errorf("cannot resume replay at slot %d: persisted EpochRewards sysvar is unavailable", slot)
	}
	if !epochRewards.Active {
		return nil
	}
	return fmt.Errorf(
		"cannot resume replay at slot %d: persisted EpochRewards is still active (distribution_starting_block_height=%d num_partitions=%d), but partition spool progress is not restored",
		slot, epochRewards.DistributionStartingBlockHeight, epochRewards.NumPartitions,
	)
}

func loadSysvarAccount(source blockAccountSource, slot uint64, pubkey solana.PublicKey, timing *metrics.Timing) (*accounts.Account, error) {
	start := time.Now()
	acct, stats, err := getAccountWithStats(source, slot, pubkey)
	timing.AddTimingSince(start)
	recordSysvarAccountReadStats(&metrics.GlobalBlockReplay.AccountLoader, stats)
	return acct, err
}

func recordSysvarAccountReadStats(dst *metrics.AccountLoader, src accountsdb.AccountReadStats) {
	dst.SysvarReads++
	dst.SysvarWorkingSetLookup.AddTiming(time.Duration(src.WorkingSetLookupNanoseconds))
	dst.SysvarClone.AddTiming(time.Duration(src.CloneNanoseconds))
	dst.SysvarAppendVecPinWait.AddTiming(time.Duration(src.AppendVecPinWaitNanoseconds))
	dst.SysvarInProgressLookup.AddTiming(time.Duration(src.InProgressNanoseconds))
	dst.SysvarReadCacheEpochWait.AddTiming(time.Duration(src.ReadCacheEpochWaitNanoseconds))
	dst.SysvarCacheLookup.AddTiming(time.Duration(src.CacheLookupNanoseconds))
	dst.SysvarIndexAndAppendVecRead.AddTiming(time.Duration(src.IndexAndAppendVecReadNanoseconds))
	dst.SysvarCachePublicationWait.AddTiming(time.Duration(src.CachePublicationWaitNanoseconds))
	dst.SysvarCachePublication.AddTiming(time.Duration(src.CachePublicationNanoseconds))
	if src.WorkingSetHit {
		dst.SysvarWorkingSetHits++
	}
	if src.InProgressHit {
		dst.SysvarInProgressHits++
	}
	if src.PendingFoldHit {
		dst.SysvarPendingFoldHits++
	}
	if src.CacheHit {
		dst.SysvarCacheHits++
	}
	if src.DurableRead {
		dst.SysvarDurableReads++
	}
	if src.CachePublicationEpochRejected {
		dst.SysvarCachePublicationEpochRejects++
	}
}

func loadBlockAccountsAndUpdateSysvars(accountsDb blockAccountSource, block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule, alpenglowClock bool, parentBankSysvars *sealevel.BankSysvars) (accounts.Accounts, accounts.Accounts, int, *sealevel.BankSysvars, error) {
	var bankSysvars *sealevel.BankSysvars
	phaseStart := time.Now()
	err := resolveAddrTableLookups(accountsDb, block)
	metrics.GlobalBlockReplay.AccountLoader.AddressTableLookups.AddTimingSince(phaseStart)
	if err != nil {
		return nil, nil, 0, bankSysvars, err
	}

	phaseStart = time.Now()
	dedupedAccts, uniqueWritableAccounts := extractAndDedupeBlockAccts(block)
	// The footer-owned nanosecond clock is bank state even when no transaction
	// mentions it. Pin the exact parent account (or AccountsDB's tombstone for
	// absence) in the same batch snapshot as all other execution accounts. This
	// preserves both the footer bounds anchor and the AccountsLtHash before-image.
	dedupedAccts = includeAlpenglowParentStateAccounts(dedupedAccts, alpenglowClock)
	publicationCapacity := publicationMapCapacity(block, uniqueWritableAccounts, alpenglowClock)
	metrics.GlobalBlockReplay.AccountLoader.DedupeBlockAccounts.AddTimingSince(phaseStart)
	ctx := context.Background()
	phaseStart = time.Now()
	slotAccts, batchStats, err := getAccountsBatchSharedWithStats(ctx, accountsDb, block.Slot, dedupedAccts)
	metrics.GlobalBlockReplay.AccountLoader.SourceBatch.AddTimingSince(phaseStart)
	recordAccountLoaderBatchStats(&metrics.GlobalBlockReplay.AccountLoader, batchStats)
	if err != nil {
		return nil, nil, 0, bankSysvars, err
	}

	phaseStart = time.Now()
	numAccts := len(slotAccts)
	metrics.GlobalBlockReplay.AccountLoader.ParentAccounts = uint64(numAccts)
	parentAccts := accounts.NewMemAccountsWithLen(uint64(numAccts))
	for _, acct := range slotAccts {
		if err = parentAccts.SetAccountWithoutLock(acct.Key, acct); err != nil {
			return nil, nil, 0, bankSysvars, err
		}
	}

	// accts is a branch-local overlay over the pristine parent snapshot; execution
	// copy-on-writes, so parentAccts stays pristine for LtHash "before" values.
	accts := accounts.NewOverlayAccountsWithSizing(parentAccts, numAccts, publicationCapacity)
	if parentBankSysvars != nil {
		if parentBankSysvars.Slot() != block.ParentSlot {
			return nil, nil, 0, nil, fmt.Errorf(
				"parent bank sysvar slot %d does not match block parent %d",
				parentBankSysvars.Slot(), block.ParentSlot,
			)
		}
		// Pin every bank-owned sysvar to the exact immutable parent generation
		// before applying this bank's lifecycle updates. This covers raw account
		// reads as well as typed reads and installs explicit tombstones for sysvars
		// absent in the parent, so neither AccountsDB nor a process-global cache can
		// leak a newer abandoned-fork generation into this child.
		if err := sealevel.RangeBankSysvarAddresses(func(key solana.PublicKey) error {
			acct, ok := parentBankSysvars.AccountView(key)
			if !ok {
				acct = &accounts.Account{Key: key, RentEpoch: math.MaxUint64}
			}
			return parentAccts.SetAccountWithoutLock(key, acct)
		}); err != nil {
			return nil, nil, 0, nil, fmt.Errorf("install parent bank sysvars: %w", err)
		}
	}
	metrics.GlobalBlockReplay.AccountLoader.ParentMapBuild.AddTimingSince(phaseStart)

	phaseStart = time.Now()
	block.FeeRateGovernor = sealevel.NewFeeRateGovernorDerived(block.PrevFeeRateGovernor, block.PrevNumSignatures)
	if block.FeeRateGovernor.PrevLamportsPerSignature == 0 {
		block.FeeRateGovernor.PrevLamportsPerSignature = block.InitialPreviousLamportsPerSignature
	}

	// load sysvar accounts and assign them to the sysvar cache
	{
		// update and cache clock sysvar
		{
			var clockAcct *accounts.Account
			var clock sealevel.SysvarClock
			clockEpochSchedule := epochSchedule
			var err error
			if parentBankSysvars != nil {
				var ok bool
				clockAcct, ok = parentBankSysvars.CloneAccount(sealevel.SysvarClockAddr)
				if !ok {
					panic("required Clock sysvar is absent from parent bank snapshot")
				}
				clock, ok = parentBankSysvars.Clock()
				if !ok {
					panic("decoded Clock sysvar is absent from parent bank snapshot")
				}
				parentEpochSchedule, ok := parentBankSysvars.EpochSchedule()
				if !ok {
					panic("decoded EpochSchedule sysvar is absent from parent bank snapshot")
				}
				clockEpochSchedule = &parentEpochSchedule
			} else if sealevel.SysvarCache.Clock.Acct != nil {
				// Prefer the in-RAM Clock (mirrors SlotHashes/RecentBlockhashes): on
				// resume it is the restored Clock as of the last rooted slot, which durable may not match.
				clockAcct = sealevel.SysvarCache.Clock.Acct.Clone()
			} else {
				clockAcct, err = loadSysvarAccount(accountsDb, block.Slot, sealevel.SysvarClockAddr, &metrics.GlobalBlockReplay.AccountLoader.SysvarClockRead)
			}
			if err != nil {
				panic("unable to retrieve clock sysvar when updating clock")
			}

			if parentBankSysvars == nil {
				err = parentAccts.SetAccountWithoutLock(sealevel.SysvarClockAddr, clockAcct.Clone())
				if err != nil {
					panic("unable to set clock sysvar to accts")
				}
				err = clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockAcct.Data))
				if err != nil {
					panic("unable to unmarshal clock sysvar")
				}
			}

			err = updateClockSysvarForMode(&clock, block, clockEpochSchedule, alpenglowClock)
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
			var slotHashesAcct *accounts.Account
			var slotHashes sealevel.SysvarSlotHashes
			var err error
			if parentBankSysvars != nil {
				var ok bool
				slotHashesAcct, ok = parentBankSysvars.CloneAccount(sealevel.SysvarSlotHashesAddr)
				if !ok {
					panic("required SlotHashes sysvar is absent from parent bank snapshot")
				}
				parentSlotHashes, ok := parentBankSysvars.SlotHashes()
				if !ok {
					panic("decoded SlotHashes sysvar is absent from parent bank snapshot")
				}
				// Update mutates the slice; detach it while sharing every other
				// decoded sysvar with the immutable parent snapshot.
				slotHashes = append(sealevel.SysvarSlotHashes(nil), parentSlotHashes...)
			} else {
				slotHashesAcct, err = loadSysvarAccount(accountsDb, block.Slot, sealevel.SysvarSlotHashesAddr, &metrics.GlobalBlockReplay.AccountLoader.SysvarSlotHashesRead)
				if err != nil {
					panic("unable to retrieve slothashes sysvar from acctsdb")
				}
			}

			if parentBankSysvars == nil && sealevel.SysvarCache.SlotHashes.Sysvar == nil {
				// Fresh start (first slot): unmarshal from AccountsDB
				decoder := bin.NewBinDecoder(slotHashesAcct.Data)
				err = slotHashes.UnmarshalWithDecoder(decoder)
				if err != nil {
					panic("unable to unmarshal slothashes sysvar")
				}

			} else if parentBankSysvars == nil {
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

			if parentBankSysvars == nil {
				// Set parentAccts BEFORE updating slotHashes to ensure LtHash delta is computed correctly.
				err = parentAccts.SetAccountWithoutLock(sealevel.SysvarSlotHashesAddr, slotHashesAcct.Clone())
				if err != nil {
					panic("unable to set slothashes sysvar to accountsdb")
				}
			}

			// Now update with the new slot/bankhash
			slotHashes.Update(block.Slot, block.ParentSlot, block.ParentBankhash)
			newSlotHashesBytes := slotHashes.MustMarshal()
			slotHashesAcct.Data = newSlotHashesBytes
			sealevel.SysvarCache.SlotHashes.Sysvar = &slotHashes
			sealevel.SysvarCache.SlotHashes.Acct = slotHashesAcct
			err = accts.SetAccountWithoutLock(sealevel.SysvarSlotHashesAddr, slotHashesAcct)
			if err != nil {
				panic("unable to set slothashes sysvar to accountsdb")
			}
		}

		// cache RecentBlockhashes sysvar
		{
			if parentBankSysvars != nil {
				if _, ok := parentBankSysvars.RecentBlockhashes(); !ok {
					panic("required RecentBlockhashes sysvar is absent from parent bank snapshot")
				}
			} else {
				recentBlockhashesAcct, err := loadSysvarAccount(accountsDb, block.Slot, sealevel.SysvarRecentBlockHashesAddr, &metrics.GlobalBlockReplay.AccountLoader.SysvarRecentBlockhashesRead)
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
		}

		// cache SlotHistory sysvar
		{
			if parentBankSysvars != nil {
				if _, ok := parentBankSysvars.SlotHistory(); !ok {
					panic("required SlotHistory sysvar is absent from parent bank snapshot")
				}
			} else {
				slotHistoryAcct, err := loadSysvarAccount(accountsDb, block.Slot, sealevel.SysvarSlotHistoryAddr, &metrics.GlobalBlockReplay.AccountLoader.SysvarSlotHistoryRead)
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
		}

		// cache StakeHistory sysvar
		{
			if parentBankSysvars != nil {
				if _, ok := parentBankSysvars.StakeHistory(); !ok {
					panic("required StakeHistory sysvar is absent from parent bank snapshot")
				}
			} else {
				stakeHistoryAcct, err := loadSysvarAccount(accountsDb, block.Slot, sealevel.SysvarStakeHistoryAddr, &metrics.GlobalBlockReplay.AccountLoader.SysvarStakeHistoryRead)
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
		}

		// cache LastRestartSlot sysvar
		{
			if parentBankSysvars != nil {
				if _, ok := parentBankSysvars.LastRestartSlot(); !ok {
					panic("required LastRestartSlot sysvar is absent from parent bank snapshot")
				}
			} else {
				lastRestartSlotAcct, err := loadSysvarAccount(accountsDb, block.Slot, sealevel.SysvarLastRestartSlotAddr, &metrics.GlobalBlockReplay.AccountLoader.SysvarLastRestartSlotRead)
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

	// The process-global cache is retained only as the ordered replay bootstrap
	// source.  Freeze a complete immutable bank-owned snapshot after applying
	// every epoch-boundary override; transaction execution never reads the
	// singleton once this snapshot is published to SlotCtx.
	loadCurrentSysvar := func(key solana.PublicKey) (*accounts.Account, bool, error) {
		accountKey := [32]byte(key)
		acct, getErr := accts.GetAccount(&accountKey)
		if getErr != nil || acct == nil {
			return nil, false, nil
		}
		return acct, true, nil
	}
	if parentBankSysvars == nil {
		// The first bank after bootstrap/resume converts the legacy ordered-replay
		// cache once. Every subsequent bank derives from its immutable parent.
		bankSysvars, err = sealevel.SnapshotLegacySysvarCache(block.Slot, loadCurrentSysvar)
		if err != nil {
			return nil, nil, 0, nil, fmt.Errorf("snapshot replay sysvars: %w", err)
		}
		var currentSysvars []*accounts.Account
		if err := bankSysvars.RangeAccountViews(func(key solana.PublicKey, _ *accounts.Account) error {
			if acct, found, _ := loadCurrentSysvar(key); found {
				currentSysvars = append(currentSysvars, acct)
			}
			return nil
		}); err != nil {
			return nil, nil, 0, nil, fmt.Errorf("enumerate replay sysvars: %w", err)
		}
		bankSysvars, err = bankSysvars.WithAccounts(currentSysvars...)
		if err != nil {
			return nil, nil, 0, nil, fmt.Errorf("apply current replay sysvars: %w", err)
		}
	} else {
		// Clock and SlotHashes change at bank start. Epoch/reward/feature staging
		// contributes any other changed sysvar accounts explicitly. Everything
		// else is immutable and shared with the parent snapshot without another
		// clone or decode.
		updates := make([]*accounts.Account, 0, 2+len(block.EpochUpdatedAccts))
		updateIndex := make(map[solana.PublicKey]int, cap(updates))
		addCurrent := func(key solana.PublicKey) error {
			acct, found, loadErr := loadCurrentSysvar(key)
			if loadErr != nil {
				return loadErr
			}
			if !found {
				return fmt.Errorf("updated bank sysvar %s is missing from slot accounts", key)
			}
			if idx, exists := updateIndex[key]; exists {
				updates[idx] = acct
				return nil
			}
			updateIndex[key] = len(updates)
			updates = append(updates, acct)
			return nil
		}
		if err := addCurrent(sealevel.SysvarClockAddr); err != nil {
			return nil, nil, 0, nil, err
		}
		if err := addCurrent(sealevel.SysvarSlotHashesAddr); err != nil {
			return nil, nil, 0, nil, err
		}
		for _, acct := range block.EpochUpdatedAccts {
			if acct != nil && sealevel.IsBankSysvarAccount(acct.Key) {
				if err := addCurrent(acct.Key); err != nil {
					return nil, nil, 0, nil, err
				}
			}
		}
		bankSysvars, err = parentBankSysvars.Derive(block.Slot, updates...)
		if err != nil {
			return nil, nil, 0, nil, fmt.Errorf("derive replay bank sysvars: %w", err)
		}
	}

	metrics.GlobalBlockReplay.AccountLoader.SysvarUpdates.AddTimingSince(phaseStart)
	return accts, parentAccts, publicationCapacity, bankSysvars, nil
}

func recordAccountLoaderBatchStats(dst *metrics.AccountLoader, src accountsdb.BatchReadStats) {
	dst.RequestedKeys = src.RequestedKeys
	dst.DurableKeys = src.DurableKeys
	dst.WorkingSetHits = src.WorkingSetHits
	dst.InProgressHits = src.InProgressHits
	dst.PendingFoldHits = src.PendingFoldHits
	dst.CacheHits = src.CacheHits
	dst.IndexHits = src.IndexHits
	dst.IndexMisses = src.IndexMisses
	dst.UniqueAppendVecs = src.UniqueAppendVecs
	dst.AppendVecChunks = src.AppendVecChunks
	dst.AppendVecAccounts = src.AppendVecAccounts
	dst.OpenFailures = src.OpenFailures
	dst.ReadFailures = src.ReadFailures
	dst.RetryAccounts = src.RetryAccounts
	dst.CommonCacheAdmissions = src.CommonCacheAdmissions
	dst.CommonCacheAdmissionsSkipped = src.CommonCacheAdmissionsSkipped
	dst.VoteCacheAdmissions = src.VoteCacheAdmissions
	dst.VoteCacheAdmissionsSkipped = src.VoteCacheAdmissionsSkipped
	dst.CachePublicationEpochRejects = src.CachePublicationEpochRejects
	dst.DecodedAccountObjects = src.DecodedAccountObjects
	dst.DecodedAccountBytes = src.DecodedAccountBytes
	dst.PlaceholderObjects = src.PlaceholderObjects
	dst.WorkingSetLookup.AddTiming(time.Duration(src.WorkingSetLookupNanoseconds))
	dst.InProgressLookup.AddTiming(time.Duration(src.InProgressNanoseconds))
	dst.AppendVecPinWait.AddTiming(time.Duration(src.AppendVecPinWaitNanoseconds))
	dst.ReadCacheEpochWait.AddTiming(time.Duration(src.ReadCacheEpochWaitNanoseconds))
	dst.CacheLookup.AddTiming(time.Duration(src.CacheLookupNanoseconds))
	dst.AdmissionFilter.AddTiming(time.Duration(src.AdmissionFilterNanoseconds))
	dst.IndexLookup.AddTiming(time.Duration(src.IndexLookupNanoseconds))
	dst.ReadPlanning.AddTiming(time.Duration(src.ReadPlanningNanoseconds))
	dst.AppendVecRead.AddTiming(time.Duration(src.AppendVecReadNanoseconds))
	dst.CachePublicationWait.AddTiming(time.Duration(src.CachePublicationWaitNanoseconds))
	dst.CachePublication.AddTiming(time.Duration(src.CachePublicationNanoseconds))
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

// epochBoundaryParentCtx reconstructs the small portion of the parent bank
// context consumed by epoch-transition code when the first executable block
// after startup crosses an epoch. This occurs when the epoch opens with one or
// more skipped slots, so lastSlotCtx has not yet been created in this process.
func epochBoundaryParentCtx(
	acctsDb *accountsdb.AccountsDb,
	block *b.Block,
	parentEpoch uint64,
	f *features.Features,
) *sealevel.SlotCtx {
	return &sealevel.SlotCtx{
		Accounts:      accounts.NewMemAccounts(),
		AccountsDb:    acctsDb,
		Slot:          block.ParentSlot,
		Epoch:         parentEpoch,
		Features:      f,
		Blockhash:     block.LastBlockhash,
		LastBlockhash: block.LastBlockhash,
	}
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

// LoadInitialEpochStakesCache is the single startup policy for choosing
// manifest versus persisted post-boundary epoch stakes. It is exported so the
// node can establish the exact replay stake view before advertising Votor.
func LoadInitialEpochStakesCache(mithrilState *state.MithrilState, resumeState *ResumeState, startEpoch, snapshotEpoch uint64) error {
	if resumeState != nil {
		epochsCrossed := startEpoch > snapshotEpoch
		if epochsCrossed && len(resumeState.ComputedEpochStakes) == 0 {
			return fmt.Errorf("resume at epoch %d (snapshot epoch %d) but no persisted epoch stakes found - cannot use stale manifest stakes (need fresh snapshot)", startEpoch, snapshotEpoch)
		}
		if len(resumeState.ComputedEpochStakes) > 0 {
			// Once replay has crossed a boundary, only its persisted effective
			// stakes are authoritative; never fall back to the snapshot manifest.
			for epoch, data := range resumeState.ComputedEpochStakes {
				loadedEpoch, err := global.DeserializeAndLoadEpochStakes(data)
				if err != nil {
					return fmt.Errorf("failed to load persisted epoch %d stakes: %w", epoch, err)
				}
				mlog.Log.Debugf("loaded persisted epoch stakes for epoch %d from state file", loadedEpoch)
			}
			if !global.HasEpochStakes(startEpoch) {
				return fmt.Errorf("missing required epoch stakes for current epoch %d - cannot resume (need fresh snapshot)", startEpoch)
			}
			return nil
		}
		// Same-epoch resume with no computed boundary state safely uses the
		// original manifest stake cache.
	}
	return buildInitialEpochStakesCache(mithrilState, startEpoch, snapshotEpoch)
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

// settleInFlightFoldAtCapacity turns the async fold worker into bounded
// backpressure only when the speculative tail crosses its hard cap. A fold
// already in flight was admitted through every finality/verification gate, so
// waiting for and applying it is safe. We deliberately do not enqueue work
// here: no in-flight fold at capacity means the normal loop could not admit
// one (or its previous fold failed), which remains a fail-closed halt.
// It returns whether the tail is still over capacity after settlement.
func settleInFlightFoldAtCapacity(
	tail *unrootedTail,
	promoter *asyncPromoter,
	tipSlot uint64,
	applyFoldOutcome func(*foldResult),
) bool {
	if tail == nil || !tail.OverCap() {
		return false
	}
	// ProcessBlock publishes its delta before the replay loop constructs the
	// resume context. Never let draining an older chunk mask a mid-slot ordering
	// bug by dropping HeldSlots below the cap while the current tip is still
	// unrecoverable.
	if tail.contexts[tipSlot] == nil {
		return true
	}
	if promoter != nil && promoter.inFlight {
		res := promoter.drain()
		if applyFoldOutcome != nil {
			applyFoldOutcome(res)
		}
	}
	return tail.OverCap()
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
	alpenglowMode := consensusOpts != nil && consensusOpts.Alpenglow
	replayFrontier := uint64(0)
	if startSlot > 0 {
		replayFrontier = startSlot - 1
	}
	global.SetReplayFrontier(replayFrontier)
	ResetChainTip()
	if alpenglowMode {
		// These maps are process-local and can otherwise retain a discarded fork
		// across the node's rooted-checkpoint recovery loop.
		global.ResetAlpenglowChainMetadata()
	}

	// Generate unique run ID for log correlation (only if not already set by startup)
	if CurrentRunID == "" {
		CurrentRunID = GenerateRunID()
	}
	// Fresh vote/stake dirty watermark for this run (gates the in-loop unwind).
	resetVoteStakeDirty()
	nextLeader := newNextLeaderCursor(identityFromTurbineKey(turbineIdentity))
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

	// RPC client for cluster data. Verifying/RPC-source flows may fetch blocks;
	// validator mode restricts it to control-plane queries.
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
	startEpoch := epochSchedule.GetEpoch(startSlot)
	currentEpoch := initialReplayEpoch(epochSchedule, startSlot, mithrilState.ManifestParentSlot, resumeState)
	var lastSlotCtx *sealevel.SlotCtx
	// Set only by a successful in-loop fork unwind. The next executable bank
	// must derive from this exact surviving parent snapshot, not the legacy
	// process-global cache left by the discarded suffix. Skipped slots leave it
	// untouched until that bank arrives.
	var unwoundParentBankSysvars *sealevel.BankSysvars
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
	expectedStatusRoot := mithrilState.ManifestParentSlot
	var statusParentBlockID solana.Hash
	var hasStatusParentBlockID bool
	var statusCheckpoint *state.TransactionStatusCheckpointRef
	if resumeState != nil {
		expectedStatusRoot = resumeState.ParentSlot
		statusParentBlockID = resumeState.ParentAlpenglowBlockID
		hasStatusParentBlockID = resumeState.HasParentAlpenglowBlockID
		if rootedCtx := mithrilState.LastRootedContext; rootedCtx != nil && rootedCtx.Slot == expectedStatusRoot {
			statusCheckpoint = rootedCtx.TransactionStatusCheckpoint
		}
	}
	transactionStatuses, err := loadTransactionStatusCacheForReplay(
		acctsDbPath,
		expectedStatusRoot,
		mithrilState.ManifestParentSlot,
		statusCheckpoint,
		statusParentBlockID,
		hasStatusParentBlockID,
	)
	if err != nil {
		result.Error = fmt.Errorf("initialize transaction AlreadyProcessed status cache: %w", err)
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
	isFirstSlotInEpoch := epochSchedule.FirstSlotInEpoch(startEpoch) == startSlot
	replayCtx.CurrentFeatures, featuresActivatedInFirstSlot, parentFeaturesActivatedInFirstSlot = scanAndEnableFeatures(acctsDb, replayCtx, startSlot, isFirstSlotInEpoch)
	if alpenglowMode {
		if err := validateAlpenglowRuntimeFeatureSet(replayCtx.CurrentFeatures, startSlot); err != nil {
			result.Error = err
			return result
		}
		applyAlpenglowRuntimeFeatureOverrides(replayCtx.CurrentFeatures, startSlot)
	}
	var initialLtHash *lthash.LtHash
	var initialNumSignatures uint64
	var initialLastBlockhash solana.Hash
	if resumeState != nil {
		initialLtHash = resumeState.AcctsLtHash
		initialNumSignatures = resumeState.NumSignatures
		initialLastBlockhash = solana.Hash(resumeState.LastBlockhash)
	} else {
		if encoded := mithrilState.ManifestAcctsLtHash; encoded != "" {
			raw, decodeErr := base64.StdEncoding.DecodeString(encoded)
			if decodeErr != nil {
				result.Error = fmt.Errorf("decode manifest accounts lt hash for initial chain tip: %w", decodeErr)
				return result
			}
			initialLtHash = new(lthash.LtHash)
			initialLtHash.InitWithHash(raw)
		}
		initialNumSignatures = mithrilState.ManifestSignatureCount
		if len(mithrilState.ManifestRecentBlockhashes) > 0 {
			if raw, decodeErr := base58.DecodeFromString(mithrilState.ManifestRecentBlockhashes[0].Blockhash); decodeErr == nil {
				initialLastBlockhash = solana.Hash(raw)
			}
		}
	}
	// Resume checkpoints persist the Alpenglow block ID and chained root, but
	// producer activation remains fail-closed until the first executable replay
	// block publishes a fully coherent parent. Restoring only those hashes here
	// would mix them with sysvar/fee state that is restored while configuring the
	// first block (and may never run across an initial certified-skip span).
	InitChainTip(initialLtHash, replayCtx.CurrentFeatures, initialNumSignatures, initialLastBlockhash, transactionStatuses.View())
	partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)

	// Load epoch stakes - persisted stakes on resume, state file on fresh start.
	// Node startup invokes the same helper before opening Votor so this second
	// call is an idempotent replay-side safety check, not a separate policy.
	snapshotEpoch := epochSchedule.GetEpoch(mithrilState.ManifestParentSlot)
	if err := LoadInitialEpochStakesCache(mithrilState, resumeState, startEpoch, snapshotEpoch); err != nil {
		result.Error = err
		return result
	}

	// Shreds before blocks: turbine shred signature verification needs every
	// available schedule from the repair epoch through the live epoch. On a
	// shreds-only node the first block can only arrive AS verified shreds, so
	// deferring this to first-block configuration deadlocks:
	// schedule <- first block <- verified shreds <- schedule. Keeping only the
	// resume epoch is also insufficient during a large catchup: current live
	// shreds would all be rejected before they can be spooled for later replay.
	if global.ManageLeaderSchedule() {
		epochs := global.GetAllCachedEpochs()
		slices.Sort(epochs)
		for _, epoch := range epochs {
			if epoch < startEpoch {
				continue
			}
			firstSlot := epochSchedule.FirstSlotInEpoch(epoch)
			if _, ok := global.LeaderForSlot(firstSlot); ok {
				continue
			}
			if _, err := PrepareLeaderScheduleLocal(epoch, epochSchedule, ""); err != nil {
				result.Error = fmt.Errorf("building leader schedule for cached epoch %d before block-source start: %w", epoch, err)
				return result
			}
			mlog.Log.Infof("leader schedule ready for epoch %d (shred verification live)", epoch)
		}
		if _, ok := global.LeaderForSlot(startSlot); !ok {
			result.Error = fmt.Errorf("leader schedule does not cover replay start slot %d (epoch %d)", startSlot, startEpoch)
			return result
		}
	}

	if err := initializeBlockHeight(rpcc, mithrilState, resumeState); err != nil {
		result.Error = err
		return result
	}
	// Alpenglow consensus engine (certificate-driven finality; nil-safe).
	var consensusEngine consensusengine.Engine
	if alpenglowMode && consensusOpts != nil {
		consensusEngine = consensusOpts.Engine
	}

	if consensusEngine != nil {
		// Certs must verify against their own epoch's validator set; without the
		// lookup the engine falls back to the latest set and cross-epoch certs
		// silently fail BLS (and the deferred-cert replay never triggers).
		if lookupSink, ok := consensusEngine.(consensusengine.AlpenglowEpochLookupSink); ok {
			lookupSink.SetAlpenglowEpochLookup(epochSchedule.GetEpoch)
		}
		_, _ = InstallCachedAlpenglowValidatorSets(consensusEngine, startEpoch)
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
	alpenglowForced := make(map[uint64]alpenglowFinalityExpectation)
	for _, ev := range mithrilState.AlpenglowEvidence {
		if !alpenglowMode {
			break
		}
		if ev.Slot <= mithrilState.LastRootedSlot {
			continue
		}
		want := alpenglowFinalityExpectation{Skip: ev.Skip, Conflict: ev.Conflict}
		if !want.Conflict {
			raw, err := hex.DecodeString(ev.Finalized)
			if err != nil || len(raw) != 32 {
				mlog.Log.Warnf("alpenglow evidence for slot %d has a malformed finalized id; treating as conflict", ev.Slot)
				want.Conflict = true
				want.Skip = false
			} else {
				copy(want.Block[:], raw)
				// Legacy evidence had no explicit skip bit. A zero finalized
				// identity with Conflict=false can only mean a finalized skip.
				if want.Block.IsZero() {
					want.Skip = true
				} else if want.Skip {
					mlog.Log.Warnf("alpenglow evidence for slot %d marks a skip with a nonzero finalized id; treating as conflict", ev.Slot)
					want.Conflict = true
					want.Skip = false
				}
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
		var checkpointAfterCommit func(*state.TransactionStatusCheckpointRef) error
		if consensusOpts != nil {
			checkpointAfterCommit = consensusOpts.TransactionStatusCheckpointAfterCommit
		}
		if hookErr := unrootedTailState.SetTransactionStatusCheckpointHooks(TransactionStatusCheckpointHooks{
			// Snapshot runs here on the replay loop during fold-job construction;
			// only its immutable bytes cross to the async worker.
			Snapshot: transactionStatuses.SnapshotThrough,
			Install: func(through uint64, payload []byte) (*state.TransactionStatusCheckpointRef, error) {
				return PrepareTransactionStatusCheckpoint(acctsDbPath, through, payload)
			},
			AfterCommit: checkpointAfterCommit,
		}); hookErr != nil {
			result.Error = fmt.Errorf("configure durable transaction status checkpoints: %w", hookErr)
			return result
		}
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
		mlog.Log.FileOnlyf("rooted-durable mode: canonical store stays rooted-only; replayed slots buffer in RAM until rooted (halt cap %d slots); folds run async off the replay loop", unrootedTailHaltCap)
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
		mlog.Log.FileOnlyf("trailing verifier active: lag=%d slots, budget=%d rps — folds gate on min(finality, verified)", TrailingVerifierCfg.LagSlots, TrailingVerifierCfg.MaxRPS)
	} else if unrootedTailState != nil && TrailingVerifierCfg.ValidatorFooterHash {
		mlog.Log.Infof("validator execution verification: RPC block metadata is disabled; received Alpenglow blocks must match their certified footer bank hash before certificate-finalized state can fold")
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
		if transactionStatuses.Root(promotedThrough) {
			mlog.Log.Infof("transaction status cache reconstructed complete %d-root coverage through durable slot %d",
				maxTransactionStatusRoots, promotedThrough)
		}
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
		global.PruneAlpenglowBlockIDsBefore(promotedThrough)
		global.PruneAlpenglowChainedRootsBefore(promotedThrough)
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
	// force-folds the trailing partial chunk (epoch-boundary settlement and
	// shutdown); force=false folds full chunks only.
	foldRootedPrefix := func(force bool) (halt bool) {
		if unrootedTailState == nil {
			return false
		}
		if safety, ok := consensusEngine.(consensusengine.AlpenglowSafetyStatus); ok {
			if err := safety.AlpenglowSafetyError(); err != nil {
				if result.Error == nil {
					result.Error = fmt.Errorf("ALPENGLOW SAFETY: %w; durable state remains at slot %d", err, mithrilState.LastRootedSlot)
				}
				mlog.Log.Errorf("ALPENGLOW SAFETY: refusing durable promotion after consensus fault: %v", err)
				return true
			}
		}
		// Apply any completed async fold first so the gates below see the
		// current durable frontier.
		applyFoldOutcome(promoter.poll())
		if lastRootedWatermark == 0 {
			return false
		}
		// In verifying mode the trailing verifier is the external execution
		// oracle; a divergence halts regardless of finality progress. Validator
		// mode has already enforced certified-footer/local bank-hash parity in
		// ProcessBlock and therefore has no trailing verifier here.
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
		// Verifying mode uses the dual watermark: certificate finality AND the
		// trailing verifier. Validator mode needs certificate finality only here
		// because every received block already passed certified-footer/local
		// bank-hash parity. Neither mode folds at/past a divergence floor.
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
			// Forced settlement: settle the worker first, then fold everything
			// (including the trailing partial chunk) synchronously through the
			// SAME gate-derived target. This is used before AccountsDB-wide epoch
			// scans as well as at shutdown, and can never fold a slot the normal
			// loop would refuse.
			if res := promoter.drain(); res != nil {
				applyFoldOutcome(res)
			}
			promotedThrough, rootedCtx, perr := unrootedTailState.flush(promoteThrough)
			if perr != nil {
				mlog.Log.Errorf("rooted-durable: forced fold stopped at slot %d: %v", promotedThrough, perr)
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
			// Votor QUIC socket to advertise in CRDS and the gossip identity —
			// without these the observer socket is never advertised
			// (gossip logs alpenglow=disabled) and a configured validator
			// identity never reaches turbine gossip (always ephemeral).
			TurbineAlpenglowAddr: turbineAlpenglowAddr,
			TurbineIdentity:      turbineIdentity,
			LeaderForSlot:        global.LeaderForSlot,
			BackupRpcEndpoints:   rpcBackups,
			StartSlot:            startSlot,
			EndSlot:              endSlot,
			BlockDir:             blockDir,
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
		opts.AlpenglowCandidateValidator = validateBlockTransactionMessages
		if co, ok := consensusEngine.(consensusengine.AlpenglowCandidateBlockObserver); ok {
			opts.AlpenglowCandidateBlockSink = co.ObserveAlpenglowCandidateBlock
		}
		if invalid, ok := consensusEngine.(consensusengine.AlpenglowInvalidBlockObserver); ok {
			opts.AlpenglowInvalidBlockSink = invalid.ObserveObjectivelyInvalidAlpenglowBlock
		}
		if firstShred, ok := consensusEngine.(consensusengine.AlpenglowFirstShredObserver); ok {
			opts.AlpenglowFirstShredSink = firstShred.ObserveAlpenglowFirstShred
		}
		// Cert-driven repair: the source's repair loop steers turbine toward
		// certified-but-unobserved blocks and cancels shred state for
		// certificate-skipped slots.
		if wb, ok := consensusEngine.(consensusengine.AlpenglowWantedBlocksSource); ok {
			opts.AlpenglowWantedBlocks = wb.AlpenglowWantedBlocks
			opts.AlpenglowSkipCertified = wb.SkipCertifiedAt
		}
		// Footer certs at ASSEMBLY time: staged catchup blocks carry the
		// certificates that prove decisions (including skips) for OLDER
		// slots. Feeding them only at replay time deadlocks a shreds-only
		// catchup behind a certificate-skipped slot — the proof of the skip
		// sits buffered one slot ahead of it. The engine dedupes, so the
		// replay-time ingestion below stays as-is.
		opts.AlpenglowFooterCertSink = func(raw []byte) {
			ingestAlpenglowFooterCertificate(consensusEngine, raw)
		}
	}
	if alpenglowMode && resumeState != nil && resumeState.HasParentAlpenglowBlockID {
		opts.InitialAlpenglowBlockID = resumeState.ParentAlpenglowBlockID
		opts.HasInitialAlpenglowBlockID = true
	}

	// Apply block fetching options if provided
	if blockFetchOpts != nil {
		opts.MaxRPS = blockFetchOpts.MaxRPS
		opts.MaxInflight = blockFetchOpts.MaxInflight
		opts.TipPollMs = blockFetchOpts.TipPollMs
		opts.TipSafetyMargin = blockFetchOpts.TipSafetyMargin
		opts.RepairCatchupMaxGapSlots = blockFetchOpts.RepairCatchupMaxGapSlots
		opts.RepairMaxRequestsPerSecond = blockFetchOpts.RepairMaxRequestsPerSecond
		opts.TurbineServeRepairAddr = blockFetchOpts.TurbineServeRepairAddr
		opts.DisableRPCBlockFetch = blockFetchOpts.DisableRPCBlockFetch
		opts.ShredSpoolDir = blockFetchOpts.ShredSpoolDir
		opts.LocalLeaderForSlot = blockFetchOpts.LocalLeaderForSlot
		opts.LocalBlocks = blockFetchOpts.LocalBlocks
		opts.GossipClient = blockFetchOpts.GossipClient
		opts.PrewarmBlocks = append(opts.PrewarmBlocks, blockFetchOpts.PrewarmBlocks...)
		opts.TurbineStakesForSlot = blockFetchOpts.TurbineStakesForSlot
		opts.TurbineEpochForSlot = blockFetchOpts.TurbineEpochForSlot
		opts.TurbineRootSlot = blockFetchOpts.TurbineRootSlot
		opts.TurbineUseChaCha8 = blockFetchOpts.TurbineUseChaCha8
		opts.TurbineDedupAddrs = blockFetchOpts.TurbineDedupAddrs

		// Mode thresholds
		opts.NearTipThreshold = blockFetchOpts.NearTipThreshold
		opts.CatchupThreshold = blockFetchOpts.CatchupThreshold
		opts.CatchupTipGateThreshold = blockFetchOpts.CatchupTipGateThreshold

		// Near-tip tuning
		opts.NearTipPollMs = blockFetchOpts.NearTipPollMs
		opts.NearTipLookahead = blockFetchOpts.NearTipLookahead
	}
	// Hand the boot-time prewarm over as late as possible: every extra
	// second collected is a slot repair never fetches. Handover stops the
	// prewarm receiver, freeing the turbine bind port for the source.
	if blockFetchOpts != nil && blockFetchOpts.TurbinePrewarm != nil {
		prewarmBlocks, prewarmDropped := blockFetchOpts.TurbinePrewarm.Handover()
		opts.PrewarmBlocks = append(opts.PrewarmBlocks, prewarmBlocks...)
		if len(prewarmBlocks) > 0 || prewarmDropped > 0 {
			mlog.Log.Infof("turbine prewarm handover: %d blocks collected during boot (%d dropped over capacity)", len(prewarmBlocks), prewarmDropped)
		}
	}

	blockStream := blockstream.NewBlockSource(opts)

	if !isLive {
		blockStream.DownloadInitialBlocks()
	}
	blockStreamDone := make(chan struct{})
	go func() {
		defer close(blockStreamDone)
		blockStream.Start()
	}()
	// A typed fork-switch error returns to runReplayWithRecovery, which starts a
	// new ReplayBlocks attempt in this process using the SAME Turbine address
	// and shred spool. The abandoned source must release both before that retry
	// is constructed; otherwise its receiver keeps the UDP port bound and the
	// replacement source can only back off until the stall watchdog fires.
	defer func() {
		blockStream.Stop()
		<-blockStreamDone
	}()

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

	// handleAlpenglowSwitch is shared by certificate contradictions and exact
	// parent-linked speculative fork transitions. Account-state correction is
	// identical; only the source rewind differs (certified sibling re-fetch vs
	// retaining an already assembled alternate child).
	handleAlpenglowSwitch := func(sw *CertifiedSwitch, rewindSource func() bool) bool {
		windowSwitches++
		if unrootedTailState == nil || promoter == nil {
			windowSwitchFallback++
			switchFallbackReasons["no-unrooted-tail"]++
			result.Error = sw
			mlog.Log.Warnf("%v — no speculative account-state tail is available for unwind", sw)
			return false
		}
		// Settle the in-flight fold before touching the overlay: the worker
		// reads chunk layers the unwind would evict. This MUST also happen
		// before rewinding the block source: if the applied fold moved the
		// durable frontier past the switch slot, mutating source ancestry first
		// would leave the source on a branch AccountsDB can no longer adopt.
		applyFoldOutcome(promoter.drain())
		if sw.Slot <= mithrilState.LastRootedSlot {
			windowSwitchFallback++
			switchFallbackReasons["durable-overlap"]++
			result.Error = sw
			mlog.Log.Warnf("%v — switch slot is at/below the durable watermark %d after settling the in-flight fold; deferring to the recovery loop", sw, mithrilState.LastRootedSlot)
			return false
		}
		if !rewindSource() {
			windowSwitchFallback++
			switchFallbackReasons["source-rewind"]++
			result.Error = fmt.Errorf("%w: block source rejected fork rewind", sw)
			mlog.Log.Warnf("%v — block source rejected the fork rewind", sw)
			return false
		}
		rs, parentBankSysvars, fallbackReason := tryInLoopUnwind(sw, unrootedTailState, mithrilState, epochSchedule, currentEpoch, partitionedRewardsInfo)
		if rs == nil {
			windowSwitchFallback++
			switchFallbackReasons[fallbackReason]++
			result.Error = sw
			mlog.Log.Warnf("%v — in-RAM unwind unavailable (%s); re-replaying from the rooted checkpoint", sw, fallbackReason)
			return false
		}
		if statusErr := transactionStatuses.Unwind(sw.Slot); statusErr != nil {
			windowSwitchFallback++
			switchFallbackReasons["status-root"]++
			result.Error = fmt.Errorf("%w: transaction status unwind refused: %v", sw, statusErr)
			mlog.Log.Warnf("%v", result.Error)
			return false
		}

		for slot := range alpenglowExecutedBlockIDs {
			if slot >= sw.Slot {
				delete(alpenglowExecutedBlockIDs, slot)
			}
		}
		global.DeleteAlpenglowBlockIDsFrom(sw.Slot)
		global.DeleteAlpenglowChainedRootsFrom(sw.Slot)
		resumeState = rs
		unwoundParentBankSysvars = parentBankSysvars
		lastSlotCtx = nil // next block configures from the rebuilt resume context
		replayCtx.Capitalization = rs.Capitalization
		global.SetBlockHeight(rs.ParentBlockHeight)
		if rs.TransactionCount != nil {
			global.SetTransactionCount(*rs.TransactionCount) // drop the discarded fork's txs
		}
		blockStream.SetLastExecutedSlot(rs.ParentSlot)
		global.SetReplayFrontier(rs.ParentSlot)
		ResetChainTip()
		windowSwitchInRAM++
		mlog.Log.Warnf("%v — unwound in RAM to executed parent slot %d; re-executing the selected chain (in-RAM switches this window: %d)", sw, rs.ParentSlot, windowSwitchInRAM)
		return true
	}

	for {
		// The collector is per replay attempt. Discarded candidates, skipped
		// slots, and typed-recovery exits must never leak timings into the next
		// successfully written slot (including a later ReplayBlocks invocation).
		metrics.GlobalBlockReplay = metrics.BlockReplay{}
		if ctx.Err() != nil {
			mlog.Log.Infof("context cancelled, stopping replay: %v", ctx.Err())
			result.WasCancelled = true
			break
		}

		var (
			block          *b.Block
			parentSwitch   *blockstream.AlpenglowParentSwitch
			ingressTimings *b.TurbineIngressTimings
			waitTime       time.Duration
			neededAt       time.Time // when replay asked the source for this slot
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
			block, parentSwitch = blockStream.NextBlockOrAlpenglowParentSwitch(ctx)
			if ingress, ok := block.CompleteTurbineReplayAdmission(time.Now()); ok {
				_ = statsd.Duration(statsd.TurbineReplayAdmission, ingress.ReplayAdmission, nil)
				ingressTimings = &ingress
			}

			waitTime = time.Since(neededAt)

			if stallDone != nil {
				close(stallDone)
			}
			if ctx.Err() != nil {
				mlog.Log.Infof("context cancelled while waiting for the next block: %v", ctx.Err())
				result.WasCancelled = true
				break
			}

			if parentSwitch != nil {
				if !parentSwitchNeedsStateUnwind(parentSwitch.SwitchSlot, currentExecutedAnchorSlot()) {
					// Repair can assemble far ahead of replay. If the alternate
					// branch is exposed before replay reaches the divergence, only
					// the source's queued speculative suffix needs replacement;
					// account state has nothing to unwind yet.
					windowSwitches++
					if !blockStream.RewindForAlpenglowParentSwitch(*parentSwitch) {
						windowSwitchFallback++
						switchFallbackReasons["source-rewind"]++
						result.Error = fmt.Errorf("alpenglow speculative switch at slot %d: block source rejected pre-execution branch selection", parentSwitch.SwitchSlot)
						break
					}
					mlog.Log.Warnf("ALPENGLOW speculative fork selected before execution: replaced queued suffix from slot %d with child %s at slot %d (replay currently at slot %d; no account-state unwind needed)",
						parentSwitch.SwitchSlot, parentSwitch.ChildID, parentSwitch.ChildSlot, currentExecutedAnchorSlot())
					continue
				}
				// A parent-linked child is speculative evidence, not a certificate.
				// Once the competing suffix is rooted, that suffix is the finalized
				// branch and a late sibling must be discarded. Settle an in-flight
				// fold first so the comparison uses the true durable frontier. The
				// old ordering rewound the source first, discovered the overlap only
				// afterwards, and halted with no retained rewind boundary.
				if promoter != nil {
					applyFoldOutcome(promoter.drain())
				}
				if parentSwitch.SwitchSlot <= mithrilState.LastRootedSlot {
					windowSwitches++
					if !blockStream.RejectAlpenglowParentSwitch(*parentSwitch) {
						windowSwitchFallback++
						switchFallbackReasons["source-reject"]++
						result.Error = fmt.Errorf("alpenglow speculative switch at slot %d: block source rejected rooted-branch retention", parentSwitch.SwitchSlot)
						break
					}
					mlog.Log.Warnf("ALPENGLOW rooted branch retained: discarded late speculative child %s at slot %d linking to ancestor %s at slot %d; switch slot %d is already durable through %d",
						parentSwitch.ChildID, parentSwitch.ChildSlot, parentSwitch.ParentID, parentSwitch.ParentSlot, parentSwitch.SwitchSlot, mithrilState.LastRootedSlot)
					continue
				}
				sw := &CertifiedSwitch{
					Slot:         parentSwitch.SwitchSlot,
					Executed:     alpenglowExecutedBlockIDs[parentSwitch.SwitchSlot],
					ParentLinked: true,
					ParentSlot:   parentSwitch.ParentSlot,
					ParentID:     parentSwitch.ParentID,
					ChildSlot:    parentSwitch.ChildSlot,
					ChildID:      parentSwitch.ChildID,
				}
				if handleAlpenglowSwitch(sw, func() bool {
					return blockStream.RewindForAlpenglowParentSwitch(*parentSwitch)
				}) {
					continue
				}
				break
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

			// An in-flight source send can race the first quarantine drain. Exact
			// emitted suffix IDs are hard-tombstoned before that send, so discard
			// any leaked descendant before it reaches consensus observation.
			if blockStream.IsObjectivelyInvalidAlpenglowBlock(block) {
				mlog.Log.Warnf("replay: discarding quarantined Alpenglow block %s at slot %d before consensus observation",
					solana.Hash(block.AlpenglowBlockID), block.Slot)
				continue
			}

			if !block.IsSkipped {
				if validationErr := validatePreConsensusTransactionStatuses(
					transactionStatuses, block, currentExecutedAnchorSlot(),
				); validationErr != nil {
					if !IsAlreadyProcessedTransactionError(validationErr) {
						result.Error = fmt.Errorf("pre-consensus block validation failed at slot %d: %w", block.Slot, validationErr)
						mlog.Log.Errorf("%v", result.Error)
						break
					}
					if quarantineErr := blockStream.QuarantineInvalidAlpenglowBlock(block); quarantineErr != nil {
						result.Error = fmt.Errorf("pre-consensus block validation failed at slot %d and the source could not quarantine it (%v): %w",
							block.Slot, quarantineErr, validationErr)
						mlog.Log.Errorf("%v", result.Error)
						break
					}
					mlog.Log.Warnf("replay: %v; exact Alpenglow candidate quarantined before ObserveBlock/voting", validationErr)
					continue
				}
			}

			// A complete Turbine candidate may carry an invalid reward certificate
			// and be skipped by the cluster. Reject it before ObserveBlock/voting
			// or any bank changes, while the selected parent is still untouched.
			if alpenglowMode && !block.IsSkipped {
				if validationErr := validatePreConsensusRewardCertificates(block, epochSchedule, block.AlpenglowShredVersion); validationErr != nil {
					if !IsInvalidRewardCertificateError(validationErr) {
						result.Error = fmt.Errorf("pre-consensus reward validation failed at slot %d: %w", block.Slot, validationErr)
						mlog.Log.Errorf("%v", result.Error)
						break
					}
					if quarantineErr := blockStream.QuarantineInvalidAlpenglowBlock(block); quarantineErr != nil {
						result.Error = fmt.Errorf("pre-consensus reward validation failed at slot %d and the source could not quarantine it (%v): %w",
							block.Slot, quarantineErr, validationErr)
						mlog.Log.Errorf("%v", result.Error)
						break
					}
					mlog.Log.Warnf("replay: %v; exact Alpenglow candidate quarantined before ObserveBlock/voting", validationErr)
					continue
				}
			}

			// Alpenglow: feed the observed block to the consensus engine. This is a
			// consensus boundary, not telemetry: a latched pool/tracker fault stops
			// replay before execution or durable promotion can continue.
			if consensusEngine != nil {
				if err := consensusEngine.ObserveBlock(ctx, consensusengine.BlockObservation{
					Block:  block,
					Source: blockStream.GetFetchStats().CurrentSource,
					At:     time.Now(),
				}); err != nil {
					result.Error = fmt.Errorf("consensus engine rejected block %d: %w", block.Slot, err)
					mlog.Log.Errorf("%v", result.Error)
					break
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
					if handleAlpenglowSwitch(sw, func() bool {
						blockStream.RewindForAlpenglowSwitch(sw.Slot, sw.Certified)
						return true
					}) {
						continue
					}
					break
				}
			}

			// Advance the certificate-finality watermark. Prefer engine
			// cert-finality; fall back to the RPC-attested finalized slot
			// (delegated) since an unstaked observer gets no certs. Poll
			// throttled (the RPC round-trip is slow).
			if alpenglowMode {
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
					// Terminal-quiet: the 100-slot summary's "consensus:
					// finalized slot" line carries this signal; per-advance
					// lines between slot rows are noise. Full detail in logs.
					mlog.Log.FileOnlyf("forkchoice: rooted watermark advanced to slot %d", rooted)
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
			// Zero is the explicit locally executed outcome for a skip. Parent-ID
			// gap inference is provisional; recording it lets a later certificate
			// naming a real block trigger the same in-RAM switch as a wrong sibling.
			if consensusEngine != nil && unrootedTailState != nil {
				alpenglowExecutedBlockIDs[block.Slot] = solana.Hash{}
			}
			// Look up leader for informational logging
			leaderStr := "unknown"
			if leader, exists := global.LeaderForSlot(block.Slot); exists {
				leaderStr = leader.String()
			}
			// Terminal: aligned skipped-slot line, reporting any PARTIAL shred
			// arrivals (leader sent something but the slot never became full).
			// Full detail (wait) stays in logs.
			partialShreds, repairedShreds, _, _ := blockStream.TurbineShredObservation(block.Slot)
			mlog.Log.InfofPrecise("%s", appendNextLeaderHint(buildSkippedStatsLine(block.Slot, leaderStr, partialShreds, repairedShreds), nextLeader.hint(block.Slot)))
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
			global.SetReplayFrontier(block.Slot)
			continue // Skip all execution - no state changes for skipped slots
		}

		if ctx.Err() != nil {
			mlog.Log.Infof("context cancelled, stopping replay: %v", ctx.Err())
			result.WasCancelled = true
			break
		}
		if ingressTimings != nil {
			record := &metrics.GlobalBlockReplay.TurbineIngress
			record.ShredCollection.AddTiming(ingressTimings.ShredCollection)
			record.CompletionQueueDelay.AddTiming(ingressTimings.CompletionQueueDelay)
			record.BlockDecode.AddTiming(ingressTimings.BlockDecode)
			record.TransactionParse.AddTiming(ingressTimings.TransactionParse)
			record.TransactionSigverify.AddTiming(ingressTimings.TransactionSigverify)
			record.ReplayAdmission.AddTiming(ingressTimings.ReplayAdmission)
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
			// Epoch transition code performs full AccountsDB scans for stake and
			// vote accounts. The normal promotion path deliberately retains a
			// rooted tail shorter than FoldBatchSlots in RAM, so without settling
			// that tail these scans can miss the last slots' vote credits and
			// compute the wrong rewards/bank hash. Force-fold only through the
			// already gated rooted parent, then fail closed if finality has not yet
			// made that exact parent durable.
			if unrootedTailState != nil {
				boundaryParentSlot := block.ParentSlot
				if lastSlotCtx != nil {
					boundaryParentSlot = lastSlotCtx.Slot
				}
				if foldRootedPrefix(true) {
					break
				}
				if err := requireDurableEpochBoundaryParent(block.Slot, boundaryParentSlot, mithrilState.LastRootedSlot); err != nil {
					result.Error = err
					mlog.Log.Errorf("%v", err)
					break
				}
				mlog.Log.FileOnlyf("epoch boundary: settled rooted parent %d into AccountsDB before epoch-wide scans", boundaryParentSlot)
			}

			mlog.Log.Infof("")
			mlog.Log.Infof("=== Epoch Boundary ===")
			mlog.Log.Infof("%d -> %d", currentEpoch, currentEpoch+1)

			var newlyActivatedFeatures, parentNewlyActivatedFeatures []*accounts.Account
			replayCtx.CurrentFeatures, newlyActivatedFeatures, parentNewlyActivatedFeatures = scanAndEnableFeatures(acctsDb, replayCtx, currentSlot, true)
			if alpenglowMode {
				applyAlpenglowRuntimeFeatureOverrides(replayCtx.CurrentFeatures, currentSlot)
			}
			partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
			boundaryParentCtx := lastSlotCtx
			if boundaryParentCtx == nil {
				boundaryParentCtx = epochBoundaryParentCtx(acctsDb, block, currentEpoch, replayCtx.CurrentFeatures)
			}
			partitionedRewardsInfo = handleEpochTransition(acctsDb, partitionedEpochRewardsEnabled, boundaryParentCtx, replayCtx, epochSchedule, replayCtx.CurrentFeatures, block, currentEpoch, rpcc, dbgOpts)
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

			// Alpenglow: install both the newly entered epoch and the future
			// leader-schedule epoch prepared at this boundary.
			if consensusEngine != nil {
				installEpochTransitionAlpenglowValidatorSets(
					consensusEngine,
					currentEpoch,
					epochSchedule.LeaderScheduleEpoch(block.Slot),
				)
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
			// First block being processed (including a start preceded by certified
			// skips). The fixed 243-slot calendar envelope is only an upper bound:
			// small clusters often finish all partitions in the first distribution
			// block. EpochRewards.Active is the persisted bank truth. Refusing every
			// restart in the envelope therefore rejects perfectly valid rooted
			// checkpoints after distribution has completed.
			withinRewardsEnvelope := rewards.IsWithinRewardsPeriod(block.Epoch, currentSlot, epochSchedule)
			if err := validatePartitionedRewardsResume(currentSlot, sealevel.SysvarCache.EpochRewards.Sysvar); err != nil {
				result.Error = err
				mlog.Log.Errorf("%v", err)
				break
			}
			if withinRewardsEnvelope {
				mlog.Log.Infof("partitioned rewards resume: slot %d is inside the calendar envelope, but the persisted EpochRewards sysvar is inactive; distribution already completed", currentSlot)
			}
		}

		block.Features = replayCtx.CurrentFeatures

		// post-epoch boundary rewards distribution
		if partitionedEpochRewardsEnabled && partitionedRewardsInfo != nil && currentSlot >= partitionedRewardsInfo.FirstStakingRewardSlot && partitionedRewardsInfo.NumRewardPartitionsRemaining > 0 {
			distributedAccts, parentDistributedAccts := distributePartitionedEpochRewardsForSlot(acctsDb, lastSlotCtx, block.EpochUpdatedAccts, replayCtx, partitionedRewardsInfo, currentSlot, block.BlockHeight)
			block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, distributedAccts...)
			block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parentDistributedAccts...)
		}

		block.EpochUpdatedAccts, block.ParentEpochUpdatedAccts = coalesceEpochAccountUpdates(
			block.EpochUpdatedAccts, block.ParentEpochUpdatedAccts,
		)

		processBlockStart := time.Now()
		metrics.GlobalBlockReplay.PreprocessBlock.AddTiming(processBlockStart.Sub(start))
		alpenglowClock := alpenglowMode
		parentBankSysvars := unwoundParentBankSysvars
		if lastSlotCtx != nil {
			parentBankSysvars = lastSlotCtx.BankSysvars()
		}
		if block.FromLocalProduction {
			lastSlotCtx, err = adoptLocalLeaderBlock(block, unrootedTailState, transactionStatuses, persistedHashes)
		} else {
			lastSlotCtx, err = ProcessBlock(acctsDb, block, epochSchedule, txParallelism, dbgOpts, persistedHashes, unrootedTailState, transactionStatuses, alpenglowClock, parentBankSysvars)
		}
		processBlockEnd := time.Now()
		metrics.GlobalBlockReplay.ProcessBlock.AddTiming(processBlockEnd.Sub(processBlockStart))
		if err != nil {
			mlog.Log.Errorf("error encountered during block replay: %s\n", err)
			result.Error = err
			// Clear any pending stake pubkeys from this failed block
			global.ClearPendingStakePubkeys()
			break
		}
		// The successful child now owns its derived snapshot. Any later bank uses
		// lastSlotCtx; the one-shot retained unwind bridge is no longer needed.
		unwoundParentBankSysvars = nil
		postProcessBlockStart := processBlockEnd
		statusViewStart := time.Now()
		statuses := transactionStatuses.View()
		metrics.GlobalBlockReplay.TransactionStatusView.AddTimingSince(statusViewStart)
		chainTipStart := time.Now()
		identity := ChainTipIdentity{}
		if alpenglowMode {
			identity = ChainTipIdentity{
				AlpenglowBlockID:              solana.Hash(block.AlpenglowBlockID),
				HasAlpenglowBlockID:           block.HasAlpenglowBlockID,
				AlpenglowChainedMerkleRoot:    solana.Hash(block.AlpenglowLastChainedRoot),
				HasAlpenglowChainedMerkleRoot: block.HasAlpenglowLastChainedRoot,
			}
		}
		if unrootedTailState != nil {
			UpdateChainTipFromSlotCtxWithBankMetadata(lastSlotCtx, block.Features, statuses, identity, ChainTipBankMetadata{BlockHeight: block.BlockHeight}, unrootedTailState)
		} else {
			UpdateChainTipFromSlotCtxWithBankMetadata(lastSlotCtx, block.Features, statuses, identity, ChainTipBankMetadata{BlockHeight: block.BlockHeight})
		}
		if alpenglowMode && block.HasAlpenglowBlockID {
			global.SetAlpenglowBlockID(block.Slot, solana.Hash(block.AlpenglowBlockID))
		}
		if alpenglowMode && block.HasAlpenglowLastChainedRoot {
			global.SetAlpenglowChainedMerkleRoot(block.Slot, solana.Hash(block.AlpenglowLastChainedRoot))
		}
		metrics.GlobalBlockReplay.ChainTipUpdate.AddTimingSince(chainTipStart)

		global.SetBlockHeight(block.BlockHeight)
		if trailingVerifier != nil {
			trailingVerifier.Record(buildSlotDigest(block))
		}
		if block.Slot > highestExecutedSlot {
			highestExecutedSlot = block.Slot // bounds the promotion-gate walk (shared fold path)
		}

		// Alpenglow: report the replayed slot's bankhash to the engine (drives cert
		// replay reconciliation). A consensus safety error halts; this callback is
		// no longer a telemetry-only observer.
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
				result.Error = fmt.Errorf("consensus engine rejected replay result %d: %w", block.Slot, err)
				mlog.Log.Errorf("%v", result.Error)
				break
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
			resumeContextStart := time.Now()
			txCountAtSlot := global.TransactionCount() // ProcessBlock already added this block's txs
			var recentBlockhashes *sealevel.SysvarRecentBlockhashes
			var slotHashes *sealevel.SysvarSlotHashes
			if bankSysvars := lastSlotCtx.BankSysvars(); bankSysvars != nil {
				if recent, ok := bankSysvars.RecentBlockhashes(); ok {
					recentBlockhashes = &recent
				}
				if hashes, ok := bankSysvars.SlotHashes(); ok {
					slotHashes = &hashes
				}
			}
			resumeCtx := &state.ResumeContext{
				Slot:                    block.Slot,
				Bankhash:                base58.Encode(lastSlotCtx.FinalBankhash),
				BlockHeight:             global.BlockHeight(),
				Epoch:                   block.Epoch,
				NumSignatures:           lastSlotCtx.NumSignatures,
				EvictedBlockhash:        base58.Encode(lastSlotCtx.LatestEvictedBlockhash[:]),
				Blockhash:               base58.Encode(lastSlotCtx.Blockhash[:]),
				RecentBlockhashes:       EncodeRecentBlockhashes(recentBlockhashes),
				SlotHashes:              EncodeSlotHashes(slotHashes),
				Capitalization:          replayCtx.Capitalization,
				SlotsPerYear:            replayCtx.SlotsPerYear,
				InflationInitial:        replayCtx.Inflation.Initial,
				InflationTerminal:       replayCtx.Inflation.Terminal,
				InflationTaper:          replayCtx.Inflation.Taper,
				InflationFoundation:     replayCtx.Inflation.FoundationVal,
				InflationFoundationTerm: replayCtx.Inflation.FoundationTerm,
				TransactionCount:        &txCountAtSlot,
			}
			if block.HasAlpenglowBlockID {
				resumeCtx.AlpenglowBlockID = solana.Hash(block.AlpenglowBlockID).String()
			}
			if block.HasAlpenglowLastChainedRoot {
				resumeCtx.AlpenglowChainedMerkleRoot = solana.Hash(block.AlpenglowLastChainedRoot).String()
			}
			if lastSlotCtx.AcctsLtHash != nil {
				resumeCtx.AcctsLtHash = base64.StdEncoding.EncodeToString(lastSlotCtx.AcctsLtHash.Hash())
			}
			// Persist the Clock from the completed bank snapshot, never from a
			// process-global cache that may already be constructing another bank.
			if bankSysvars := lastSlotCtx.BankSysvars(); bankSysvars != nil {
				if raw, ok := bankSysvars.RawView(sealevel.SysvarClockAddr); ok {
					resumeCtx.Clock = base64.StdEncoding.EncodeToString(raw)
				}
			}
			if lastSlotCtx.FeeRateGovernor != nil {
				resumeCtx.LamportsPerSignature = lastSlotCtx.FeeRateGovernor.LamportsPerSignature
				resumeCtx.PrevLamportsPerSig = lastSlotCtx.FeeRateGovernor.PrevLamportsPerSignature
			}
			unrootedTailState.SetContext(block.Slot, resumeCtx, lastSlotCtx.BankSysvars())
			metrics.GlobalBlockReplay.ResumeContext.AddTimingSince(resumeContextStart)
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
				if bankSysvars := lastSlotCtx.BankSysvars(); bankSysvars != nil {
					if recent, ok := bankSysvars.RecentBlockhashes(); ok {
						copyRecent := append(sealevel.SysvarRecentBlockhashes(nil), recent...)
						result.LastRecentBlockhashes = &copyRecent
					}
					if slotHashes, ok := bankSysvars.SlotHashes(); ok {
						copySlotHashes := append(sealevel.SysvarSlotHashes(nil), slotHashes...)
						result.LastSlotHashes = &copySlotHashes
					}
				}
				result.LastEvictedBlockhash = lastSlotCtx.LatestEvictedBlockhash
				result.LastBlockhash = lastSlotCtx.Blockhash
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

		// Rooted-durable backpressure runs only after this slot is complete and its
		// resume context is attached. If the async writer is merely slower than
		// replay, wait for its already-admitted fold and apply it before deciding
		// the tail is truly stalled. No new fold is admitted here: a still-full
		// tail remains a fail-closed error.
		if unrootedTailState != nil && unrootedTailState.OverCap() {
			hadInFlightFold := promoter != nil && promoter.inFlight
			stillOverCap := settleInFlightFoldAtCapacity(unrootedTailState, promoter, block.Slot, applyFoldOutcome)
			if stillOverCap {
				// Diagnose WHY rooting stalled. Validator mode verifies the footer
				// bank hash inline and has no trailing-verifier watermark; do not
				// misreport that safe path as verified=0.
				verifiedWM := uint64(0)
				verifiedDiag := "n/a/not-required"
				if trailingVerifier != nil {
					verifiedWM = trailingVerifier.VerifiedWatermark()
					verifiedDiag = fmt.Sprintf("%d", verifiedWM)
				} else if TrailingVerifierCfg.ValidatorFooterHash {
					verifiedDiag = "n/a/footer-inline"
				}
				diag := fmt.Sprintf("durable=%d finality=%d verified=%s replay=%d fold_in_flight=%t",
					mithrilState.LastRootedSlot, lastRootedWatermark, verifiedDiag, block.Slot, hadInFlightFold)
				hint := ""
				if trailingVerifier != nil && TrailingVerifierCfg.Required && lastRootedWatermark > verifiedWM+uint64(FoldBatchSlots) {
					hint = " — the trailing verifier cannot keep pace with replay (finality is ahead; the verifier fetches every block's execution metas via RPC, which is slow for very large blocks). Options: verifier.required=false to gate folds on certificate finality only, a faster/archival RPC for the verifier, or wait for peer bankhash cross-checking to replace the RPC oracle"
				}
				result.Error = fmt.Errorf("rooted-durable: speculative state exceeded %d held slots at slot %d; rooting stalled (%s)%s; halting", unrootedTailHaltCap, block.Slot, diag, hint)
				mlog.Log.Errorf("%v", result.Error)
				break
			}
		}

		// Stop before per-slot logging, summary generation, and metric export so
		// PostProcessBlock accounts only for execution-critical state publication.
		slotReplayEnd := time.Now()
		metrics.GlobalBlockReplay.PostProcessBlock.AddTiming(slotReplayEnd.Sub(postProcessBlockStart))
		slotReplayDuration := slotReplayEnd.Sub(start)
		metrics.GlobalBlockReplay.SlotReplay.AddTiming(slotReplayDuration)

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
		mlog.Log.InfofPrecise("%s", appendNextLeaderHint(buildSlotStatsLine(block.Slot, leaderStr, txnCount, totalCU, execMsLine, hasShreds, readySecsLine, asmSecsLine, block.RepairedShreds), nextLeader.hint(block.Slot)))
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
		global.SetReplayFrontier(block.Slot)

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
					progress += fmt.Sprintf(" | behind shred tip: replay %d, full %d", replayGap, fullGap)
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
					repairLine := fmt.Sprintf("  repair: %d slots, %d shreds", windowRepairedSlots, windowRepairedShreds)
					if quality := blockStream.RepairPeerQualityLine(); quality != "" {
						repairLine += " | " + quality
					}
					mlog.Log.InfofPrecise("%s", repairLine)
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
		preFlushEvidence := len(mithrilState.AlpenglowEvidence)
		foldRootedPrefix(true)
		// The cancel-path state save ran BEFORE this flush; if the flush
		// advanced the watermark or its finality gate recorded new evidence,
		// re-save so the state file matches the store and a restart cannot lose
		// a shutdown-only safety finding.
		stateChanged := mithrilState.LastRootedSlot > preFlushRooted || len(mithrilState.AlpenglowEvidence) > preFlushEvidence
		if result.StateWrittenOnCancel && onCancelWriteState != nil && stateChanged {
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

		// Capture blockhash context from the completed bank snapshot because
		// appendvec writes are not necessarily fsynced yet.
		if bankSysvars := lastSlotCtx.BankSysvars(); bankSysvars != nil {
			if recent, ok := bankSysvars.RecentBlockhashes(); ok {
				copyRecent := append(sealevel.SysvarRecentBlockhashes(nil), recent...)
				result.LastRecentBlockhashes = &copyRecent
			}
			if slotHashes, ok := bankSysvars.SlotHashes(); ok {
				copySlotHashes := append(sealevel.SysvarSlotHashes(nil), slotHashes...)
				result.LastSlotHashes = &copySlotHashes
			}
		}
		result.LastEvictedBlockhash = lastSlotCtx.LatestEvictedBlockhash
		result.LastBlockhash = lastSlotCtx.Blockhash
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
	adhRemoved := accountsDeltaHashRemoved(slotCtx)
	sysvarAccts := collectSysvarAcctsForAdh(slotCtx)
	var writableAccts []*accounts.Account
	var alreadyAdded map[solana.PublicKey]bool
	if !adhRemoved {
		writableAccts = make([]*accounts.Account, 0, len(slotCtx.WritableAccts)+len(block.UpdatedAccts)+len(rentAccts)+4)
		alreadyAdded = make(map[solana.PublicKey]bool, len(slotCtx.WritableAccts))
		for pk := range slotCtx.WritableAccts {
			acct, _ := slotCtx.GetAccount(pk)
			writableAccts = append(writableAccts, acct)
			alreadyAdded[pk] = true
		}
	}
	modifiedAccts := make([]*accounts.Account, 0, len(slotCtx.ModifiedAccts)+len(block.UpdatedAccts)+len(rentAccts)+4)

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
		alreadyPresent := slotCtx.ModifiedAccts[eua.Key]
		if !adhRemoved {
			alreadyPresent = alreadyAdded[eua.Key]
		}
		if alreadyPresent {
			continue
		}
		acct, err := slotCtx.GetAccount(eua.Key)
		if err != nil {
			acct, err = slotCtx.GetAccountFromAccountsDb(eua.Key)
			if err != nil {
				panic(fmt.Sprintf("unable to fetch %s from neither SlotCtx nor accountsdb for inclusion in bankhash in slot %d", eua.Key, slotCtx.Slot))
			}
		}
		if !adhRemoved {
			writableAccts = append(writableAccts, acct)
		}
		modifiedAccts = append(modifiedAccts, acct)
	}

	if !adhRemoved {
		writableAccts = append(writableAccts, rentAccts...)
	}
	modifiedAccts = append(modifiedAccts, rentAccts...)

	if !adhRemoved {
		writableAccts = append(writableAccts, sysvarAccts...)
	}
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

func newSlotCtx(block *b.Block, accts accounts.Accounts, parentAccts accounts.Accounts, acctsDb *accountsdb.AccountsDb, tail unrootedState, accountMapCapacity int) *sealevel.SlotCtx {
	writableMapCapacity := accountMapCapacity
	if block.Features != nil && block.Features.IsActive(features.RemoveAccountsDeltaHash) {
		writableMapCapacity = 0
	}
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
		ModifiedAccts: make(map[solana.PublicKey]bool, accountMapCapacity),
		WritableAccts: make(map[solana.PublicKey]bool, writableMapCapacity),

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

type blockTransactionExecutionPlan struct {
	messageIdentities   *b.PreparedTransactionMessageIdentities
	execute             []bool
	processedTxCount    uint64
	processedSignatures uint64
}

// validateBlockTransactionVersions is the authoritative bank-boundary feature
// check for transactions arriving from any block source. Native Turbine must be
// able to decode a V1 wire packet before the candidate bank is constructed, so
// ingress cannot safely decide whether V1 is active. The candidate block's
// feature snapshot can, and must reject pre-activation V1 before transaction
// execution can turn UnsupportedVersion into a nil-fee replay invariant panic.
func validateBlockTransactionVersions(block *b.Block) error {
	if block == nil {
		return errors.New("nil block")
	}
	v1Active := block.Features != nil && block.Features.IsActive(features.EnableTxV1)
	if v1Active {
		return nil
	}
	for idx, tx := range block.Transactions {
		if tx != nil && tx.Message.GetVersion() == solana.MessageVersionV1 {
			return fmt.Errorf("transaction %d uses V1 before EnableTxV1 activation: %w", idx, TxErrUnsupportedVersion)
		}
	}
	return nil
}

// planBlockTransactionExecution mirrors Agave's AlreadyProcessed check for
// transactions presented to one bank. A duplicate message makes the whole
// block invalid; replay must never silently filter it and compute a bank hash
// over different contents than the producer committed.
func planBlockTransactionExecution(block *b.Block) (blockTransactionExecutionPlan, error) {
	if block == nil {
		return blockTransactionExecutionPlan{}, errors.New("nil block")
	}
	identities, err := block.PrepareTransactionMessageIdentities()
	if err != nil {
		return blockTransactionExecutionPlan{}, err
	}
	plan := blockTransactionExecutionPlan{
		messageIdentities: identities,
		execute:           make([]bool, identities.Len()),
	}
	seen := make(map[[32]byte]int, len(block.Transactions))
	var duplicates *DuplicateTransactionMessagesError
	for idx, tx := range block.Transactions {
		messageHash := identities.Identity(idx).MessageHash
		if firstIndex, duplicate := seen[messageHash]; duplicate {
			if duplicates == nil {
				duplicates = &DuplicateTransactionMessagesError{Slot: block.Slot}
			}
			duplicates.DuplicateCount++
			if len(duplicates.Occurrences) < maxDuplicateTransactionOccurrences {
				duplicates.Occurrences = append(duplicates.Occurrences, DuplicateTransactionOccurrence{
					Index: idx, FirstIndex: firstIndex,
				})
			}
			continue
		}
		seen[messageHash] = idx
		plan.execute[idx] = true
		plan.processedTxCount++
		plan.processedSignatures += uint64(tx.Message.Header.NumRequiredSignatures)
	}
	if duplicates != nil {
		return plan, duplicates
	}
	return plan, nil
}

func validateBlockTransactionMessages(block *b.Block) error {
	if block == nil {
		return errors.New("nil block")
	}
	_, err := planBlockTransactionExecution(block)
	return err
}

// validatePreConsensusTransactionStatuses validates the candidate's immutable
// ingress lineage without mutating it before consensus observation. Turbine
// and Lightbringer populate SourceParentSlot during assembly, while ParentSlot
// is intentionally filled later by configureBlock. Sources without ingress
// parent metadata use their existing replay parent or the selected execution
// anchor. ProcessBlock validates the configured ParentSlot again before commit.
func validatePreConsensusTransactionStatuses(
	statuses *TransactionStatusCache,
	block *b.Block,
	selectedParentSlot uint64,
) error {
	if block == nil {
		return errors.New("nil block")
	}
	plan, err := planBlockTransactionExecution(block)
	if err != nil {
		return err
	}
	candidate := *block
	switch {
	case block.SourceParentSlot != 0:
		candidate.ParentSlot = block.SourceParentSlot
	case block.ParentSlot != 0:
		candidate.ParentSlot = block.ParentSlot
	default:
		candidate.ParentSlot = selectedParentSlot
	}
	return statuses.validateBlockWithPlan(&candidate, plan)
}

func sequentialTxLoop(slotCtx *sealevel.SlotCtx, sigverifyWg *sync.WaitGroup, block *b.Block, executionPlan blockTransactionExecutionPlan, dbgOpts *DebugOptions, shouldVerifySignatures bool) (fees.TxFeeInfoAccumulator, uint64) {
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	var totalComputeUnitsConsumed uint64
	// process & execute each transaction in turn
	for idx, tx := range block.Transactions {
		if !executionPlan.execute[idx] {
			continue
		}
		var txMeta *rpc.TransactionMeta
		if block.TxMetas != nil {
			txMeta = block.TxMetas[idx]
		}
		txFeeInfo, txComputeUnitsConsumed, txErr := ProcessTransaction(slotCtx, sigverifyWg, tx, txMeta, dbgOpts, nil, shouldVerifySignatures)
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

func parallelTxLoop(slotCtx *sealevel.SlotCtx, sigverifyWg *sync.WaitGroup, block *b.Block, rblock *b.Block, executionPlan blockTransactionExecutionPlan, txParallelism int, dbgOpts *DebugOptions, shouldVerifySignatures bool) (fees.TxFeeInfoAccumulator, uint64) {
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	txFeeInfos := make([]*fees.TxFeeInfo, len(block.Transactions))
	txComputeUnitsConsumed := make([]uint64, len(block.Transactions))
	errs := make([]error, len(block.Transactions))

	plannerBlock := block
	if rblock.FromLiveStream {
		plannerBlock = rblock
	}

	if canUseDependencyPlanner(plannerBlock) {
		do := make(chan int, len(block.Transactions))
		done := make(chan int, len(block.Transactions))
		plannerDone := make(chan struct{})
		go func() {
			defer close(plannerDone)
			plannerStart := time.Now()
			topsortPlannerStream(plannerBlock, do, done, func() {
				metrics.GlobalBlockReplay.DependencyPlannerBuild.AddTimingSince(plannerStart)
			})
			metrics.GlobalBlockReplay.DependencyPlannerDispatch.AddTimingSince(plannerStart)
		}()

		wg := &sync.WaitGroup{}
		wg.Add(txParallelism)
		for i := range txParallelism {
			go func() {
				defer wg.Done()
				for idx := range do {
					if !executionPlan.execute[idx] {
						done <- idx
						continue
					}
					tx := block.Transactions[idx]
					var txMeta *rpc.TransactionMeta
					if idx < len(rblock.TxMetas) {
						txMeta = rblock.TxMetas[idx]
					}
					txFeeInfos[idx], txComputeUnitsConsumed[idx], errs[idx] = ProcessTransaction(slotCtx, sigverifyWg, rblock.Transactions[idx], txMeta, dbgOpts, sealevel.BorrowedAccountArenas[i], shouldVerifySignatures)
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
					done <- idx
				}

			}()
		}

		wg.Wait()
		close(done)
		<-plannerDone
	} else if rblock.FromLiveStream {
		plannerDispatchStart := time.Now()
		var plannerBuildDuration time.Duration
		batchWg := &sync.WaitGroup{}
		workersWg := &sync.WaitGroup{}
		do := make(chan uint64, txParallelism)
		workersWg.Add(txParallelism)
		for i := range txParallelism {
			go func(workerIdx int) {
				defer workersWg.Done()
				for idx := range do {
					tx := block.Transactions[idx]
					var txMeta *rpc.TransactionMeta
					if int(idx) < len(rblock.TxMetas) {
						txMeta = rblock.TxMetas[idx]
					}
					txFeeInfos[idx], txComputeUnitsConsumed[idx], errs[idx] = ProcessTransaction(slotCtx, sigverifyWg, rblock.Transactions[idx], txMeta, dbgOpts, sealevel.BorrowedAccountArenas[workerIdx], shouldVerifySignatures)
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
					batchWg.Done()
				}
			}(i)
		}

		relaxIntraBatchAccountLocks := rblock.Features != nil &&
			rblock.Features.IsActive(features.RelaxIntraBatchAccountLocks)
		for _, entry := range rblock.Entries {
			plannerBuildStart := time.Now()
			batches := lightbringerEntryExecutionBatches(rblock.Transactions, entry, relaxIntraBatchAccountLocks)
			plannerBuildDuration += time.Since(plannerBuildStart)
			for _, batch := range batches {
				executable := 0
				for _, txIdx := range batch {
					if executionPlan.execute[txIdx] {
						executable++
					}
				}
				batchWg.Add(executable)
				for _, txIdx := range batch {
					if executionPlan.execute[txIdx] {
						do <- txIdx
					}
				}
				batchWg.Wait()
			}
		}
		close(do)
		workersWg.Wait()
		metrics.GlobalBlockReplay.DependencyPlannerBuild.AddTiming(plannerBuildDuration)
		metrics.GlobalBlockReplay.DependencyPlannerDispatch.AddTimingSince(plannerDispatchStart)
	} else {
		panic("dependency planner unavailable for non-Lightbringer block")
	}

	var totalComputeUnitsConsumed uint64
	for idx, txFeeInfo := range txFeeInfos {
		if !executionPlan.execute[idx] {
			continue
		}
		totalComputeUnitsConsumed += txComputeUnitsConsumed[idx]
		if txFeeInfo == nil {
			// This happens when IsTransactionAgeValid returns false (blockhash not found)
			tx := block.Transactions[idx]
			var recentBlockhashes sealevel.SysvarRecentBlockhashes
			if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
				recentBlockhashes, _ = bankSysvars.RecentBlockhashes()
			}
			mlog.Log.Errorf("txFeeInfo is nil for tx %s in slot %d", tx.Signatures[0], block.Slot)
			mlog.Log.Errorf("  tx blockhash: %s", tx.Message.RecentBlockhash)
			mlog.Log.Errorf("  LatestEvictedBlockhash: %x", slotCtx.LatestEvictedBlockhash[:8])
			if len(recentBlockhashes) > 0 {
				mlog.Log.Errorf("  RecentBlockhashes: %d entries, newest=%x, oldest=%x",
					len(recentBlockhashes), recentBlockhashes[0].Blockhash[:8], recentBlockhashes[len(recentBlockhashes)-1].Blockhash[:8])
			} else {
				mlog.Log.Errorf("  RecentBlockhashes: nil or empty!")
			}
			panic(fmt.Sprintf("txFeeInfo is nil - blockhash validation failed for tx %s", tx.Signatures[0]))
		}
		txFeeAccumulator.Add(txFeeInfo)
	}

	return txFeeAccumulator, totalComputeUnitsConsumed
}

// prepareDependencyPlannerBlock preserves unresolved transaction account keys
// only when the dependency planner needs them. Live replay explicitly plans
// from the execution block after address-table resolution, and sequential
// replay has no planner, so cloning either kind would be pure overhead.
func prepareDependencyPlannerBlock(block *b.Block, txParallelism int) (*b.Block, error) {
	if block == nil {
		return nil, errors.New("nil block")
	}
	if txParallelism <= 0 || block.FromLiveStream {
		return block, nil
	}

	unresolvedBlock := &b.Block{
		Transactions: make([]*solana.Transaction, len(block.Transactions)),
		TxMetas:      make([]*rpc.TransactionMeta, len(block.TxMetas)),
		Slot:         block.Slot,
		ParentSlot:   block.ParentSlot,
	}
	for i := range block.Transactions {
		clonedTx, err := cloneTransaction(block.Transactions[i])
		if err != nil {
			return nil, fmt.Errorf("clone transaction %d in slot %d: %w", i, block.Slot, err)
		}
		unresolvedBlock.Transactions[i] = clonedTx
		if i < len(block.TxMetas) && block.TxMetas[i] != nil {
			unresolvedBlock.TxMetas[i] = &rpc.TransactionMeta{}
			*unresolvedBlock.TxMetas[i] = *block.TxMetas[i]
		}
	}

	return unresolvedBlock, nil
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
	transactionStatuses *TransactionStatusCache,
	alpenglowClock bool,
	parentBankSysvars *sealevel.BankSysvars,
) (*sealevel.SlotCtx, error) {
	if block == nil {
		return nil, errors.New("validate transaction messages: nil block")
	}
	if err := validateBlockTransactionVersions(block); err != nil {
		return nil, fmt.Errorf("validate transaction versions for slot %d: %w", block.Slot, err)
	}
	executionPlanStart := time.Now()
	executionPlan, err := planBlockTransactionExecution(block)
	metrics.GlobalBlockReplay.TransactionExecutionPlan.AddTimingSince(executionPlanStart)
	if err != nil {
		return nil, fmt.Errorf("validate transaction messages for slot %d: %w", block.Slot, err)
	}
	statusValidationStart := time.Now()
	statusValidationErr := transactionStatuses.validateBlockWithPlan(block, executionPlan)
	metrics.GlobalBlockReplay.TransactionStatusValidation.AddTimingSince(statusValidationStart)
	if statusValidationErr != nil {
		return nil, fmt.Errorf("validate transaction statuses for slot %d: %w", block.Slot, statusValidationErr)
	}
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
					block.Slot, stage, stageDuration.Round(time.Second), len(block.Transactions), block.FromLiveStream)
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
	defer func() {
		sigverifyJoinStart := time.Now()
		sigverifyWg.Wait()
		metrics.GlobalBlockReplay.SignatureVerificationJoin.AddTimingSince(sigverifyJoinStart)
	}()
	plannerPreparationStart := time.Now()
	plannerBlock, err := prepareDependencyPlannerBlock(block, txParallelism)
	metrics.GlobalBlockReplay.DependencyPlannerPreparation.AddTimingSince(plannerPreparationStart)
	if err != nil {
		panic(fmt.Sprintf("unable to prepare dependency planner block for slot %d: %v", block.Slot, err))
	}

	start := time.Now()
	setReplayStage("load_accounts")
	loadAcctsRegion := trace.StartRegion(ctx, "LoadBlockAccounts")
	// In rooted-durable mode, block accounts/sysvars load through the unrooted
	// tail (overlay→durable) so execution sees confirmed-but-unrooted state.
	var blockSrc blockAccountSource = acctsDb
	if tail != nil {
		blockSrc = tail
	}
	accts, parentAccts, accountMapCapacity, bankSysvars, err := loadBlockAccountsAndUpdateSysvars(blockSrc, block, epochSchedule, alpenglowClock, parentBankSysvars)
	loadAcctsRegion.End()
	if err != nil {
		panic(fmt.Sprintf("unable to load slot accounts and update sysvars: %s", err))
	}
	if err := bankSysvars.ValidateForExecution(); err != nil {
		return nil, fmt.Errorf("invalid bank sysvar snapshot at slot %d: %w", block.Slot, err)
	}
	metrics.GlobalBlockReplay.LoadBlockAccounts.AddTimingSince(start)

	slotCtxSetupStart := time.Now()
	slotCtx := newSlotCtx(block, accts, parentAccts, acctsDb, tail, accountMapCapacity)
	if err := slotCtx.PublishBankSysvars(bankSysvars); err != nil {
		return nil, fmt.Errorf("publish bank sysvars at slot %d: %w", block.Slot, err)
	}
	bankEpochScheduleValue, ok := bankSysvars.EpochSchedule()
	if !ok {
		return nil, fmt.Errorf("bank-local EpochSchedule sysvar unavailable at slot %d", block.Slot)
	}
	bankEpochSchedule := &bankEpochScheduleValue
	if requireAlpenglowBlockFooter(block, slotCtx, alpenglowClock) {
		if err := validateAlpenglowFooterNanosecondClock(slotCtx, block); err != nil {
			return nil, err
		}
	}
	slotCtx.TraceCtx = ctx
	slotCtx.NumSignatures = executionPlan.processedSignatures
	metrics.GlobalBlockReplay.SlotCtxSetup.AddTimingSince(slotCtxSetupStart)
	var txFeeAccumulator fees.TxFeeInfoAccumulator
	var totalComputeUnitsConsumed uint64
	start = time.Now()

	setReplayStage("tx_loop")
	txLoopRegion := trace.StartRegion(ctx, "TxLoop")
	shouldVerifySignatures := !block.TransactionSignaturesVerified()
	if txParallelism > 0 {
		txFeeAccumulator, totalComputeUnitsConsumed = parallelTxLoop(slotCtx, &sigverifyWg, plannerBlock, block, executionPlan, txParallelism, dbgOpts, shouldVerifySignatures)
	} else {
		txFeeAccumulator, totalComputeUnitsConsumed = sequentialTxLoop(slotCtx, &sigverifyWg, block, executionPlan, dbgOpts, shouldVerifySignatures)
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
	bankRent, ok := slotCtx.BankSysvars().Rent()
	if !ok {
		return nil, fmt.Errorf("bank-local Rent sysvar unavailable at slot %d", block.Slot)
	}
	rentAccts := rent.CollectRentEagerly(slotCtx, &bankRent, bankEpochSchedule)
	metrics.GlobalBlockReplay.Rent.AddTimingSince(start)

	start = time.Now()
	setReplayStage("run_incinerator")
	runIncinerator(slotCtx)
	metrics.GlobalBlockReplay.RunIncinerator.AddTimingSince(start)

	// Alpenglow banks set the Clock timestamp from the block footer after execution.
	if alpenglowClock {
		footerClockStart := time.Now()
		if err := applyAlpenglowFooterClock(slotCtx, block, bankEpochSchedule); err != nil {
			metrics.GlobalBlockReplay.AlpenglowFooterClock.AddTimingSince(footerClockStart)
			return nil, fmt.Errorf("apply alpenglow footer clock at slot %d: %w", block.Slot, err)
		}
		if err := updateAlpenglowNanosecondClockAccount(slotCtx, block); err != nil {
			metrics.GlobalBlockReplay.AlpenglowFooterClock.AddTimingSince(footerClockStart)
			return nil, err
		}
		metrics.GlobalBlockReplay.AlpenglowFooterClock.AddTimingSince(footerClockStart)
		voteRewardsStart := time.Now()
		voteRewardsErr := ApplyAlpenglowVoteRewards(slotCtx, block, bankEpochSchedule, block.SkipRewardCert, block.NotarRewardCert, block.BlockFinalCert, block.AlpenglowShredVersion)
		metrics.GlobalBlockReplay.AlpenglowVoteRewards.AddTimingSince(voteRewardsStart)
		if voteRewardsErr != nil {
			return nil, voteRewardsErr
		}
	}
	if err := finalizeBankSysvars(slotCtx); err != nil {
		return nil, fmt.Errorf("finalize bank sysvars at slot %d: %w", block.Slot, err)
	}

	setReplayStage("compile_accounts")
	start = time.Now()
	writableAccts, modifiedAccts := compileWritableAndModifiedAccts(slotCtx, block, rentAccts)
	metrics.GlobalBlockReplay.CompileWritableAndModifiedAccts.AddTimingSince(start)
	start = time.Now()
	ensureParentsErr := ensureParentAccountsForModified(slotCtx, modifiedAccts)
	metrics.GlobalBlockReplay.EnsureParentAccountsForModified.AddTimingSince(start)
	if ensureParentsErr != nil {
		return nil, ensureParentsErr
	}

	start = time.Now()
	setReplayStage("bankhash")
	slotCtx.FinalBankhash = bankhash.CalculateBankHash(slotCtx, writableAccts, modifiedAccts, block.ParentBankhash, slotCtx.NumSignatures, block.Blockhash)
	metrics.GlobalBlockReplay.BankHash.AddTimingSince(start)
	if alpenglowClock {
		footerVerificationStart := time.Now()
		footerVerificationErr := verifyAlpenglowBlockFooter(slotCtx, block, alpenglowClock)
		metrics.GlobalBlockReplay.AlpenglowFooterVerification.AddTimingSince(footerVerificationStart)
		if footerVerificationErr != nil {
			writeFooterBankhashMismatchArtifact(footerVerificationErr, block, slotCtx, writableAccts, modifiedAccts)
			return nil, footerVerificationErr
		}
	}

	// Bankhash consensus enforcement is handled in the replay loop (not here)
	// because forkchoice is fed after ProcessBlock returns — checking here would
	// never see votes from recently submitted blocks and could deadlock.

	// Enter critical commit window - panics here may leave AccountsDB inconsistent
	commitSlot.Store(slotCtx.Slot)
	commitInProgress.Store(true)
	blockUpdateStart := time.Now()
	setReplayStage("store_accounts")
	persistedSlot := slotCtx.Slot
	persistedBankhash := append([]byte(nil), slotCtx.FinalBankhash...)
	persistedBlockSlot := block.Slot
	stakeIndexDir := filepath.Join(acctsDb.AcctsDir, "..")
	afterStoreAccounts := func() {
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
	// In rooted-durable mode the callback above is synchronous, so this includes
	// the complete critical-path overlay publication. Legacy StoreAccounts only
	// enqueues here; its asynchronous disk work deliberately belongs to no slot's
	// replay wall time and must never update a later slot's metrics record.
	metrics.GlobalBlockReplay.BlockUpdateAccounts.AddTimingSince(blockUpdateStart)
	if err != nil {
		return slotCtx, err
	}
	statusCommitStart := time.Now()
	statusErr := transactionStatuses.commitBlockWithPlan(block, executionPlan)
	metrics.GlobalBlockReplay.TransactionStatusCommit.AddTimingSince(statusCommitStart)
	if statusErr != nil {
		return nil, fmt.Errorf("commit transaction statuses for slot %d after bank state commit: %w", block.Slot, statusErr)
	}

	global.IncrTransactionCount(executionPlan.processedTxCount)
	setReplayStage("done")
	return slotCtx, err
}
