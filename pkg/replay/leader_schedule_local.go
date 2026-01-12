package replay

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/panjf2000/ants/v2"
	"golang.org/x/exp/rand"
)

const (
	// NumConsecutiveLeaderSlots matches Solana's NUM_CONSECUTIVE_LEADER_SLOTS
	NumConsecutiveLeaderSlots = 4
	// MaxMismatchLogsPerEpoch caps mismatch logging to avoid disk churn
	MaxMismatchLogsPerEpoch = 100
	// SampleBoundarySlots is how many slots to check at epoch boundaries
	SampleBoundarySlots = 2000
	// SampleRandomSlots is how many random slots to sample in the middle
	SampleRandomSlots = 1000
	// MaxMissingVoteCacheStakePercent is the maximum percentage of stake that can be
	// missing from VoteCache before we fail. Since local schedule is the source of truth,
	// missing VoteCache entries mean that stake is dropped from the schedule, which would
	// produce an incorrect schedule.
	// Set to 0 for zero tolerance - any missing stake is a hard failure.
	// The VoteCache rebuild from AccountsDB should ensure this never triggers.
	MaxMissingVoteCacheStakePercent = 0.0
	// DefaultVoteCacheRebuildConcurrency is the default number of concurrent workers
	// for rebuilding vote cache from AccountsDB at epoch boundaries.
	DefaultVoteCacheRebuildConcurrency = 16
)

var (
	mismatchLogOnce   sync.Once
	mismatchLogFile   *os.File
	mismatchLogWriter *bufio.Writer
	mismatchLogMu     sync.Mutex
)

// defaultLogsDir is the fallback directory for mismatch logs
const defaultLogsDir = "/mnt/mithril-logs"

// mismatchLogPath stores the resolved path for use in warnings
var mismatchLogPath string

// resolveLogsDir returns the leader_schedule subdirectory within the run directory.
// Creates a dedicated subdirectory to keep leader schedule files organized.
func resolveLogsDir(logsDir string) string {
	var baseDir string
	// First try mlog's directory (for run ID correlation)
	if mlogDir := mlog.GetLogDir(); mlogDir != "" {
		baseDir = mlogDir
	} else if logsDir != "" {
		baseDir = logsDir
	} else {
		baseDir = defaultLogsDir
	}
	// Return leader_schedule subdirectory
	return filepath.Join(baseDir, "leader_schedule")
}

// initMismatchLog creates/opens the mismatch log file (once per process).
// Uses the same log directory as Mithril's main logs with run ID for correlation.
func initMismatchLog(logsDir string) {
	mismatchLogOnce.Do(func() {
		logsDir = resolveLogsDir(logsDir)
		// Create directory if it doesn't exist
		if err := os.MkdirAll(logsDir, 0755); err != nil {
			mlog.Log.Warnf("failed to create mismatch log directory: %v", err)
			return
		}

		// Use run ID in filename for correlation with main log
		runID := mlog.GetRunID()
		var filename string
		if runID != "" {
			shortRunID := runID
			if len(shortRunID) > 8 {
				shortRunID = shortRunID[:8]
			}
			filename = fmt.Sprintf("mismatch_%s.log", shortRunID)
		} else {
			filename = "mismatch.log"
		}
		mismatchLogPath = filepath.Join(logsDir, filename)

		var err error
		mismatchLogFile, err = os.OpenFile(mismatchLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			mlog.Log.Warnf("failed to open leader schedule mismatch log: %v", err)
			return
		}
		mismatchLogWriter = bufio.NewWriter(mismatchLogFile)
		mlog.Log.FileOnlyf("leader schedule mismatch log: %s", mismatchLogPath)
	})
}

// getMismatchLogPath returns the path to the mismatch log file
func getMismatchLogPath() string {
	if mismatchLogPath != "" {
		return mismatchLogPath
	}
	return filepath.Join(resolveLogsDir(""), "leader_schedule_mismatch.log")
}

// flushMismatchLog flushes buffered writes (call at end of epoch validation)
func flushMismatchLog() {
	mismatchLogMu.Lock()
	defer mismatchLogMu.Unlock()
	if mismatchLogWriter != nil {
		mismatchLogWriter.Flush()
	}
}

// logMismatch writes a mismatch entry (capped per epoch to avoid disk churn)
func logMismatch(epoch, slot uint64, localLeader, rpcLeader solana.PublicKey,
	voteAcct solana.PublicKey, stake uint64, mismatchCount *int) {
	if mismatchLogWriter == nil || *mismatchCount >= MaxMismatchLogsPerEpoch {
		return
	}
	*mismatchCount++
	mismatchLogMu.Lock()
	defer mismatchLogMu.Unlock()
	entry := fmt.Sprintf("[%s] epoch=%d slot=%d local=%s rpc=%s vote_acct=%s stake=%d\n",
		time.Now().Format(time.RFC3339), epoch, slot, localLeader, rpcLeader, voteAcct, stake)
	mismatchLogWriter.WriteString(entry)
}

// logInputSnapshot writes the top stakes to the mismatch log for debugging
func logInputSnapshot(epoch uint64, voteAcctStakes map[solana.PublicKey]uint64,
	voteAcctMap map[solana.PublicKey]*epochstakes.VoteAccount) {
	if mismatchLogWriter == nil {
		return
	}

	// Sort by stake descending to get top 10
	type stakeEntry struct {
		voteAcct solana.PublicKey
		stake    uint64
		nodePk   solana.PublicKey
	}
	entries := make([]stakeEntry, 0, len(voteAcctStakes))
	for pk, stake := range voteAcctStakes {
		var nodePk solana.PublicKey
		if va := voteAcctMap[pk]; va != nil {
			nodePk = va.NodePubkey
		}
		entries = append(entries, stakeEntry{voteAcct: pk, stake: stake, nodePk: nodePk})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].stake > entries[j].stake
	})

	mismatchLogMu.Lock()
	defer mismatchLogMu.Unlock()

	mismatchLogWriter.WriteString(fmt.Sprintf("\n[INPUTS] epoch=%d top_stakes:\n", epoch))
	for i := 0; i < min(10, len(entries)); i++ {
		e := entries[i]
		mismatchLogWriter.WriteString(fmt.Sprintf("  %d. vote=%s node=%s stake=%d\n",
			i+1, e.voteAcct, e.nodePk, e.stake))
	}
}

// VoteCacheRebuildError holds info about a failed vote account for logging
type VoteCacheRebuildError struct {
	VoteAcct solana.PublicKey
	Stake    uint64
	Reason   string
	Err      error
}

// RebuildVoteCacheFromAccountsDB rebuilds the VoteCache from AccountsDB for all vote accounts
// in the stake map. This ensures correctness at epoch boundaries by reading the canonical
// state directly from AccountsDB.
//
// Parameters:
//   - acctsDb: the AccountsDB instance
//   - slot: the slot at which to read account state (typically lastSlotCtx.Slot)
//   - voteAcctStakes: the stake map for the new epoch (vote account -> stake)
//   - maxConcurrency: number of concurrent workers (0 = use default)
//
// Returns error if any vote account is missing or has invalid state.
// This is a blocking operation and should be called before building the leader schedule.
func RebuildVoteCacheFromAccountsDB(
	acctsDb *accountsdb.AccountsDb,
	slot uint64,
	voteAcctStakes map[solana.PublicKey]uint64,
	maxConcurrency int,
) error {
	if maxConcurrency <= 0 {
		maxConcurrency = DefaultVoteCacheRebuildConcurrency
	}

	startTime := time.Now()
	totalAccounts := len(voteAcctStakes)
	var zeroStakeCount int
	for _, stake := range voteAcctStakes {
		if stake == 0 {
			zeroStakeCount++
		}
	}
	nonZeroAccounts := totalAccounts - zeroStakeCount

	mlog.Log.FileOnlyf("vote cache rebuild: starting slot=%d accounts=%d (non-zero=%d) concurrency=%d",
		slot, totalAccounts, nonZeroAccounts, maxConcurrency)

	// Counters for stats (use atomics for thread safety)
	var successCount atomic.Int64
	var missingCount atomic.Int64
	var unmarshalErrCount atomic.Int64
	var zeroNodePkCount atomic.Int64
	var missingStake atomic.Uint64
	var unmarshalErrStake atomic.Uint64
	var zeroNodePkStake atomic.Uint64

	// Track first few errors for each category (with mutex for thread safety)
	const maxErrorsPerCategory = 5
	var errorsMu sync.Mutex
	var missingErrors []VoteCacheRebuildError
	var unmarshalErrors []VoteCacheRebuildError
	var zeroNodePkErrors []VoteCacheRebuildError

	// Track first error for reporting (use sync.Once to capture exactly one error)
	var firstError error
	var firstErrorOnce sync.Once

	// Create worker pool
	var wg sync.WaitGroup
	pool, err := ants.NewPoolWithFunc(maxConcurrency, func(i interface{}) {
		defer wg.Done()

		item := i.(struct {
			pk    solana.PublicKey
			stake uint64
		})

		// Read vote account from AccountsDB
		voteAcct, err := acctsDb.GetAccount(slot, item.pk)
		if err != nil {
			missingCount.Add(1)
			missingStake.Add(item.stake)
			errorsMu.Lock()
			if len(missingErrors) < maxErrorsPerCategory {
				missingErrors = append(missingErrors, VoteCacheRebuildError{
					VoteAcct: item.pk,
					Stake:    item.stake,
					Reason:   "not_found_in_accountsdb",
					Err:      err,
				})
			}
			errorsMu.Unlock()
			firstErrorOnce.Do(func() {
				firstError = fmt.Errorf("missing vote account %s (stake=%d): %w", item.pk, item.stake, err)
			})
			return
		}

		// Unmarshal vote state
		versionedVoteState, err := sealevel.UnmarshalVersionedVoteState(voteAcct.Data)
		if err != nil {
			unmarshalErrCount.Add(1)
			unmarshalErrStake.Add(item.stake)
			errorsMu.Lock()
			if len(unmarshalErrors) < maxErrorsPerCategory {
				unmarshalErrors = append(unmarshalErrors, VoteCacheRebuildError{
					VoteAcct: item.pk,
					Stake:    item.stake,
					Reason:   fmt.Sprintf("unmarshal_failed (data_len=%d)", len(voteAcct.Data)),
					Err:      err,
				})
			}
			errorsMu.Unlock()
			firstErrorOnce.Do(func() {
				firstError = fmt.Errorf("failed to unmarshal vote account %s (stake=%d): %w", item.pk, item.stake, err)
			})
			return
		}

		// Validate NodePubkey is non-zero
		nodePk := versionedVoteState.NodePubkey()
		var zeroPk solana.PublicKey
		if nodePk == zeroPk {
			zeroNodePkCount.Add(1)
			zeroNodePkStake.Add(item.stake)
			errorsMu.Lock()
			if len(zeroNodePkErrors) < maxErrorsPerCategory {
				zeroNodePkErrors = append(zeroNodePkErrors, VoteCacheRebuildError{
					VoteAcct: item.pk,
					Stake:    item.stake,
					Reason:   "zero_nodepubkey",
				})
			}
			errorsMu.Unlock()
			firstErrorOnce.Do(func() {
				firstError = fmt.Errorf("vote account %s has zero NodePubkey (stake=%d)", item.pk, item.stake)
			})
			return
		}

		// Update VoteCache
		global.PutVoteCacheItem(item.pk, versionedVoteState)
		successCount.Add(1)
	})
	if err != nil {
		return fmt.Errorf("failed to create worker pool: %w", err)
	}
	defer pool.Release()

	// Submit all vote accounts to the pool
	for pk, stake := range voteAcctStakes {
		if stake == 0 {
			continue // Skip zero-stake accounts
		}
		wg.Add(1)
		item := struct {
			pk    solana.PublicKey
			stake uint64
		}{pk: pk, stake: stake}
		if err := pool.Invoke(item); err != nil {
			wg.Done()
			return fmt.Errorf("failed to submit work to pool: %w", err)
		}
	}

	// Wait for all workers to complete
	wg.Wait()

	duration := time.Since(startTime)

	// Calculate total stake for percentage
	var totalStake uint64
	for _, stake := range voteAcctStakes {
		totalStake += stake
	}
	successStake := totalStake - missingStake.Load() - unmarshalErrStake.Load() - zeroNodePkStake.Load()

	// Terminal: single line summary
	skipped := nonZeroAccounts - int(successCount.Load())
	mlog.Log.Infof("vote cache: loaded=%d skipped=%d duration=%v",
		successCount.Load(), skipped, duration)

	// File only: detailed results
	mlog.Log.FileOnlyf("vote cache rebuild details: slot=%d duration=%v", slot, duration)
	mlog.Log.FileOnlyf("  accounts: total=%d non_zero=%d success=%d",
		totalAccounts, nonZeroAccounts, successCount.Load())
	mlog.Log.FileOnlyf("  stake: total=%d success=%d (%.2f%%)",
		totalStake, successStake, float64(successStake)/float64(totalStake)*100)

	// Check for any failures
	missing := missingCount.Load()
	unmarshalErr := unmarshalErrCount.Load()
	zeroNodePk := zeroNodePkCount.Load()
	totalFailed := missing + unmarshalErr + zeroNodePk

	if totalFailed > 0 {
		totalFailedStake := missingStake.Load() + unmarshalErrStake.Load() + zeroNodePkStake.Load()
		failedPercent := float64(totalFailedStake) / float64(totalStake) * 100

		// File only: detailed failure info (always log for debugging)
		mlog.Log.FileOnlyf("vote cache rebuild failures:")
		mlog.Log.FileOnlyf("  slot=%d", slot)
		mlog.Log.FileOnlyf("  failures: missing=%d (stake=%d) unmarshal_err=%d (stake=%d) zero_nodepk=%d (stake=%d)",
			missing, missingStake.Load(), unmarshalErr, unmarshalErrStake.Load(), zeroNodePk, zeroNodePkStake.Load())
		mlog.Log.FileOnlyf("  total_failed=%d total_failed_stake=%d (%.4f%% of total)",
			totalFailed, totalFailedStake, failedPercent)

		// File only: first few errors in each category
		if len(missingErrors) > 0 {
			mlog.Log.FileOnlyf("  missing_accounts (first %d):", len(missingErrors))
			for i, e := range missingErrors {
				mlog.Log.FileOnlyf("    %d. vote=%s stake=%d err=%v", i+1, e.VoteAcct, e.Stake, e.Err)
			}
		}
		if len(unmarshalErrors) > 0 {
			mlog.Log.FileOnlyf("  unmarshal_errors (first %d):", len(unmarshalErrors))
			for i, e := range unmarshalErrors {
				mlog.Log.FileOnlyf("    %d. vote=%s stake=%d reason=%s err=%v", i+1, e.VoteAcct, e.Stake, e.Reason, e.Err)
			}
		}
		if len(zeroNodePkErrors) > 0 {
			mlog.Log.FileOnlyf("  zero_nodepk_accounts (first %d):", len(zeroNodePkErrors))
			for i, e := range zeroNodePkErrors {
				mlog.Log.FileOnlyf("    %d. vote=%s stake=%d", i+1, e.VoteAcct, e.Stake)
			}
		}

		// Small percentage of unavailable vote accounts is expected on mainnet (dead/closed validators)
		// Only ERROR if significant stake is missing - otherwise it's just noise
		if failedPercent > 5.0 {
			mlog.Log.Errorf("VOTE CACHE REBUILD: slot=%d skipped=%d (%.4f%% stake) - exceeds threshold",
				slot, totalFailed, failedPercent)
			if firstError != nil {
				return fmt.Errorf("vote cache rebuild: %d unavailable (%.4f%% stake): %w",
					totalFailed, failedPercent, firstError)
			}
			return fmt.Errorf("vote cache rebuild: %d unavailable (%.4f%% stake)",
				totalFailed, failedPercent)
		}

		// Expected mainnet behavior - log to file only
		mlog.Log.FileOnlyf("vote cache rebuild: slot=%d skipped=%d unavailable vote accounts (%.4f%% stake)",
			slot, totalFailed, failedPercent)
		return nil
	}

	mlog.Log.FileOnlyf("  result: SUCCESS (all %d non-zero accounts rebuilt)", nonZeroAccounts)
	return nil
}

// StakeEntry holds a vote account and its stake for logging
type StakeEntry struct {
	VoteAcct   solana.PublicKey
	NodePubkey solana.PublicKey
	Stake      uint64
	Reason     string // For skipped entries: "zero_stake", "missing_vote_acct", "zero_nodepk"
}

// dumpFullScheduleData writes complete validator data to CSV files for debugging.
// Creates epoch-specific files in the logs directory with ALL validators.
// Includes run ID in filename to prevent overwriting on re-runs.
func dumpFullScheduleData(
	epoch uint64,
	source string, // "snapshot", "vote_cache", or "rpc"
	validEntries []StakeEntry,
	skippedEntries []StakeEntry,
	totalStake uint64,
	logsDir string,
) {
	logsDir = resolveLogsDir(logsDir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		mlog.Log.Warnf("dumpFullScheduleData: failed to create logs dir: %v", err)
		return
	}

	// Get short run ID for filename (prevents overwriting on re-runs)
	runID := mlog.GetRunID()
	shortRunID := ""
	if runID != "" {
		shortRunID = runID
		if len(shortRunID) > 8 {
			shortRunID = shortRunID[:8]
		}
		shortRunID = "_" + shortRunID
	}

	// Write validators CSV
	validatorsFile := filepath.Join(logsDir, fmt.Sprintf("epoch%d_%s%s_validators.csv", epoch, source, shortRunID))
	if err := writeValidatorsCSV(validatorsFile, epoch, source, validEntries, totalStake); err != nil {
		mlog.Log.Warnf("dumpFullScheduleData: failed to write validators CSV: %v", err)
	} else {
		mlog.Log.FileOnlyf("leader schedule validators dumped to: %s (%d entries)", validatorsFile, len(validEntries))
	}

	// Write skipped CSV if there are any
	if len(skippedEntries) > 0 {
		skippedFile := filepath.Join(logsDir, fmt.Sprintf("epoch%d_%s%s_skipped.csv", epoch, source, shortRunID))
		if err := writeSkippedCSV(skippedFile, epoch, skippedEntries); err != nil {
			mlog.Log.Warnf("dumpFullScheduleData: failed to write skipped CSV: %v", err)
		} else {
			mlog.Log.FileOnlyf("leader schedule skipped accounts dumped to: %s (%d entries)", skippedFile, len(skippedEntries))
		}
	}
}

// dumpFullScheduleDataWithSummary writes validators CSV, skipped CSV, and a summary file.
// This is the preferred function when all metadata is available.
// Includes run ID in filenames to prevent overwriting on re-runs.
func dumpFullScheduleDataWithSummary(
	validEntries []StakeEntry,
	skippedEntries []StakeEntry,
	summary ScheduleSummary,
	logsDir string,
) {
	logsDir = resolveLogsDir(logsDir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		mlog.Log.Warnf("dumpFullScheduleDataWithSummary: failed to create logs dir: %v", err)
		return
	}

	epoch := summary.BlockEpoch
	source := summary.Source

	// Get short run ID for filename (prevents overwriting on re-runs)
	shortRunID := ""
	if summary.RunID != "" {
		shortRunID = summary.RunID
		if len(shortRunID) > 8 {
			shortRunID = shortRunID[:8]
		}
		shortRunID = "_" + shortRunID
	}

	// Write validators CSV
	validatorsFile := filepath.Join(logsDir, fmt.Sprintf("epoch%d_%s%s_validators.csv", epoch, source, shortRunID))
	if err := writeValidatorsCSV(validatorsFile, epoch, source, validEntries, summary.FilteredStake); err != nil {
		mlog.Log.Warnf("dumpFullScheduleDataWithSummary: failed to write validators CSV: %v", err)
	} else {
		mlog.Log.FileOnlyf("leader schedule validators dumped to: %s (%d entries)", validatorsFile, len(validEntries))
	}

	// Write skipped CSV if there are any
	if len(skippedEntries) > 0 {
		skippedFile := filepath.Join(logsDir, fmt.Sprintf("epoch%d_%s%s_skipped.csv", epoch, source, shortRunID))
		if err := writeSkippedCSV(skippedFile, epoch, skippedEntries); err != nil {
			mlog.Log.Warnf("dumpFullScheduleDataWithSummary: failed to write skipped CSV: %v", err)
		} else {
			mlog.Log.FileOnlyf("leader schedule skipped accounts dumped to: %s (%d entries)", skippedFile, len(skippedEntries))
		}
	}

	// Write summary file
	summaryFile := filepath.Join(logsDir, fmt.Sprintf("epoch%d_%s%s_summary.txt", epoch, source, shortRunID))
	if err := writeSummaryFile(summaryFile, summary); err != nil {
		mlog.Log.Warnf("dumpFullScheduleDataWithSummary: failed to write summary: %v", err)
	} else {
		mlog.Log.FileOnlyf("leader schedule summary dumped to: %s", summaryFile)
	}
}

// DumpTieBreakDebug writes tie-break debugging info to a file.
// This verifies that equal-stake validators are sorted by pubkey DESC (Agave behavior).
func DumpTieBreakDebug(
	epoch uint64,
	voteAcctStakes map[solana.PublicKey]uint64,
	voteAcctMap map[solana.PublicKey]*epochstakes.VoteAccount,
	logsDir string,
) {
	logsDir = resolveLogsDir(logsDir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		mlog.Log.Warnf("DumpTieBreakDebug: failed to create logs dir: %v", err)
		return
	}

	runID := mlog.GetRunID()
	shortRunID := ""
	if runID != "" {
		shortRunID = runID
		if len(shortRunID) > 8 {
			shortRunID = shortRunID[:8]
		}
		shortRunID = "_" + shortRunID
	}

	filename := fmt.Sprintf("epoch%d_tiebreak%s.txt", epoch, shortRunID)
	filePath := filepath.Join(logsDir, filename)

	f, err := os.Create(filePath)
	if err != nil {
		mlog.Log.Warnf("DumpTieBreakDebug: failed to create file: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	// Get sorted stakes with tie-break info
	allEntries, tieGroups := leaderschedule.GetSortedStakesDebug(voteAcctMap, voteAcctStakes)

	w.WriteString("# Tie-Break Debug for Leader Schedule\n")
	w.WriteString(fmt.Sprintf("# Epoch: %d\n", epoch))
	w.WriteString(fmt.Sprintf("# Total validators: %d\n", len(allEntries)))
	w.WriteString(fmt.Sprintf("# Tie groups (equal stake): %d\n", len(tieGroups)))
	w.WriteString(fmt.Sprintf("# Generated: %s\n", time.Now().UTC().Format(time.RFC3339)))
	w.WriteString("#\n")
	w.WriteString("# Expected behavior: within each tie group, pubkeys should be sorted DESC (higher bytes first)\n")
	w.WriteString("# BytesCmp shows comparison vs previous entry: -1 means current < previous (correct for DESC)\n")
	w.WriteString("#\n\n")

	if len(tieGroups) == 0 {
		w.WriteString("No tie groups found - all validators have unique stake.\n")
		mlog.Log.FileOnlyf("tie-break debug: epoch=%d no ties found", epoch)
		return
	}

	// Sort tie groups by stake descending for consistent output
	type tieGroupInfo struct {
		stake   uint64
		entries []leaderschedule.TieBreakEntry
	}
	var sortedGroups []tieGroupInfo
	for stake, entries := range tieGroups {
		sortedGroups = append(sortedGroups, tieGroupInfo{stake: stake, entries: entries})
	}
	sort.Slice(sortedGroups, func(i, j int) bool {
		return sortedGroups[i].stake > sortedGroups[j].stake
	})

	for _, group := range sortedGroups {
		w.WriteString(fmt.Sprintf("## Tie group: stake=%d (%d validators)\n", group.stake, len(group.entries)))
		w.WriteString("rank,node_pubkey,stake,first_8_bytes_hex,bytes_cmp_vs_prev\n")
		for _, entry := range group.entries {
			w.WriteString(fmt.Sprintf("%d,%s,%d,%x,%d\n",
				entry.Rank, entry.NodePk.String(), entry.Stake, entry.RawBytes, entry.BytesCmp))
		}
		w.WriteString("\n")
	}

	// Log to file only (not terminal)
	mlog.Log.FileOnlyf("tie-break debug: epoch=%d tie_groups=%d written to %s", epoch, len(tieGroups), filePath)

	// Log the specific tie if we're looking for it (stake 2499999939665440)
	if group, ok := tieGroups[2499999939665440]; ok {
		mlog.Log.FileOnlyf("tie-break debug: found target tie group stake=2499999939665440:")
		for _, entry := range group {
			mlog.Log.FileOnlyf("  rank=%d node=%s bytes_cmp=%d", entry.Rank, entry.NodePk.String(), entry.BytesCmp)
		}
	}

	// Diagnostic: Check specific vote account → node mappings for epoch 905 debugging
	// Vote accounts that caused the tie-break mismatch:
	debugVoteAccts := []struct {
		vote         string
		expectedNode string
	}{
		{"33hurzEz6aEnzfESL6pnNyR6DCgcKzssT1pwSzDCBTRQ", "Aw5wEMXhbygFLR7jHtHpih8QvxVBGAMTqsQ2SjWPk1ex"},
		{"BU3ZgGBXFJwNTrN6VUJ88k9SJ71SyWfBJTabYqRErm4F", "2GUnfxZavKoPfS9s3VSEjaWDzB3vNf5RojUhprCS1rSx"},
	}
	for _, d := range debugVoteAccts {
		votePk := solana.MustPublicKeyFromBase58(d.vote)
		expectedNodePk := solana.MustPublicKeyFromBase58(d.expectedNode)
		stake, hasStake := voteAcctStakes[votePk]
		va := voteAcctMap[votePk]
		if hasStake || va != nil {
			var actualNode solana.PublicKey
			if va != nil {
				actualNode = va.NodePubkey
			}
			match := actualNode == expectedNodePk
			mlog.Log.FileOnlyf("vote-node-mapping: vote=%s expected_node=%s actual_node=%s stake=%d match=%v",
				d.vote, d.expectedNode, actualNode.String(), stake, match)
			if !match {
				mlog.Log.Warnf("VOTE-NODE MISMATCH: vote=%s expected=%s actual=%s stake=%d",
					d.vote, d.expectedNode, actualNode.String(), stake)
			}
		}
	}
}

// writeValidatorsCSV writes all validators to a CSV file
func writeValidatorsCSV(filepath string, epoch uint64, source string, entries []StakeEntry, totalStake uint64) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	// Header comments
	w.WriteString(fmt.Sprintf("# Leader Schedule - Epoch %d\n", epoch))
	w.WriteString(fmt.Sprintf("# Source: %s\n", source))
	w.WriteString(fmt.Sprintf("# Total Validators: %d\n", len(entries)))
	w.WriteString(fmt.Sprintf("# Total Stake: %d\n", totalStake))
	w.WriteString(fmt.Sprintf("# Generated: %s\n", time.Now().UTC().Format(time.RFC3339)))
	w.WriteString("#\n")
	w.WriteString("rank,vote_account,node_pubkey,stake,stake_percent\n")

	// Write all entries (already sorted by stake descending)
	for i, e := range entries {
		var stakePercent float64
		if totalStake > 0 {
			stakePercent = float64(e.Stake) / float64(totalStake) * 100.0
		}
		w.WriteString(fmt.Sprintf("%d,%s,%s,%d,%.6f\n",
			i+1, e.VoteAcct, e.NodePubkey, e.Stake, stakePercent))
	}

	return nil
}

// writeSkippedCSV writes all skipped accounts to a CSV file
func writeSkippedCSV(filepath string, epoch uint64, entries []StakeEntry) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	// Header comments
	w.WriteString(fmt.Sprintf("# Leader Schedule Skipped Accounts - Epoch %d\n", epoch))
	w.WriteString(fmt.Sprintf("# Total Skipped: %d\n", len(entries)))
	w.WriteString(fmt.Sprintf("# Generated: %s\n", time.Now().UTC().Format(time.RFC3339)))
	w.WriteString("#\n")
	w.WriteString("# Reasons:\n")
	w.WriteString("#   zero_stake - Vote account has 0 stake\n")
	w.WriteString("#   missing_vote_acct - Vote account not found in VoteAcctMap\n")
	w.WriteString("#   missing_vote_cache - Vote account not found in VoteCache\n")
	w.WriteString("#   zero_nodepk - Vote account has zero NodePubkey\n")
	w.WriteString("#\n")
	w.WriteString("# Note: node_pubkey is empty for missing_vote_acct/missing_vote_cache since\n")
	w.WriteString("# the vote account data was not available to extract the NodePubkey.\n")
	w.WriteString("#\n")
	w.WriteString("vote_account,node_pubkey,stake,reason\n")

	for _, e := range entries {
		w.WriteString(fmt.Sprintf("%s,%s,%d,%s\n", e.VoteAcct, e.NodePubkey, e.Stake, e.Reason))
	}

	return nil
}

// ScheduleSummary holds all metadata for the summary file
type ScheduleSummary struct {
	// Epoch info
	BlockEpoch    uint64
	ScheduleEpoch uint64
	FirstSlot     uint64
	SlotsInEpoch  uint64
	Repeat        uint64

	// Stake info
	TotalInputStake     uint64 // Total stake from EpochStakes (before filtering)
	FilteredStake       uint64 // Stake used in schedule (after filtering)
	MissingStake        uint64 // Stake skipped due to missing data
	MissingStakePercent float64

	// Validator counts
	ValidatorsInput    int // Total vote accounts in EpochStakes
	ValidatorsUsed     int // Validators included in schedule
	ValidatorsSkipped  int // Validators skipped (zero stake + missing + zero nodepk)
	SkippedZeroStake   int
	SkippedMissingData int // missing_vote_acct or missing_vote_cache
	SkippedZeroNodePk  int

	// Hashes
	LocalHash string
	RPCHash   string // Empty if RPC validation not enabled

	// Run info
	RunID     string
	Source    string // "snapshot" or "vote_cache"
	Timestamp time.Time
}

// writeSummaryFile writes a comprehensive summary file for the epoch
func writeSummaryFile(filepath string, summary ScheduleSummary) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	w.WriteString("# Leader Schedule Summary\n")
	w.WriteString(fmt.Sprintf("# Generated: %s\n", summary.Timestamp.Format(time.RFC3339)))
	w.WriteString(fmt.Sprintf("# Run ID: %s\n", summary.RunID))
	w.WriteString("#\n")

	w.WriteString("## Epoch Info\n")
	w.WriteString(fmt.Sprintf("block_epoch=%d\n", summary.BlockEpoch))
	w.WriteString(fmt.Sprintf("schedule_epoch=%d\n", summary.ScheduleEpoch))
	w.WriteString(fmt.Sprintf("first_slot=%d\n", summary.FirstSlot))
	w.WriteString(fmt.Sprintf("slots_in_epoch=%d\n", summary.SlotsInEpoch))
	w.WriteString(fmt.Sprintf("repeat=%d\n", summary.Repeat))
	w.WriteString(fmt.Sprintf("source=%s\n", summary.Source))
	w.WriteString("\n")

	w.WriteString("## Stake Info\n")
	w.WriteString(fmt.Sprintf("total_input_stake=%d\n", summary.TotalInputStake))
	w.WriteString(fmt.Sprintf("filtered_stake=%d\n", summary.FilteredStake))
	w.WriteString(fmt.Sprintf("missing_stake=%d\n", summary.MissingStake))
	w.WriteString(fmt.Sprintf("missing_stake_percent=%.4f\n", summary.MissingStakePercent))
	w.WriteString("\n")

	w.WriteString("## Validator Counts\n")
	w.WriteString(fmt.Sprintf("validators_input=%d\n", summary.ValidatorsInput))
	w.WriteString(fmt.Sprintf("validators_used=%d\n", summary.ValidatorsUsed))
	w.WriteString(fmt.Sprintf("validators_skipped=%d\n", summary.ValidatorsSkipped))
	w.WriteString(fmt.Sprintf("skipped_zero_stake=%d\n", summary.SkippedZeroStake))
	w.WriteString(fmt.Sprintf("skipped_missing_data=%d\n", summary.SkippedMissingData))
	w.WriteString(fmt.Sprintf("skipped_zero_nodepk=%d\n", summary.SkippedZeroNodePk))
	w.WriteString("\n")

	w.WriteString("## Hashes\n")
	w.WriteString(fmt.Sprintf("local_hash=%s\n", summary.LocalHash))
	if summary.RPCHash != "" {
		w.WriteString(fmt.Sprintf("rpc_hash=%s\n", summary.RPCHash))
	}
	w.WriteString("\n")

	return nil
}

// ValidationStats holds statistics from schedule validation
type ValidationStats struct {
	SkippedZeroStake            int
	SkippedMissingNodePk        int
	SkippedMissingNodePkStake   uint64 // Stake dropped due to zero NodePubkey
	SkippedMissingVoteAcct      int
	SkippedMissingVoteAcctStake uint64 // Stake dropped due to missing VoteCache entries
	TotalVoteAccts              int
	TotalStake                  uint64
	MinStake                    uint64
	MaxStake                    uint64
	ValidatorCount              int // Validators with non-zero stake and valid NodePubkey
	MismatchCount               int
	Capped                      bool
	TopStakes                   []StakeEntry // Top 10 by stake
	BottomStakes                []StakeEntry // Bottom 10 by stake
	MissingVoteAccts            []StakeEntry // First few missing vote accounts (for debugging)
	ZeroNodePkAccts             []StakeEntry // First few zero NodePubkey accounts
}

// logScheduleBuildSummary logs a comprehensive summary of the schedule build.
// Called once per epoch when building the leader schedule.
// Terminal output is minimal; detailed info goes to log file only.
func logScheduleBuildSummary(
	epoch uint64,
	scheduleEpoch uint64,
	firstSlot uint64,
	slotsInEpoch uint64,
	source string, // "snapshot" or "vote_cache"
	stats ValidationStats,
	fullHash string,
) {
	// File only: single line summary
	mlog.Log.FileOnlyf("leader schedule: epoch=%d validators=%d stake=%d hash=%s",
		epoch, stats.ValidatorCount, stats.TotalStake, fullHash)

	// File only: detailed build info
	mlog.Log.FileOnlyf("leader schedule build details: epoch=%d schedule_epoch=%d first_slot=%d slots=%d repeat=%d source=%s",
		epoch, scheduleEpoch, firstSlot, slotsInEpoch, NumConsecutiveLeaderSlots, source)
	mlog.Log.FileOnlyf("  validators=%d total_stake=%d min_stake=%d max_stake=%d zero_stake_count=%d",
		stats.ValidatorCount, stats.TotalStake, stats.MinStake, stats.MaxStake, stats.SkippedZeroStake)
	mlog.Log.FileOnlyf("  hash=%s", fullHash)
	mlog.Log.FileOnlyf("  skipped: missing_vote_acct=%d (stake=%d) missing_nodepk=%d (stake=%d)",
		stats.SkippedMissingVoteAcct, stats.SkippedMissingVoteAcctStake, stats.SkippedMissingNodePk, stats.SkippedMissingNodePkStake)

	// File only: top 10 stakes
	if len(stats.TopStakes) > 0 {
		mlog.Log.FileOnlyf("  top_stakes (showing %d):", len(stats.TopStakes))
		for i, e := range stats.TopStakes {
			mlog.Log.FileOnlyf("    %2d. vote=%s node=%s stake=%d",
				i+1, e.VoteAcct, e.NodePubkey, e.Stake)
		}
	}

	// File only: bottom 10 stakes
	if len(stats.BottomStakes) > 0 {
		mlog.Log.FileOnlyf("  bottom_stakes (showing %d):", len(stats.BottomStakes))
		for i, e := range stats.BottomStakes {
			mlog.Log.FileOnlyf("    %2d. vote=%s node=%s stake=%d",
				i+1, e.VoteAcct, e.NodePubkey, e.Stake)
		}
	}

	// File only: offending accounts if any were skipped
	if len(stats.MissingVoteAccts) > 0 {
		mlog.Log.FileOnlyf("  missing_vote_accts (first %d):", len(stats.MissingVoteAccts))
		for i, e := range stats.MissingVoteAccts {
			mlog.Log.FileOnlyf("    %d. vote=%s stake=%d", i+1, e.VoteAcct, e.Stake)
		}
	}
	if len(stats.ZeroNodePkAccts) > 0 {
		mlog.Log.FileOnlyf("  zero_nodepk_accts (first %d):", len(stats.ZeroNodePkAccts))
		for i, e := range stats.ZeroNodePkAccts {
			mlog.Log.FileOnlyf("    %d. vote=%s stake=%d", i+1, e.VoteAcct, e.Stake)
		}
	}
}

// logHardFailContext logs detailed context when schedule build fails.
// Terminal shows brief error; file gets full details.
func logHardFailContext(
	epoch uint64,
	reason string,
	stats ValidationStats,
	voteAcctStakes map[solana.PublicKey]uint64,
) {
	// Terminal: brief error
	mlog.Log.Errorf("LEADER SCHEDULE BUILD FAILED: epoch=%d reason=%s", epoch, reason)

	// File only: detailed context
	mlog.Log.FileOnlyf("LEADER SCHEDULE BUILD FAILED DETAILS:")
	mlog.Log.FileOnlyf("  epoch=%d reason=%s", epoch, reason)
	mlog.Log.FileOnlyf("  input_vote_accts=%d total_stake_available=%d",
		stats.TotalVoteAccts, stats.TotalStake)
	mlog.Log.FileOnlyf("  skipped: zero_stake=%d missing_vote_acct=%d (stake=%d) missing_nodepk=%d (stake=%d)",
		stats.SkippedZeroStake, stats.SkippedMissingVoteAcct, stats.SkippedMissingVoteAcctStake, stats.SkippedMissingNodePk, stats.SkippedMissingNodePkStake)

	// File only: first few offending accounts
	if len(stats.MissingVoteAccts) > 0 {
		mlog.Log.FileOnlyf("  missing_vote_accts (first %d):", len(stats.MissingVoteAccts))
		for i, e := range stats.MissingVoteAccts {
			mlog.Log.FileOnlyf("    %d. vote=%s stake=%d", i+1, e.VoteAcct, e.Stake)
		}
	}
	if len(stats.ZeroNodePkAccts) > 0 {
		mlog.Log.FileOnlyf("  zero_nodepk_accts (first %d):", len(stats.ZeroNodePkAccts))
		for i, e := range stats.ZeroNodePkAccts {
			mlog.Log.FileOnlyf("    %d. vote=%s stake=%d", i+1, e.VoteAcct, e.Stake)
		}
	}

	// File only: valid top stakes for context
	if len(stats.TopStakes) > 0 {
		mlog.Log.FileOnlyf("  top_stakes_found (showing %d):", min(5, len(stats.TopStakes)))
		for i := 0; i < min(5, len(stats.TopStakes)); i++ {
			e := stats.TopStakes[i]
			mlog.Log.FileOnlyf("    %d. vote=%s node=%s stake=%d", i+1, e.VoteAcct, e.NodePubkey, e.Stake)
		}
	}
}

// buildLocalLeaderSchedule builds a leader schedule from local state.
// Returns nil schedule if no valid stakes are available.
// Also returns all valid and skipped entries for CSV dump.
func buildLocalLeaderSchedule(
	epoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	voteAcctStakes map[solana.PublicKey]uint64,
	voteAcctMap map[solana.PublicKey]*epochstakes.VoteAccount,
) (*leaderschedule.LeaderSchedule, ValidationStats, []StakeEntry, []StakeEntry) {
	stats := ValidationStats{
		TotalVoteAccts: len(voteAcctStakes),
		MinStake:       ^uint64(0), // Start with max value
	}

	// Collect ALL valid and skipped entries for CSV dump
	var validEntries []StakeEntry
	var skippedEntries []StakeEntry

	// Filter and build epochVoteAccts map (only entries with stake > 0 and valid NodePubkey)
	epochVoteAccts := make(map[solana.PublicKey]*epochstakes.VoteAccount)
	filteredStakes := make(map[solana.PublicKey]uint64)

	for votePk, stake := range voteAcctStakes {
		if stake == 0 {
			stats.SkippedZeroStake++
			skippedEntries = append(skippedEntries, StakeEntry{
				VoteAcct: votePk,
				Stake:    stake,
				Reason:   "zero_stake",
			})
			continue
		}

		va := voteAcctMap[votePk]
		if va == nil {
			stats.SkippedMissingVoteAcct++
			stats.SkippedMissingVoteAcctStake += stake
			skippedEntries = append(skippedEntries, StakeEntry{
				VoteAcct: votePk,
				Stake:    stake,
				Reason:   "missing_vote_acct",
			})
			// Track first few for quick debugging in logs
			if len(stats.MissingVoteAccts) < 5 {
				stats.MissingVoteAccts = append(stats.MissingVoteAccts, StakeEntry{
					VoteAcct: votePk,
					Stake:    stake,
				})
			}
			continue
		}

		// Check for zero NodePubkey (missing)
		var zeroPk solana.PublicKey
		if va.NodePubkey == zeroPk {
			stats.SkippedMissingNodePk++
			stats.SkippedMissingNodePkStake += stake
			skippedEntries = append(skippedEntries, StakeEntry{
				VoteAcct: votePk,
				Stake:    stake,
				Reason:   "zero_nodepk",
			})
			// Track first few for quick debugging in logs
			if len(stats.ZeroNodePkAccts) < 5 {
				stats.ZeroNodePkAccts = append(stats.ZeroNodePkAccts, StakeEntry{
					VoteAcct: votePk,
					Stake:    stake,
				})
			}
			continue
		}

		epochVoteAccts[votePk] = va
		filteredStakes[votePk] = stake
		stats.TotalStake += stake

		// Track min/max
		if stake < stats.MinStake {
			stats.MinStake = stake
		}
		if stake > stats.MaxStake {
			stats.MaxStake = stake
		}

		validEntries = append(validEntries, StakeEntry{
			VoteAcct:   votePk,
			NodePubkey: va.NodePubkey,
			Stake:      stake,
		})
	}

	stats.ValidatorCount = len(validEntries)

	// Guard: empty stakes would panic in weightedrand
	if len(filteredStakes) == 0 {
		stats.MinStake = 0 // Reset since no valid entries
		return nil, stats, validEntries, skippedEntries
	}

	// Sort entries by stake descending, then node pubkey descending (matches schedule computation)
	sort.Slice(validEntries, func(i, j int) bool {
		if validEntries[i].Stake != validEntries[j].Stake {
			return validEntries[i].Stake > validEntries[j].Stake
		}
		// Tie-break by node pubkey descending (higher bytes first) - matches Agave
		return bytes.Compare(validEntries[i].NodePubkey[:], validEntries[j].NodePubkey[:]) > 0
	})

	// Capture top 10 and bottom 10 for log summary
	for i := 0; i < min(10, len(validEntries)); i++ {
		stats.TopStakes = append(stats.TopStakes, validEntries[i])
	}
	for i := max(0, len(validEntries)-10); i < len(validEntries); i++ {
		stats.BottomStakes = append(stats.BottomStakes, validEntries[i])
	}

	// Get epoch length (handles warmup epochs correctly)
	slotsInEpoch := epochSchedule.SlotsInEpoch(epoch)

	// Build the schedule using leaderschedule.New
	ls := leaderschedule.New(
		epochVoteAccts,
		filteredStakes,
		epochSchedule,
		epoch,
		slotsInEpoch,
		NumConsecutiveLeaderSlots,
	)

	return ls, stats, validEntries, skippedEntries
}

// buildLocalLeaderScheduleFromVoteCache builds schedule using global.VoteCache() for NodePubkey lookups.
// Used at epoch boundaries when epochVoteAcctsMap may not be available.
// Returns nil schedule if no valid stakes are available.
// Also returns all valid and skipped entries for CSV dump.
func buildLocalLeaderScheduleFromVoteCache(
	epoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	voteAcctStakes map[solana.PublicKey]uint64,
) (*leaderschedule.LeaderSchedule, ValidationStats, []StakeEntry, []StakeEntry) {
	stats := ValidationStats{
		TotalVoteAccts: len(voteAcctStakes),
		MinStake:       ^uint64(0), // Start with max value
	}

	voteCache := global.VoteCache()

	// Collect ALL valid and skipped entries for CSV dump
	var validEntries []StakeEntry
	var skippedEntries []StakeEntry

	// Build epochVoteAccts map from vote cache
	epochVoteAccts := make(map[solana.PublicKey]*epochstakes.VoteAccount)
	filteredStakes := make(map[solana.PublicKey]uint64)

	for votePk, stake := range voteAcctStakes {
		if stake == 0 {
			stats.SkippedZeroStake++
			skippedEntries = append(skippedEntries, StakeEntry{
				VoteAcct: votePk,
				Stake:    stake,
				Reason:   "zero_stake",
			})
			continue
		}

		vs := voteCache[votePk]
		if vs == nil {
			stats.SkippedMissingVoteAcct++
			stats.SkippedMissingVoteAcctStake += stake
			skippedEntries = append(skippedEntries, StakeEntry{
				VoteAcct: votePk,
				Stake:    stake,
				Reason:   "missing_vote_cache",
			})
			// Track first few for quick debugging in logs
			if len(stats.MissingVoteAccts) < 5 {
				stats.MissingVoteAccts = append(stats.MissingVoteAccts, StakeEntry{
					VoteAcct: votePk,
					Stake:    stake,
				})
			}
			continue
		}

		nodePk := vs.NodePubkey()
		var zeroPk solana.PublicKey
		if nodePk == zeroPk {
			stats.SkippedMissingNodePk++
			stats.SkippedMissingNodePkStake += stake
			skippedEntries = append(skippedEntries, StakeEntry{
				VoteAcct: votePk,
				Stake:    stake,
				Reason:   "zero_nodepk",
			})
			// Track first few for quick debugging in logs
			if len(stats.ZeroNodePkAccts) < 5 {
				stats.ZeroNodePkAccts = append(stats.ZeroNodePkAccts, StakeEntry{
					VoteAcct: votePk,
					Stake:    stake,
				})
			}
			continue
		}

		// Create a VoteAccount with the NodePubkey
		va := &epochstakes.VoteAccount{
			NodePubkey: nodePk,
		}
		epochVoteAccts[votePk] = va
		filteredStakes[votePk] = stake
		stats.TotalStake += stake

		// Track min/max
		if stake < stats.MinStake {
			stats.MinStake = stake
		}
		if stake > stats.MaxStake {
			stats.MaxStake = stake
		}

		validEntries = append(validEntries, StakeEntry{
			VoteAcct:   votePk,
			NodePubkey: nodePk,
			Stake:      stake,
		})
	}

	stats.ValidatorCount = len(validEntries)

	// Guard: empty stakes would panic in weightedrand
	if len(filteredStakes) == 0 {
		stats.MinStake = 0 // Reset since no valid entries
		return nil, stats, validEntries, skippedEntries
	}

	// Sort entries by stake descending, then node pubkey descending (matches schedule computation)
	sort.Slice(validEntries, func(i, j int) bool {
		if validEntries[i].Stake != validEntries[j].Stake {
			return validEntries[i].Stake > validEntries[j].Stake
		}
		// Tie-break by node pubkey descending (higher bytes first) - matches Agave
		return bytes.Compare(validEntries[i].NodePubkey[:], validEntries[j].NodePubkey[:]) > 0
	})

	// Capture top 10 and bottom 10 for log summary
	for i := 0; i < min(10, len(validEntries)); i++ {
		stats.TopStakes = append(stats.TopStakes, validEntries[i])
	}
	for i := max(0, len(validEntries)-10); i < len(validEntries); i++ {
		stats.BottomStakes = append(stats.BottomStakes, validEntries[i])
	}

	// Get epoch length (handles warmup epochs correctly)
	slotsInEpoch := epochSchedule.SlotsInEpoch(epoch)

	ls := leaderschedule.New(
		epochVoteAccts,
		filteredStakes,
		epochSchedule,
		epoch,
		slotsInEpoch,
		NumConsecutiveLeaderSlots,
	)

	return ls, stats, validEntries, skippedEntries
}

// scheduleFullHash computes a SHA256 hash of the entire leader schedule.
// Returns base64-encoded first 16 bytes of the hash.
// Takes ~20-50ms for a full epoch (432k slots).
func scheduleFullHash(ls *leaderschedule.LeaderSchedule, firstSlot uint64, numSlots uint64) string {
	if ls == nil {
		return "nil"
	}

	h := sha256.New()
	for i := uint64(0); i < numSlots; i++ {
		slot := firstSlot + i
		leader, ok := ls.LeaderForSlot(slot)
		if ok {
			h.Write(leader[:])
		}
	}

	return base64.StdEncoding.EncodeToString(h.Sum(nil)[:16])
}

// validateLeaderSchedule compares local vs RPC schedule and logs mismatches.
// Does NOT return error - mismatches are logged but don't stop replay.
func validateLeaderSchedule(
	blockEpoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	rpcSchedule *leaderschedule.LeaderSchedule,
	logsDir string,
) {
	if rpcSchedule == nil {
		mlog.Log.Warnf("leader schedule validation: rpc schedule is nil, skipping")
		return
	}

	// Initialize mismatch log file (once per process)
	initMismatchLog(logsDir)

	// Stakes are stored under the epoch they're EFFECTIVE for, not the boundary epoch.
	// E.g., stakes frozen at end of epoch 499 are effective during epoch 500,
	// so they're stored as EpochStakes(500). LeaderScheduleEpoch returns 499 (the
	// boundary), but lookup should use blockEpoch (500).
	firstSlot := epochSchedule.FirstSlotInEpoch(blockEpoch)

	// Fetch stakes for blockEpoch (stored under the epoch they're effective for)
	voteAcctStakes := global.EpochStakes(blockEpoch)
	voteAcctMap := global.EpochStakesVoteAccts(blockEpoch)

	// Guard: skip if no stake data available for this epoch
	if len(voteAcctStakes) == 0 {
		mlog.Log.Warnf("leader schedule validation: no stake data for epoch=%d, skipping", blockEpoch)
		return
	}

	// Use blockEpoch for slot count (this is the epoch we're building schedule for)
	numSlots := epochSchedule.SlotsInEpoch(blockEpoch)

	// Log input snapshot for debugging
	logInputSnapshot(blockEpoch, voteAcctStakes, voteAcctMap)

	// Build local schedule for blockEpoch (same as RPC)
	localSchedule, stats, _, _ := buildLocalLeaderSchedule(blockEpoch, epochSchedule, voteAcctStakes, voteAcctMap)

	// Guard: skip if local schedule couldn't be built (empty stakes after filtering)
	if localSchedule == nil {
		mlog.Log.Warnf("leader schedule validation: could not build local schedule (no valid stakes), skipping")
		mlog.Log.FileOnlyf("  epoch=%d vote_accts=%d skipped: zero_stake=%d missing_nodepk=%d missing_vote_acct=%d",
			blockEpoch, stats.TotalVoteAccts, stats.SkippedZeroStake, stats.SkippedMissingNodePk, stats.SkippedMissingVoteAcct)
		return
	}

	// Sample and compare slots
	mismatchCount := 0

	// Generate slots to sample: first 2k, last 2k, plus random 1k in middle
	slotsToSample := make([]uint64, 0, SampleBoundarySlots*2+SampleRandomSlots)

	// First boundary slots
	for i := uint64(0); i < min(SampleBoundarySlots, numSlots); i++ {
		slotsToSample = append(slotsToSample, firstSlot+i)
	}

	// Last boundary slots
	if numSlots > SampleBoundarySlots {
		startLast := numSlots - min(SampleBoundarySlots, numSlots)
		for i := startLast; i < numSlots; i++ {
			slotsToSample = append(slotsToSample, firstSlot+i)
		}
	}

	// Random slots in middle (deterministic based on blockEpoch for reproducibility)
	if numSlots > SampleBoundarySlots*2 {
		rng := rand.New(rand.NewSource(blockEpoch))
		middleStart := uint64(SampleBoundarySlots)
		middleEnd := numSlots - SampleBoundarySlots
		for i := 0; i < SampleRandomSlots && middleEnd > middleStart; i++ {
			offset := rng.Uint64() % (middleEnd - middleStart)
			slotsToSample = append(slotsToSample, firstSlot+middleStart+offset)
		}
	}

	// Compare sampled slots
	for _, slot := range slotsToSample {
		localLeader, localOk := localSchedule.LeaderForSlot(slot)
		rpcLeader, rpcOk := rpcSchedule.LeaderForSlot(slot)

		if !localOk || !rpcOk {
			continue // Skip slots not in schedules
		}

		if localLeader != rpcLeader {
			// Find the vote account that maps to the local leader for debugging
			var matchingVoteAcct solana.PublicKey
			var matchingStake uint64
			for votePk, va := range voteAcctMap {
				if va != nil && va.NodePubkey == localLeader {
					matchingVoteAcct = votePk
					matchingStake = voteAcctStakes[votePk]
					break
				}
			}
			logMismatch(blockEpoch, slot, localLeader, rpcLeader, matchingVoteAcct, matchingStake, &mismatchCount)
		}
	}

	stats.MismatchCount = mismatchCount
	stats.Capped = mismatchCount >= MaxMismatchLogsPerEpoch

	// Flush mismatch log
	flushMismatchLog()

	// File only: per-epoch validation summary
	mlog.Log.FileOnlyf("leader schedule validation: epoch=%d first_slot=%d slots=%d",
		blockEpoch, firstSlot, numSlots)
	mlog.Log.FileOnlyf("  vote_accts=%d total_stake=%d skipped: zero_stake=%d missing_nodepk=%d missing_vote_acct=%d",
		stats.TotalVoteAccts, stats.TotalStake, stats.SkippedZeroStake, stats.SkippedMissingNodePk, stats.SkippedMissingVoteAcct)
	mlog.Log.FileOnlyf("  sampled=%d mismatches=%d (capped=%v)", len(slotsToSample), stats.MismatchCount, stats.Capped)

	// Terminal: only warn on mismatches
	if stats.MismatchCount > 0 {
		mlog.Log.Warnf("leader schedule validation: %d MISMATCHES epoch=%d - see %s",
			stats.MismatchCount, blockEpoch, getMismatchLogPath())
	}
}

// validateLeaderScheduleFromVoteCache validates using global.VoteCache() for NodePubkey lookups.
// Used at epoch boundaries when epochVoteAcctsMap may not be available from snapshot.
func validateLeaderScheduleFromVoteCache(
	blockEpoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	rpcSchedule *leaderschedule.LeaderSchedule,
	logsDir string,
) {
	if rpcSchedule == nil {
		mlog.Log.Warnf("leader schedule validation: rpc schedule is nil, skipping")
		return
	}

	// Initialize mismatch log file (once per process)
	initMismatchLog(logsDir)

	// Stakes are stored under the epoch they're EFFECTIVE for, not the boundary epoch.
	firstSlot := epochSchedule.FirstSlotInEpoch(blockEpoch)

	// Fetch stakes for blockEpoch (stored under the epoch they're effective for)
	voteAcctStakes := global.EpochStakes(blockEpoch)

	// Guard: skip if no stake data available for this epoch
	if len(voteAcctStakes) == 0 {
		mlog.Log.Warnf("leader schedule validation: no stake data for epoch=%d, skipping", blockEpoch)
		return
	}

	// Use blockEpoch for slot count (this is the epoch we're building schedule for)
	numSlots := epochSchedule.SlotsInEpoch(blockEpoch)

	// Build local schedule for blockEpoch (same as RPC)
	localSchedule, stats, _, _ := buildLocalLeaderScheduleFromVoteCache(blockEpoch, epochSchedule, voteAcctStakes)

	// Guard: skip if local schedule couldn't be built (empty stakes after filtering)
	if localSchedule == nil {
		mlog.Log.Warnf("leader schedule validation: could not build local schedule (no valid stakes), skipping")
		mlog.Log.FileOnlyf("  epoch=%d vote_accts=%d skipped: zero_stake=%d missing_nodepk=%d missing_vote_state=%d",
			blockEpoch, stats.TotalVoteAccts, stats.SkippedZeroStake, stats.SkippedMissingNodePk, stats.SkippedMissingVoteAcct)
		return
	}

	// Sample and compare slots
	mismatchCount := 0

	// Generate slots to sample: first 2k, last 2k, plus random 1k in middle
	slotsToSample := make([]uint64, 0, SampleBoundarySlots*2+SampleRandomSlots)

	// First boundary slots
	for i := uint64(0); i < min(SampleBoundarySlots, numSlots); i++ {
		slotsToSample = append(slotsToSample, firstSlot+i)
	}

	// Last boundary slots
	if numSlots > SampleBoundarySlots {
		startLast := numSlots - min(SampleBoundarySlots, numSlots)
		for i := startLast; i < numSlots; i++ {
			slotsToSample = append(slotsToSample, firstSlot+i)
		}
	}

	// Random slots in middle (deterministic based on blockEpoch for reproducibility)
	if numSlots > SampleBoundarySlots*2 {
		rng := rand.New(rand.NewSource(blockEpoch))
		middleStart := uint64(SampleBoundarySlots)
		middleEnd := numSlots - SampleBoundarySlots
		for i := 0; i < SampleRandomSlots && middleEnd > middleStart; i++ {
			offset := rng.Uint64() % (middleEnd - middleStart)
			slotsToSample = append(slotsToSample, firstSlot+middleStart+offset)
		}
	}

	// Compare sampled slots
	voteCache := global.VoteCache()
	for _, slot := range slotsToSample {
		localLeader, localOk := localSchedule.LeaderForSlot(slot)
		rpcLeader, rpcOk := rpcSchedule.LeaderForSlot(slot)

		if !localOk || !rpcOk {
			continue
		}

		if localLeader != rpcLeader {
			// Find the vote account that maps to the local leader
			var matchingVoteAcct solana.PublicKey
			var matchingStake uint64
			for votePk, vs := range voteCache {
				if vs != nil && vs.NodePubkey() == localLeader {
					matchingVoteAcct = votePk
					matchingStake = voteAcctStakes[votePk]
					break
				}
			}
			logMismatch(blockEpoch, slot, localLeader, rpcLeader, matchingVoteAcct, matchingStake, &mismatchCount)
		}
	}

	stats.MismatchCount = mismatchCount
	stats.Capped = mismatchCount >= MaxMismatchLogsPerEpoch

	flushMismatchLog()

	// File only: per-epoch validation summary
	mlog.Log.FileOnlyf("leader schedule validation (vote cache): epoch=%d first_slot=%d slots=%d",
		blockEpoch, firstSlot, numSlots)
	mlog.Log.FileOnlyf("  vote_accts=%d total_stake=%d skipped: zero_stake=%d missing_nodepk=%d missing_vote_state=%d",
		stats.TotalVoteAccts, stats.TotalStake, stats.SkippedZeroStake, stats.SkippedMissingNodePk, stats.SkippedMissingVoteAcct)
	mlog.Log.FileOnlyf("  sampled=%d mismatches=%d (capped=%v)", len(slotsToSample), stats.MismatchCount, stats.Capped)

	// Terminal: only warn on mismatches
	if stats.MismatchCount > 0 {
		mlog.Log.Warnf("leader schedule validation: %d MISMATCHES epoch=%d - see %s",
			stats.MismatchCount, blockEpoch, getMismatchLogPath())
	}
}

// PrepareLeaderScheduleLocal builds the leader schedule from local state and sets it as the source of truth.
// This is the primary entry point for leader schedule - no RPC dependency.
// Returns the schedule summary (for RPC validation) and error if schedule cannot be built.
func PrepareLeaderScheduleLocal(
	epoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	logsDir string,
) (*ScheduleSummary, error) {
	voteAcctStakes := global.EpochStakes(epoch)
	voteAcctMap := global.EpochStakesVoteAccts(epoch)

	// The RNG seed uses `epoch` directly (the epoch we're building the schedule for)
	// Note: LeaderScheduleEpoch() returns something different (next epoch's prep slot) - don't use it here
	firstSlot := epochSchedule.FirstSlotInEpoch(epoch)
	numSlots := epochSchedule.SlotsInEpoch(epoch)

	if len(voteAcctStakes) == 0 {
		mlog.Log.Errorf("LEADER SCHEDULE BUILD FAILED: epoch=%d reason=no_stake_data", epoch)
		mlog.Log.FileOnlyf("  rng_epoch=%d first_slot=%d slots=%d", epoch, firstSlot, numSlots)
		mlog.Log.FileOnlyf("  EpochStakes(%d) returned nil or empty", epoch)
		return nil, fmt.Errorf("no stake data available for epoch %d", epoch)
	}

	schedule, stats, validEntries, skippedEntries := buildLocalLeaderSchedule(epoch, epochSchedule, voteAcctStakes, voteAcctMap)

	// Calculate total input stake (before filtering)
	var totalInputStake uint64
	for _, stake := range voteAcctStakes {
		totalInputStake += stake
	}

	if schedule == nil {
		logHardFailContext(epoch, "no_valid_stakes_after_filtering", stats, voteAcctStakes)
		// Still dump whatever data we have for debugging even on failure
		dumpFullScheduleData(epoch, "local", validEntries, skippedEntries, stats.TotalStake, logsDir)
		return nil, fmt.Errorf("could not build leader schedule for epoch %d: no valid stakes after filtering (zero_stake=%d, missing_nodepk=%d, missing_vote_acct=%d)",
			epoch, stats.SkippedZeroStake, stats.SkippedMissingNodePk, stats.SkippedMissingVoteAcct)
	}

	// Set as source of truth
	global.SetLeaderSchedule(schedule)

	// Compute hash for logging
	fullHash := scheduleFullHash(schedule, firstSlot, numSlots)

	// Log comprehensive summary
	logScheduleBuildSummary(epoch, epoch, firstSlot, numSlots, "snapshot", stats, fullHash)

	// Build summary with all metadata
	// Include all missing stake: missing_vote_acct + zero_nodepk
	missingStake := stats.SkippedMissingVoteAcctStake + stats.SkippedMissingNodePkStake
	var missingPercent float64
	if totalInputStake > 0 {
		missingPercent = float64(missingStake) / float64(totalInputStake) * 100.0
	}
	summary := ScheduleSummary{
		BlockEpoch:          epoch,
		ScheduleEpoch:       epoch, // RNG seed epoch = block epoch
		FirstSlot:           firstSlot,
		SlotsInEpoch:        numSlots,
		Repeat:              NumConsecutiveLeaderSlots,
		TotalInputStake:     totalInputStake,
		FilteredStake:       stats.TotalStake,
		MissingStake:        missingStake,
		MissingStakePercent: missingPercent,
		ValidatorsInput:     stats.TotalVoteAccts,
		ValidatorsUsed:      stats.ValidatorCount,
		ValidatorsSkipped:   stats.SkippedZeroStake + stats.SkippedMissingVoteAcct + stats.SkippedMissingNodePk,
		SkippedZeroStake:    stats.SkippedZeroStake,
		SkippedMissingData:  stats.SkippedMissingVoteAcct,
		SkippedZeroNodePk:   stats.SkippedMissingNodePk,
		LocalHash:           fullHash,
		RunID:               mlog.GetRunID(),
		Source:              "snapshot", // From snapshot loading at startup
		Timestamp:           time.Now().UTC(),
	}

	// Dump ALL validators, skipped accounts, and summary to files
	dumpFullScheduleDataWithSummary(validEntries, skippedEntries, summary, logsDir)

	// Dump tie-break debug info (shows how equal-stake validators are ordered)
	DumpTieBreakDebug(epoch, voteAcctStakes, voteAcctMap, logsDir)

	// Dump first 1000 slots if dump flag is set (for debugging against RPC)
	if config.GetBool("replay.dump_leader_schedule") {
		DumpLeaderSchedule(epoch, epochSchedule, schedule, logsDir, 1000)
	}

	return &summary, nil
}

// PrepareLeaderScheduleLocalFromVoteCache builds the leader schedule using vote cache for NodePubkey lookups.
// Used at epoch boundaries when EpochStakesVoteAccts may not have the new epoch's data yet.
// Returns the schedule summary (for RPC validation) and error if schedule cannot be built.
func PrepareLeaderScheduleLocalFromVoteCache(
	epoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	logsDir string,
) (*ScheduleSummary, error) {
	voteAcctStakes := global.EpochStakes(epoch)

	// The RNG seed uses `epoch` directly (the epoch we're building the schedule for)
	// Note: LeaderScheduleEpoch() returns something different (next epoch's prep slot) - don't use it here
	firstSlot := epochSchedule.FirstSlotInEpoch(epoch)
	numSlots := epochSchedule.SlotsInEpoch(epoch)

	if len(voteAcctStakes) == 0 {
		mlog.Log.Errorf("LEADER SCHEDULE BUILD FAILED: epoch=%d reason=no_stake_data", epoch)
		mlog.Log.FileOnlyf("  rng_epoch=%d first_slot=%d slots=%d source=vote_cache", epoch, firstSlot, numSlots)
		mlog.Log.FileOnlyf("  EpochStakes(%d) returned nil or empty", epoch)
		mlog.Log.FileOnlyf("  VoteCache size=%d", len(global.VoteCache()))
		return nil, fmt.Errorf("no stake data available for epoch %d", epoch)
	}

	schedule, stats, validEntries, skippedEntries := buildLocalLeaderScheduleFromVoteCache(epoch, epochSchedule, voteAcctStakes)

	// Calculate total input stake (before filtering)
	var totalInputStake uint64
	for _, stake := range voteAcctStakes {
		totalInputStake += stake
	}

	if schedule == nil {
		logHardFailContext(epoch, "no_valid_stakes_after_filtering (vote_cache)", stats, voteAcctStakes)
		// Still dump whatever data we have for debugging even on failure
		dumpFullScheduleData(epoch, "local_vote_cache", validEntries, skippedEntries, stats.TotalStake, logsDir)
		return nil, fmt.Errorf("could not build leader schedule for epoch %d: no valid stakes after filtering (zero_stake=%d, missing_nodepk=%d, missing_vote_state=%d)",
			epoch, stats.SkippedZeroStake, stats.SkippedMissingNodePk, stats.SkippedMissingVoteAcct)
	}

	// Safety check: fail if too much stake is missing from VoteCache.
	// Since local schedule is the source of truth, missing entries produce incorrect schedules.
	missingStake := stats.SkippedMissingVoteAcctStake
	if totalInputStake > 0 && missingStake > 0 {
		missingPercent := float64(missingStake) / float64(totalInputStake) * 100.0
		if missingPercent > MaxMissingVoteCacheStakePercent {
			logHardFailContext(epoch, fmt.Sprintf("vote_cache_too_incomplete (%.2f%% > %.1f%%)", missingPercent, MaxMissingVoteCacheStakePercent), stats, voteAcctStakes)
			// Dump data even on failure for debugging
			dumpFullScheduleData(epoch, "local_vote_cache", validEntries, skippedEntries, stats.TotalStake, logsDir)
			return nil, fmt.Errorf("vote cache too incomplete for epoch %d: %.2f%% stake missing (threshold %.1f%%), missing_accts=%d missing_stake=%d total_stake=%d",
				epoch, missingPercent, MaxMissingVoteCacheStakePercent,
				stats.SkippedMissingVoteAcct, missingStake, totalInputStake)
		}
		// Log warning if any stake is missing, even below threshold
		mlog.Log.Warnf("leader schedule: epoch=%d has %.2f%% stake missing from VoteCache (count=%d stake=%d)",
			epoch, missingPercent, stats.SkippedMissingVoteAcct, missingStake)
	}

	// Set as source of truth
	global.SetLeaderSchedule(schedule)

	// Compute hash for logging
	fullHash := scheduleFullHash(schedule, firstSlot, numSlots)

	// Log comprehensive summary
	logScheduleBuildSummary(epoch, epoch, firstSlot, numSlots, "vote_cache", stats, fullHash)

	// Build summary with all metadata
	// Include all missing stake: missing_vote_acct + zero_nodepk
	totalMissingStake := stats.SkippedMissingVoteAcctStake + stats.SkippedMissingNodePkStake
	var missingPercent float64
	if totalInputStake > 0 {
		missingPercent = float64(totalMissingStake) / float64(totalInputStake) * 100.0
	}
	summary := ScheduleSummary{
		BlockEpoch:          epoch,
		ScheduleEpoch:       epoch, // RNG seed epoch = block epoch
		FirstSlot:           firstSlot,
		SlotsInEpoch:        numSlots,
		Repeat:              NumConsecutiveLeaderSlots,
		TotalInputStake:     totalInputStake,
		FilteredStake:       stats.TotalStake,
		MissingStake:        totalMissingStake,
		MissingStakePercent: missingPercent,
		ValidatorsInput:     stats.TotalVoteAccts,
		ValidatorsUsed:      stats.ValidatorCount,
		ValidatorsSkipped:   stats.SkippedZeroStake + stats.SkippedMissingVoteAcct + stats.SkippedMissingNodePk,
		SkippedZeroStake:    stats.SkippedZeroStake,
		SkippedMissingData:  stats.SkippedMissingVoteAcct,
		SkippedZeroNodePk:   stats.SkippedMissingNodePk,
		LocalHash:           fullHash,
		RunID:               mlog.GetRunID(),
		Source:              "transition", // From epoch boundary transition
		Timestamp:           time.Now().UTC(),
	}

	// Dump ALL validators, skipped accounts, and summary to files
	dumpFullScheduleDataWithSummary(validEntries, skippedEntries, summary, logsDir)

	// Dump first 1000 slots if dump flag is set (for debugging against RPC)
	if config.GetBool("replay.dump_leader_schedule") {
		DumpLeaderSchedule(epoch, epochSchedule, schedule, logsDir, 1000)
	}

	return &summary, nil
}

// DumpLeaderSchedule writes the first N slots of the schedule to a file for debugging.
// File is written to logsDir/leader_schedule_dump_epoch<N>.txt
// Useful for comparing against RPC getLeaderSchedule results.
func DumpLeaderSchedule(
	epoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	schedule *leaderschedule.LeaderSchedule,
	logsDir string,
	numSlots int,
) {
	if schedule == nil {
		mlog.Log.Warnf("DumpLeaderSchedule: schedule is nil")
		return
	}

	logsDir = resolveLogsDir(logsDir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		mlog.Log.Warnf("DumpLeaderSchedule: failed to create logs dir: %v", err)
		return
	}

	filename := fmt.Sprintf("leader_schedule_dump_epoch%d.txt", epoch)
	filepath := filepath.Join(logsDir, filename)

	f, err := os.Create(filepath)
	if err != nil {
		mlog.Log.Warnf("DumpLeaderSchedule: failed to create file: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	firstSlot := epochSchedule.FirstSlotInEpoch(epoch)
	totalSlots := epochSchedule.SlotsInEpoch(epoch)

	// Write header
	w.WriteString(fmt.Sprintf("# Leader Schedule Dump - Epoch %d\n", epoch))
	w.WriteString(fmt.Sprintf("# First slot: %d\n", firstSlot))
	w.WriteString(fmt.Sprintf("# Total slots in epoch: %d\n", totalSlots))
	w.WriteString(fmt.Sprintf("# Dumping first %d slots\n", numSlots))
	w.WriteString(fmt.Sprintf("# Format: slot_offset,absolute_slot,leader_pubkey\n"))
	w.WriteString("#\n")

	// Dump first N slots
	for i := 0; i < numSlots && uint64(i) < totalSlots; i++ {
		slot := firstSlot + uint64(i)
		leader, ok := schedule.LeaderForSlot(slot)
		if ok {
			w.WriteString(fmt.Sprintf("%d,%d,%s\n", i, slot, leader.String()))
		} else {
			w.WriteString(fmt.Sprintf("%d,%d,NOT_FOUND\n", i, slot))
		}
	}

	mlog.Log.FileOnlyf("leader schedule dumped to: %s (first %d slots)", filepath, numSlots)
}

// dumpScheduleSlotsCSV dumps the full schedule to a CSV for slot-by-slot comparison.
// Format: slot,leader_pubkey (simple format for easy diffing)
// Called when mismatch is detected or when replay.dump_leader_schedule is set.
func dumpScheduleSlotsCSV(
	epoch uint64,
	source string, // "local" or "rpc"
	schedule *leaderschedule.LeaderSchedule,
	firstSlot uint64,
	numSlots uint64,
	logsDir string,
) string {
	if schedule == nil {
		return ""
	}

	logsDir = resolveLogsDir(logsDir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		mlog.Log.Warnf("dumpScheduleSlotsCSV: failed to create logs dir: %v", err)
		return ""
	}

	// Get short run ID for filename
	runID := mlog.GetRunID()
	shortRunID := ""
	if runID != "" {
		shortRunID = runID
		if len(shortRunID) > 8 {
			shortRunID = shortRunID[:8]
		}
		shortRunID = "_" + shortRunID
	}

	filename := fmt.Sprintf("epoch%d_%s_slots%s.csv", epoch, source, shortRunID)
	filePath := filepath.Join(logsDir, filename)

	f, err := os.Create(filePath)
	if err != nil {
		mlog.Log.Warnf("dumpScheduleSlotsCSV: failed to create file: %v", err)
		return ""
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	// Minimal header - just slot,leader for easy diffing
	w.WriteString("slot,leader\n")

	// Dump all slots
	for i := uint64(0); i < numSlots; i++ {
		slot := firstSlot + i
		leader, ok := schedule.LeaderForSlot(slot)
		if ok {
			w.WriteString(fmt.Sprintf("%d,%s\n", slot, leader.String()))
		} else {
			w.WriteString(fmt.Sprintf("%d,\n", slot)) // Empty leader for missing
		}
	}

	mlog.Log.FileOnlyf("leader schedule slots dumped to: %s (%d slots)", filePath, numSlots)
	return filePath
}

// DumpScheduleMismatch dumps both local and RPC schedules to CSV files for analysis.
// Called when a hash mismatch is detected during validation.
// Returns paths to local and RPC slot files.
func DumpScheduleMismatch(
	epoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	localSchedule *leaderschedule.LeaderSchedule,
	rpcSchedule *leaderschedule.LeaderSchedule,
	logsDir string,
) (localPath, rpcPath string) {
	firstSlot := epochSchedule.FirstSlotInEpoch(epoch)
	numSlots := epochSchedule.SlotsInEpoch(epoch)

	localPath = dumpScheduleSlotsCSV(epoch, "local", localSchedule, firstSlot, numSlots, logsDir)
	rpcPath = dumpScheduleSlotsCSV(epoch, "rpc", rpcSchedule, firstSlot, numSlots, logsDir)

	if localPath != "" && rpcPath != "" {
		mlog.Log.FileOnlyf("schedule mismatch dumps: local=%s rpc=%s", localPath, rpcPath)
		mlog.Log.FileOnlyf("  run: scripts/diff_leader_schedules.py %s %s", localPath, rpcPath)
	}

	return localPath, rpcPath
}

// dumpRPCValidatorList extracts validators from RPC schedule and dumps to CSV.
// Since RPC only gives us slot -> leader, we count slot appearances per leader.
// File is named epoch<N>_rpc_<runid>_validators.csv for comparison with local.
func dumpRPCValidatorList(
	epoch uint64,
	rpcSchedule *leaderschedule.LeaderSchedule,
	firstSlot uint64,
	numSlots uint64,
	logsDir string,
) {
	if rpcSchedule == nil {
		return
	}

	logsDir = resolveLogsDir(logsDir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		mlog.Log.Warnf("dumpRPCValidatorList: failed to create logs dir: %v", err)
		return
	}

	// Count slot appearances per leader
	leaderSlots := make(map[solana.PublicKey]uint64)
	for i := uint64(0); i < numSlots; i++ {
		slot := firstSlot + i
		leader, ok := rpcSchedule.LeaderForSlot(slot)
		if ok {
			leaderSlots[leader]++
		}
	}

	// Build entries sorted by slot count (descending) for comparison with local
	type rpcEntry struct {
		leader    solana.PublicKey
		slotCount uint64
	}
	entries := make([]rpcEntry, 0, len(leaderSlots))
	for leader, count := range leaderSlots {
		entries = append(entries, rpcEntry{leader: leader, slotCount: count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].slotCount != entries[j].slotCount {
			return entries[i].slotCount > entries[j].slotCount
		}
		// Tie-break by pubkey descending (matches local sort)
		return bytes.Compare(entries[i].leader[:], entries[j].leader[:]) > 0
	})

	// Get short run ID for filename
	runID := mlog.GetRunID()
	shortRunID := ""
	if runID != "" {
		shortRunID = runID
		if len(shortRunID) > 8 {
			shortRunID = shortRunID[:8]
		}
		shortRunID = "_" + shortRunID
	}

	filename := fmt.Sprintf("epoch%d_rpc%s_validators.csv", epoch, shortRunID)
	filePath := filepath.Join(logsDir, filename)

	f, err := os.Create(filePath)
	if err != nil {
		mlog.Log.Warnf("dumpRPCValidatorList: failed to create file: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	// Header
	w.WriteString(fmt.Sprintf("# RPC Leader Schedule - Epoch %d\n", epoch))
	w.WriteString(fmt.Sprintf("# Source: rpc\n"))
	w.WriteString(fmt.Sprintf("# Total Leaders: %d\n", len(entries)))
	w.WriteString(fmt.Sprintf("# Total Slots: %d\n", numSlots))
	w.WriteString(fmt.Sprintf("# Generated: %s\n", time.Now().UTC().Format(time.RFC3339)))
	w.WriteString("#\n")
	w.WriteString("# NOTE: RPC schedule only provides slot->leader mapping.\n")
	w.WriteString("# Stake is not available from RPC, so we show slot_count instead.\n")
	w.WriteString("# Compare slot_count with local schedule to identify discrepancies.\n")
	w.WriteString("#\n")
	w.WriteString("rank,node_pubkey,slot_count\n")

	for i, e := range entries {
		w.WriteString(fmt.Sprintf("%d,%s,%d\n", i+1, e.leader, e.slotCount))
	}

	mlog.Log.FileOnlyf("RPC validator list dumped to: %s (%d leaders)", filePath, len(entries))
}

// BackgroundValidateAgainstRPC optionally validates local schedule against RPC in background.
// This is purely for debugging and does not affect the source of truth.
// Computes full SHA256 hash of entire schedule (~20-50ms) for complete comparison.
// Always writes a validation summary file with full local summary and RPC hash.
// Also dumps RPC-derived validator list for comparison.
func BackgroundValidateAgainstRPC(
	epoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	localSchedule *leaderschedule.LeaderSchedule,
	rpcSchedule *leaderschedule.LeaderSchedule,
	localSummary *ScheduleSummary,
	logsDir string,
) {
	if rpcSchedule == nil || localSchedule == nil {
		return
	}

	firstSlot := epochSchedule.FirstSlotInEpoch(epoch)
	numSlots := epochSchedule.SlotsInEpoch(epoch)

	// Compute full hash for RPC schedule
	rpcHash := scheduleFullHash(rpcSchedule, firstSlot, numSlots)

	// Use local summary's hash if available, else compute
	localHash := localSummary.LocalHash
	if localHash == "" {
		localHash = scheduleFullHash(localSchedule, firstSlot, numSlots)
	}

	matched := localHash == rpcHash

	// Update summary with RPC data and write validation file
	localSummary.RPCHash = rpcHash

	// Always write validation summary file with full local summary + RPC data
	writeValidationSummary(localSummary, matched, logsDir)

	if matched {
		mlog.Log.FileOnlyf("leader schedule RPC validation: epoch=%d MATCH hash=%s", epoch, localHash)
		return
	}

	// Only dump RPC validator list on mismatch (expensive I/O)
	dumpRPCValidatorList(epoch, rpcSchedule, firstSlot, numSlots, logsDir)

	// Hashes differ - log to mismatch file with details
	initMismatchLog(logsDir)

	mismatchLogMu.Lock()
	if mismatchLogWriter != nil {
		mismatchLogWriter.WriteString(fmt.Sprintf("\n[%s] RPC VALIDATION MISMATCH epoch=%d\n", time.Now().Format(time.RFC3339), epoch))
		mismatchLogWriter.WriteString(fmt.Sprintf("  local_hash=%s rpc_hash=%s\n", localHash, rpcHash))
	}
	mismatchLogMu.Unlock()

	mlog.Log.Warnf("leader schedule RPC validation: MISMATCH epoch=%d local_hash=%s rpc_hash=%s - see %s",
		epoch, localHash, rpcHash, getMismatchLogPath())

	flushMismatchLog()

	// Dump both schedules to CSV for detailed analysis
	DumpScheduleMismatch(epoch, epochSchedule, localSchedule, rpcSchedule, logsDir)
}

// writeValidationSummary writes a summary file with full local summary and RPC comparison.
func writeValidationSummary(summary *ScheduleSummary, matched bool, logsDir string) {
	logsDir = resolveLogsDir(logsDir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		mlog.Log.Warnf("writeValidationSummary: failed to create logs dir: %v", err)
		return
	}

	shortRunID := ""
	if summary.RunID != "" {
		shortRunID = summary.RunID
		if len(shortRunID) > 8 {
			shortRunID = shortRunID[:8]
		}
		shortRunID = "_" + shortRunID
	}

	filename := fmt.Sprintf("epoch%d_validation%s.txt", summary.BlockEpoch, shortRunID)
	filePath := filepath.Join(logsDir, filename)

	f, err := os.Create(filePath)
	if err != nil {
		mlog.Log.Warnf("writeValidationSummary: failed to create file: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	status := "MATCH"
	if !matched {
		status = "MISMATCH"
	}

	w.WriteString("# Leader Schedule Validation Summary\n")
	w.WriteString(fmt.Sprintf("# Generated: %s\n", time.Now().UTC().Format(time.RFC3339)))
	w.WriteString(fmt.Sprintf("# Run ID: %s\n", summary.RunID))
	w.WriteString("#\n")
	w.WriteString(fmt.Sprintf("## Result: %s\n\n", status))

	// Epoch Info (same as local summary)
	w.WriteString("## Epoch Info\n")
	w.WriteString(fmt.Sprintf("block_epoch=%d\n", summary.BlockEpoch))
	w.WriteString(fmt.Sprintf("schedule_epoch=%d\n", summary.ScheduleEpoch))
	w.WriteString(fmt.Sprintf("first_slot=%d\n", summary.FirstSlot))
	w.WriteString(fmt.Sprintf("slots_in_epoch=%d\n", summary.SlotsInEpoch))
	w.WriteString(fmt.Sprintf("repeat=%d\n", summary.Repeat))
	w.WriteString(fmt.Sprintf("source=%s\n", summary.Source))
	w.WriteString("\n")

	// Stake Info
	w.WriteString("## Stake Info\n")
	w.WriteString(fmt.Sprintf("total_input_stake=%d\n", summary.TotalInputStake))
	w.WriteString(fmt.Sprintf("filtered_stake=%d\n", summary.FilteredStake))
	w.WriteString(fmt.Sprintf("missing_stake=%d\n", summary.MissingStake))
	w.WriteString(fmt.Sprintf("missing_stake_percent=%.4f\n", summary.MissingStakePercent))
	w.WriteString("\n")

	// Validator Counts
	w.WriteString("## Validator Counts\n")
	w.WriteString(fmt.Sprintf("validators_input=%d\n", summary.ValidatorsInput))
	w.WriteString(fmt.Sprintf("validators_used=%d\n", summary.ValidatorsUsed))
	w.WriteString(fmt.Sprintf("validators_skipped=%d\n", summary.ValidatorsSkipped))
	w.WriteString(fmt.Sprintf("skipped_zero_stake=%d\n", summary.SkippedZeroStake))
	w.WriteString(fmt.Sprintf("skipped_missing_data=%d\n", summary.SkippedMissingData))
	w.WriteString(fmt.Sprintf("skipped_zero_nodepk=%d\n", summary.SkippedZeroNodePk))
	w.WriteString("\n")

	// Hashes - local and RPC side by side
	w.WriteString("## Comparison\n")
	w.WriteString(fmt.Sprintf("local_hash=%s\n", summary.LocalHash))
	w.WriteString(fmt.Sprintf("rpc_hash=%s\n", summary.RPCHash))
	w.WriteString(fmt.Sprintf("\nstatus=%s\n", status))

	mlog.Log.FileOnlyf("leader schedule validation summary written to: %s", filePath)
}

// fetchLeaderScheduleFromRPC fetches leader schedule from RPC for validation purposes.
// Does NOT set it as the global schedule - this is for background validation only.
// Tries primary endpoint first, then backups with fewer retries.
// Uses the epoch-aware RPC method to ensure correct schedule during historical catchup.
// RPC method: getLeaderSchedule with epoch parameter
func fetchLeaderScheduleFromRPC(
	epoch uint64,
	epochSchedule *sealevel.SysvarEpochSchedule,
	rpcClient *rpcclient.RpcClient,
	backupEndpoints []string,
) (*leaderschedule.LeaderSchedule, error) {
	firstSlotInEpoch := epochSchedule.FirstSlotInEpoch(epoch)

	// Try primary endpoint first (fewer retries since this is background validation)
	// Pass epoch explicitly to get correct schedule during catchup
	leaderMap, err := fetchLeaderScheduleForEpochWithRetry(rpcClient, epoch, 3)
	if err == nil {
		return leaderschedule.NewLeaderScheduleFromKeyedSlots(leaderMap, firstSlotInEpoch), nil
	}

	lastErr := err
	mlog.Log.Debugf("RPC leader schedule fetch (validation) for epoch %d failed on primary %s: %v", epoch, rpcClient.Endpoint(), err)

	// Try backup endpoints with fewer retries
	for _, endpoint := range backupEndpoints {
		backupClient := rpcclient.NewRpcClient(endpoint)
		leaderMap, err := fetchLeaderScheduleForEpochWithRetry(backupClient, epoch, 2)
		if err == nil {
			mlog.Log.Debugf("RPC leader schedule for epoch %d fetched from backup %s (for validation)", epoch, endpoint)
			return leaderschedule.NewLeaderScheduleFromKeyedSlots(leaderMap, firstSlotInEpoch), nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("RPC leader schedule fetch for epoch %d failed from all endpoints: %w", epoch, lastErr)
}
