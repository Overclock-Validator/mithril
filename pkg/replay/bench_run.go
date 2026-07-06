package replay

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

type BenchRunOpts struct {
	BundleDir     string
	DbDir         string // scratch accountsdb dir, created fresh (wiped if it exists)
	TxParallelism int    // 0 = sequential tx loop
}

type BenchBlockStat struct {
	Slot       uint64
	VoteTxs    int
	NonVoteTxs int
	CU         uint64
	Exec       time.Duration
}

type BenchRunResult struct {
	Blocks        []BenchBlockStat
	TotalExec     time.Duration
	TotalTxs      int
	TotalCU       uint64
	FinalSlot     uint64
	FinalBankhash string
}

// createScratchAccountsDb lays out an empty single-shard accountsdb in dir
// and opens it.
func createScratchAccountsDb(dir string) (*accountsdb.AccountsDb, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "accounts"), 0755); err != nil {
		return nil, err
	}
	var numShards [8]byte
	binary.LittleEndian.PutUint64(numShards[:], 1)
	if err := os.WriteFile(filepath.Join(dir, "num_shards"), numShards[:], 0644); err != nil {
		return nil, err
	}
	db, err := accountsdb.OpenDb([]string{dir})
	if err != nil {
		return nil, err
	}
	db.InitCaches()
	return db, nil
}

func loadBenchBlock(path string) (*b.Block, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk := new(b.Block)
	if err := json.Unmarshal(data, blk); err != nil {
		return nil, fmt.Errorf("unmarshaling %s: %w", path, err)
	}
	blk.FixupTxVersions()
	return blk, nil
}

// benchSetupVoteAccts replicates setupInitialVoteAcctsAndStakeAccts without
// the stake_pubkeys.idx scan: the vote account set comes from the epoch
// stakes cache (loaded from the bundle's state file), and the bundle carries
// those vote accounts.
func benchSetupVoteAccts(acctsDb *accountsdb.AccountsDb, block *b.Block) error {
	block.VoteTimestamps = make(map[solana.PublicKey]sealevel.BlockTimestamp)
	block.EpochStakesPerVoteAcct = make(map[solana.PublicKey]uint64)

	epochStakes := global.EpochStakes(block.Epoch)
	if len(epochStakes) == 0 {
		return fmt.Errorf("no epoch stakes cached for epoch %d", block.Epoch)
	}
	if err := RebuildVoteCacheFromAccountsDB(acctsDb, block.Slot, epochStakes, 0); err != nil {
		return fmt.Errorf("vote cache rebuild: %w", err)
	}
	rebuildAuthorizedVotersFromVoteCache(block.Epoch)

	maps.Copy(block.EpochStakesPerVoteAcct, epochStakes)
	block.TotalEpochStake = global.EpochTotalStake(block.Epoch)

	for pk, voteState := range global.VoteCache() {
		if voteState != nil {
			if ts := voteState.LastTimestamp(); ts != nil {
				block.VoteTimestamps[pk] = *ts
			}
		}
	}
	return nil
}

// benchConfigureInitialBlock mirrors configureInitialBlockFromResume with the
// stake-index-driven vote setup swapped for the epoch-stakes-driven one.
func benchConfigureInitialBlock(acctsDb *accountsdb.AccountsDb,
	block *b.Block,
	resumeState *ResumeState,
	mithrilState *state.MithrilState,
	epochSchedule *sealevel.SysvarEpochSchedule) error {

	copy(block.ParentBankhash[:], resumeState.ParentBankhash)
	block.ParentSlot = resumeState.ParentSlot
	block.AcctsLtHash = resumeState.AcctsLtHash

	prevFeeRateGovernor := reconstructFeeRateGovernor(mithrilState)
	if prevFeeRateGovernor == nil {
		return fmt.Errorf("state file missing manifest_fee_rate_governor")
	}
	prevFeeRateGovernor.LamportsPerSignature = resumeState.LamportsPerSignature
	prevFeeRateGovernor.PrevLamportsPerSignature = resumeState.PrevLamportsPerSignature
	block.PrevFeeRateGovernor = prevFeeRateGovernor
	block.PrevNumSignatures = resumeState.NumSignatures

	if err := benchSetupVoteAccts(acctsDb, block); err != nil {
		return err
	}
	configureGlobalCtx(block)

	if err := ensureStakeHistorySysvarCached(acctsDb, block.Slot); err != nil {
		return err
	}

	if _, err := PrepareLeaderScheduleLocal(block.Epoch, epochSchedule, ""); err != nil {
		return err
	}
	var exists bool
	block.Leader, exists = global.LeaderForSlot(block.Slot)
	if !exists {
		return fmt.Errorf("no leader for slot %d", block.Slot)
	}

	setBlockHeight(block)

	if resumeState.RecentBlockhashes == nil || len(*resumeState.RecentBlockhashes) == 0 {
		return fmt.Errorf("bundle state file has no blockhash context")
	}
	var zeroHash [32]byte
	if resumeState.EvictedBlockhash == zeroHash || resumeState.LastBlockhash == zeroHash {
		return fmt.Errorf("bundle state file has zero evicted/last blockhash")
	}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = resumeState.RecentBlockhashes
	block.LatestEvictedBlockhash = resumeState.EvictedBlockhash
	block.LastBlockhash = resumeState.LastBlockhash
	if resumeState.SlotHashes != nil {
		sealevel.SysvarCache.SlotHashes.Sysvar = resumeState.SlotHashes
	}

	global.SetLatestBlockHash(block.LastBlockhash)
	return nil
}

// RunBenchBundle replays a bench bundle against a scratch accountsdb and
// reports timing. Divergence from the recorded TxMetas panics, same as
// normal RPC-sourced replay.
func RunBenchBundle(opts BenchRunOpts) (*BenchRunResult, error) {
	manifest, err := ReadBenchManifest(opts.BundleDir)
	if err != nil {
		return nil, fmt.Errorf("reading bundle manifest: %w", err)
	}
	mithrilState, err := state.LoadState(opts.BundleDir)
	if err != nil {
		return nil, fmt.Errorf("loading bundle state file: %w", err)
	}
	resumeState, err := BuildResumeState(mithrilState)
	if err != nil {
		return nil, err
	}
	if resumeState.ParentSlot != manifest.ParentSlot {
		return nil, fmt.Errorf("bundle state file last_slot %d != manifest parent_slot %d",
			resumeState.ParentSlot, manifest.ParentSlot)
	}

	// Collect block files up front so a broken bundle fails before db setup.
	blockDir := filepath.Join(opts.BundleDir, benchBlocksDir)
	entries, err := os.ReadDir(blockDir)
	if err != nil {
		return nil, err
	}
	var blockPaths []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".json" {
			blockPaths = append(blockPaths, filepath.Join(blockDir, e.Name()))
		}
	}
	sort.Strings(blockPaths) // slot numbers are equal-width in practice; verified below
	if len(blockPaths) == 0 {
		return nil, fmt.Errorf("no blocks in bundle")
	}

	mlog.Log.Infof("seeding scratch accountsdb at %s", opts.DbDir)
	acctsDb, err := createScratchAccountsDb(opts.DbDir)
	if err != nil {
		return nil, err
	}
	defer acctsDb.CloseDb()

	accts, err := readBenchAccounts(opts.BundleDir)
	if err != nil {
		return nil, err
	}
	stored := make(chan struct{})
	if err := acctsDb.StoreAccounts(accts, manifest.ParentSlot, func() { close(stored) }); err != nil {
		return nil, err
	}
	<-stored
	if err := acctsDb.StoreBankHashForSlot(manifest.ParentSlot, resumeState.ParentBankhash); err != nil {
		return nil, err
	}
	mlog.Log.Infof("stored %d accounts at slot %d", len(accts), manifest.ParentSlot)

	// Global state setup, mirroring ReplayBlocks' resume path.
	cacheConstantSysvars(acctsDb)
	epochSchedule, _, err := bankEpochScheduleForReplay(mithrilState)
	if err != nil {
		return nil, err
	}
	global.SetCalcUnixTimeForClockSysvar(true)
	global.SetManageLeaderSchedule(true)

	replayCtx, err := newReplayCtx(mithrilState, resumeState)
	if err != nil {
		return nil, err
	}
	global.IncrTransactionCount(mithrilState.ManifestTransactionCount)
	replayCtx.CurrentFeatures, _, _ = scanAndEnableFeatures(acctsDb, replayCtx, manifest.FirstSlot, false)

	for epoch, data := range resumeState.ComputedEpochStakes {
		if _, err := global.DeserializeAndLoadEpochStakes(data); err != nil {
			return nil, fmt.Errorf("loading epoch %d stakes: %w", epoch, err)
		}
	}
	for voteAcctStr, voterStrs := range mithrilState.ManifestEpochAuthorizedVoters {
		voteAcct, err := base58.DecodeFromString(voteAcctStr)
		if err != nil {
			return nil, fmt.Errorf("decoding authorized voter key %s: %w", voteAcctStr, err)
		}
		for _, voterStr := range voterStrs {
			voter, err := base58.DecodeFromString(voterStr)
			if err != nil {
				return nil, fmt.Errorf("decoding authorized voter %s: %w", voterStr, err)
			}
			global.PutEpochAuthorizedVoter(voteAcct, voter)
		}
	}
	global.SetBlockHeight(resumeState.ParentBlockHeight)

	if SerializedParameterArena == nil {
		SerializedParameterArena = arena.New[byte](512 << 20)
	}
	if opts.TxParallelism > 0 && len(sealevel.BorrowedAccountArenas) < opts.TxParallelism {
		sealevel.BorrowedAccountArenas = make([]*arena.Arena[sealevel.BorrowedAccount], opts.TxParallelism)
		for i := range opts.TxParallelism {
			sealevel.BorrowedAccountArenas[i] = arena.New[sealevel.BorrowedAccount](1024)
		}
	}

	dbgOpts, err := NewDebugOptions(nil, nil, false)
	if err != nil {
		return nil, err
	}

	result := &BenchRunResult{}
	pt := &persistedTracker{}
	var lastSlotCtx *sealevel.SlotCtx
	lastSlot := manifest.ParentSlot

	for _, path := range blockPaths {
		block, err := loadBenchBlock(path)
		if err != nil {
			return nil, err
		}
		if block.Slot <= lastSlot {
			return nil, fmt.Errorf("bundle blocks out of order: %d after %d", block.Slot, lastSlot)
		}
		lastSlot = block.Slot
		block.Epoch = epochSchedule.GetEpoch(block.Slot)

		if lastSlotCtx == nil {
			err = benchConfigureInitialBlock(acctsDb, block, resumeState, mithrilState, epochSchedule)
		} else {
			err = configureBlock(block, lastSlotCtx, epochSchedule)
		}
		if err != nil {
			return nil, fmt.Errorf("configuring block %d: %w", block.Slot, err)
		}
		block.Features = replayCtx.CurrentFeatures

		start := time.Now()
		lastSlotCtx, err = ProcessBlock(acctsDb, block, epochSchedule, opts.TxParallelism, dbgOpts, pt)
		if err != nil {
			return nil, fmt.Errorf("replaying block %d: %w", block.Slot, err)
		}
		execTime := time.Since(start)

		global.SetBlockHeight(block.BlockHeight)
		replayCtx.Capitalization -= lastSlotCtx.LamportsBurnt

		stat := BenchBlockStat{
			Slot: block.Slot,
			CU:   lastSlotCtx.TotalComputeUnitsConsumed,
			Exec: execTime,
		}
		for _, tx := range block.Transactions {
			if tx.IsVote() {
				stat.VoteTxs++
			} else {
				stat.NonVoteTxs++
			}
		}
		result.Blocks = append(result.Blocks, stat)
		result.TotalExec += execTime
		result.TotalTxs += stat.VoteTxs + stat.NonVoteTxs
		result.TotalCU += stat.CU

		mlog.Log.InfofPrecise("slot %-10d | txns: v:%-5d nv:%-5d | cu: %-10d | exec:%7.3fs",
			block.Slot, stat.VoteTxs, stat.NonVoteTxs, stat.CU, execTime.Seconds())
	}

	acctsDb.WaitForStoreWorker()
	result.FinalSlot = lastSlot
	result.FinalBankhash = base58.Encode(lastSlotCtx.FinalBankhash)
	return result, nil
}
