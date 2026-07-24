package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

type BenchExportOpts struct {
	// AccountsPath is the accountsdb directory of a stopped instance
	// (same as the node's storage config).
	AccountsPath string
	RpcEndpoint  string
	NumBlocks    uint64
	OutDir       string
	// MaxRPS paces block fetches to stay under public-RPC rate limits.
	// Zero means unpaced. Already-downloaded blocks load from the bundle
	// dir, so a rate-limited export can be re-run to completion.
	MaxRPS float64
}

// sysvarAccountAddrs are the sysvar accounts replay reads each slot.
var sysvarAccountAddrs = []solana.PublicKey{
	sealevel.SysvarClockAddr,
	sealevel.SysvarSlotHashesAddr,
	sealevel.SysvarRecentBlockHashesAddr,
	sealevel.SysvarSlotHistoryAddr,
	sealevel.SysvarStakeHistoryAddr,
	sealevel.SysvarLastRestartSlotAddr,
	sealevel.SysvarEpochScheduleAddr,
	sealevel.SysvarRentAddr,
	sealevel.SysvarFeesAddr,
	sealevel.SysvarEpochRewardsAddr,
}

// ExportBenchBundle packages everything needed to replay blocks B+1..B+n
// standalone, where B is the last slot the local instance replayed. Blocks
// are fetched from RPC; accounts are read from the local accountsdb, which
// must not be in use by a running node.
func ExportBenchBundle(opts BenchExportOpts) error {
	if opts.AccountsPath == "" {
		return fmt.Errorf("no accounts path given")
	}
	if opts.NumBlocks == 0 {
		return fmt.Errorf("num blocks must be > 0")
	}

	mithrilState, err := state.LoadState(opts.AccountsPath)
	if err != nil {
		return fmt.Errorf("loading state file: %w", err)
	}
	if !mithrilState.HasResumeData() {
		return fmt.Errorf("state file has no resume data; replay at least one block first")
	}
	parentSlot := mithrilState.LastSlot
	firstSlot := parentSlot + 1
	lastSlot := parentSlot + opts.NumBlocks

	epochSchedule, _, err := bankEpochScheduleForReplay(mithrilState)
	if err != nil {
		return err
	}
	epoch := epochSchedule.GetEpoch(firstSlot)
	if epochSchedule.GetEpoch(lastSlot) != epoch {
		return fmt.Errorf("slots %d..%d cross an epoch boundary; standalone replay supports a single epoch only", firstSlot, lastSlot)
	}
	if rewards.IsWithinRewardsPeriod(epoch, firstSlot, epochSchedule) ||
		rewards.IsWithinRewardsPeriod(epoch, lastSlot, epochSchedule) {
		return fmt.Errorf("slots %d..%d fall in the epoch rewards distribution period; not supported", firstSlot, lastSlot)
	}

	// Current-epoch stakes give the vote account set to bundle.
	stakesJSON, ok := mithrilState.ComputedEpochStakes[epoch]
	if !ok {
		return fmt.Errorf("state file has no computed epoch stakes for epoch %d", epoch)
	}
	if _, err := global.DeserializeAndLoadEpochStakes([]byte(stakesJSON)); err != nil {
		return fmt.Errorf("loading epoch %d stakes: %w", epoch, err)
	}

	acctsDb, err := accountsdb.OpenDb(opts.AccountsPath)
	if err != nil {
		return fmt.Errorf("opening accountsdb: %w", err)
	}
	acctsDb.InitCaches()
	defer acctsDb.CloseDb()

	if err := os.MkdirAll(filepath.Join(opts.OutDir, benchBlocksDir), 0755); err != nil {
		return err
	}

	// Fetch blocks, write each to the bundle unresolved, then resolve ALTs
	// locally to learn the full account working set.
	rpcc := rpcclient.NewRpcClient(opts.RpcEndpoint)
	keySet := make(map[solana.PublicKey]struct{})
	addKey := func(pk solana.PublicKey) { keySet[pk] = struct{}{} }

	var fetchInterval time.Duration
	if opts.MaxRPS > 0 {
		fetchInterval = time.Duration(float64(time.Second) / opts.MaxRPS)
	}
	var lastFetch time.Time

	var skipped []uint64
	numBlocks := 0
	for slot := firstSlot; slot <= lastSlot; slot++ {
		blockPath := filepath.Join(opts.OutDir, benchBlocksDir, fmt.Sprintf("%d.json", slot))

		var blk *b.Block
		if blk, err = loadBenchBlock(blockPath); err == nil {
			mlog.Log.Infof("block %d already bundled", slot)
		} else {
			if wait := fetchInterval - time.Since(lastFetch); wait > 0 {
				time.Sleep(wait)
			}
			lastFetch = time.Now()
			blockResult, err := rpcc.GetBlockFinalized(slot)
			if err != nil {
				if errors.Is(err, rpcclient.SlotSkipped) {
					skipped = append(skipped, slot)
					continue
				}
				return fmt.Errorf("fetching block %d: %w", slot, err)
			}
			blk = b.FromBlockResult(blockResult, slot, rpcc)

			data, err := json.Marshal(blk)
			if err != nil {
				return fmt.Errorf("marshaling block %d: %w", slot, err)
			}
			if err := os.WriteFile(blockPath, data, 0644); err != nil {
				return err
			}
		}
		numBlocks++

		// ALT table accounts themselves, then the keys they resolve to.
		for _, tx := range blk.Transactions {
			if !tx.Message.IsVersioned() {
				continue
			}
			for _, tableKey := range tx.Message.GetAddressTableLookups().GetTableIDs() {
				addKey(tableKey)
			}
		}
		for _, tx := range blk.Transactions {
			if err := ResolveAddrTableLookupsForTx(context.Background(), acctsDb, parentSlot, tx); err != nil {
				mlog.Log.Warnf("slot %d: ALT resolution failed for tx %s: %v (bundle may be incomplete)",
					slot, tx.Signatures[0], err)
			}
		}
		for _, pk := range extractAndDedupeBlockAccts(blk) {
			addKey(pk)
		}
		if blk.Leader != (solana.PublicKey{}) {
			addKey(blk.Leader)
		}
		mlog.Log.Infof("bundled block %d (%d txs, %d unique keys so far)", slot, len(blk.Transactions), len(keySet))
	}
	if numBlocks == 0 {
		return fmt.Errorf("all slots in %d..%d were skipped", firstSlot, lastSlot)
	}

	for _, addr := range sysvarAccountAddrs {
		addKey(addr)
	}
	for _, gate := range features.AllFeatureGates {
		addKey(gate.Address)
	}
	for votePk := range global.EpochStakes(epoch) {
		addKey(votePk)
	}
	addKey(a.IncineratorAddr)

	keys := make([]solana.PublicKey, 0, len(keySet))
	for pk := range keySet {
		keys = append(keys, pk)
	}
	sort.Slice(keys, func(i, j int) bool { return string(keys[i][:]) < string(keys[j][:]) })

	batch, err := acctsDb.GetAccountsBatch(context.Background(), parentSlot, keys)
	if err != nil {
		return fmt.Errorf("batch account read: %w", err)
	}
	var accts []*accounts.Account
	missing := 0
	for _, acct := range batch {
		if acct == nil {
			missing++
			continue
		}
		accts = append(accts, acct)
	}

	// Upgradeable programs keep their ELF in a separate programdata account
	// that replay loads lazily; pull those in too.
	var programDataKeys []solana.PublicKey
	for _, acct := range accts {
		if acct.Owner != a.BpfLoaderUpgradeableAddr {
			continue
		}
		st, err := sealevel.UnmarshalUpgradeableLoaderState(acct.Data)
		if err != nil || st.Type != sealevel.UpgradeableLoaderStateTypeProgram {
			continue
		}
		if _, seen := keySet[st.Program.ProgramDataAddress]; !seen {
			keySet[st.Program.ProgramDataAddress] = struct{}{}
			programDataKeys = append(programDataKeys, st.Program.ProgramDataAddress)
		}
	}
	if len(programDataKeys) > 0 {
		batch, err := acctsDb.GetAccountsBatch(context.Background(), parentSlot, programDataKeys)
		if err != nil {
			return fmt.Errorf("programdata batch read: %w", err)
		}
		for _, acct := range batch {
			if acct == nil {
				missing++
				continue
			}
			accts = append(accts, acct)
		}
	}

	mlog.Log.Infof("bundling %d accounts (%d keys not found in accountsdb, likely created in-window)", len(accts), missing)
	if err := writeBenchAccounts(opts.OutDir, accts); err != nil {
		return fmt.Errorf("writing accounts.bin: %w", err)
	}

	// Copy the state file verbatim; the runner rebuilds ResumeState from it.
	stateData, err := os.ReadFile(filepath.Join(opts.AccountsPath, "mithril_state.json"))
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(opts.OutDir, "mithril_state.json"), stateData, 0644); err != nil {
		return err
	}

	manifest := &BenchManifest{
		SchemaVersion: 1,
		ParentSlot:    parentSlot,
		FirstSlot:     firstSlot,
		LastSlot:      lastSlot,
		NumBlocks:     numBlocks,
		NumAccounts:   len(accts),
		SkippedSlots:  skipped,
		RpcEndpoint:   opts.RpcEndpoint,
		SourceCommit:  mithrilState.LastWriterCommit,
	}
	if err := writeBenchManifest(opts.OutDir, manifest); err != nil {
		return err
	}

	mlog.Log.Infof("bench bundle written to %s: slots %d..%d (%d blocks, %d skipped), %d accounts",
		opts.OutDir, firstSlot, lastSlot, numBlocks, len(skipped), len(accts))
	return nil
}
